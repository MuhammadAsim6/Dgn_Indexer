/**
 * PostgreSQL sink implementation.
 * Updated for Zigchain Custom Modules (DEX, Liquidity, WASM) AND Governance.
 */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { Sink, SinkConfig } from './types.js';
import { createPgPool, getPgPool, closePgPool } from '../db/pg.js';
import { ensureCorePartitions, ensureIbcPartitions } from '../db/partitions.js';
import { upsertProgress } from '../db/progress.js';
import { execBatchedInsert } from './pg/batch.js';
import { recordMissingBlock, resolveMissingBlock } from '../db/missing_blocks.js';
import { getLogger } from '../utils/logger.js';
import {
  pickMessages,
  pickLogs,
  attrsToPairs,
  toNum,
  buildFeeFromDecodedFee,
  collectSignersFromMessages,
  pickSigner,
  parseCoin,
  parseCoins, // ✅ ADDED
  findAttr,
  normArray,
  parseDec,
  tryParseJson,
  toBigIntStr,
  toDateFromTimestamp,
  decodeHexToJson // ✅ ADDED: For IBC packet_data_hex decoding
} from './pg/parsing.js';

// Standard Flushers
import { flushBlocks } from './pg/flushers/blocks.js';
import { flushTxs } from './pg/flushers/txs.js';
import { flushMsgs } from './pg/flushers/msgs.js';
import { flushEvents } from './pg/flushers/events.js';
import { flushAttrs } from './pg/flushers/attrs.js';
import { flushTransfers } from './pg/flushers/transfers.js';
import { flushWasmExec } from './pg/flushers/wasm_exec.js';
import { flushWasmEvents } from './pg/flushers/wasm_events.js';
import { flushWasmEventAttrs } from './pg/flushers/wasm_event_attrs.js';
// ✅ ADDED: IBC Flusher
import { flushIbcPackets } from './pg/flushers/ibc_packets.js';
import { flushIbcTransfers } from './pg/flushers/ibc_transfers.js';
import { flushIbcChannels } from './pg/flushers/ibc_channels.js';
import { flushIbcClients } from './pg/flushers/ibc_clients.js';
import { flushIbcDenoms } from './pg/flushers/ibc_denoms.js';
import { flushIbcConnections } from './pg/flushers/ibc_connections.js';
import { flushCw20Transfers } from './pg/flushers/cw20_transfers.js';

// ✅ ADDED: Bank & Params Flushers
import { flushBalanceDeltas } from './pg/flushers/bank.js';
import { flushNetworkParams } from './pg/flushers/params.js';
// ✅ ADDED: WASM Registry Flusher
import { flushWasmRegistry } from './pg/flushers/wasm.js';
import { flushWasmAdminChanges } from './pg/flushers/wasm_admin_changes.js';

// ✅ Zigchain Flusher
import { flushZigchainData } from './pg/flushers/zigchain.js';

// ✅ Unknown Messages Quarantine
import { flushUnknownMessages } from './pg/flushers/unknown_msgs.js';

// ✅ Zigchain Factory Supply Tracking
import { flushFactorySupplyEvents } from './pg/flushers/factory_supply.js';
import { flushWasmSwaps } from './pg/flushers/wasm_swaps.js';
import { flushTokenRegistry } from './pg/flushers/tokens.js';
import { fetchContractInfoViaAbci } from './pg/helpers/wasm_abci.js';

import {
  buildTokenRegistryRow,
  inferTokenType,
  normalizeIbcDenom
} from '../utils/token-registry.js';
import type { TokenRegistrySource, TokenRegistryType } from '../utils/token-registry.js';
import { createRpcClientFromConfig, RpcClient } from '../rpc/client.js';
import { decodeAnyWithRoot, loadProtoRoot } from '../decode/dynamicProto.js';
import { Root } from 'protobufjs';
import path from 'node:path';

const log = getLogger('sink/postgres');

export type PostgresMode = 'block-atomic' | 'batch-insert';

function extractExpiration(val: any): Date | null {
  const direct = toDateFromTimestamp(val);
  if (direct) return direct;
  const nested = val?.expiration ?? val?.basic?.expiration ?? val?.periodic?.expiration ?? val?.allowance?.expiration;
  return toDateFromTimestamp(nested);
}

function normalizeNonEmptyString(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function normalizeUintAmount(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  return /^\d+$/.test(s) ? s : null;
}

function normalizeNonNegativeInt(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  if (!Number.isSafeInteger(n) || n < 0 || n > 2147483647) return null;
  return n;
}

function isLikelyChannelId(value: string | null): boolean {
  return !!value && /^channel-\d+$/i.test(value);
}

function isLikelyPortId(value: string | null): boolean {
  return !!value && /^(transfer|[a-z][a-z0-9._-]{1,63})$/i.test(value);
}

function isLikelyClientId(value: string | null): boolean {
  return !!value && /^\d{2,}-[a-z0-9-]+-\d+$/i.test(value);
}

function isLikelyDenom(value: string | null): boolean {
  if (!value) return false;
  if (isLikelyChannelId(value) || isLikelyClientId(value)) return false;
  return /^[a-z][a-z0-9/:._-]{1,127}$/i.test(value);
}

type WrapperIbcSettingsNormalized = {
  denom: string | null;
  native_client_id: string | null;
  counterparty_client_id: string | null;
  native_port: string | null;
  counterparty_port: string | null;
  native_channel: string | null;
  counterparty_channel: string | null;
  decimal_difference: number | null;
  legacy_shift_mapped: boolean;
};

function normalizeWrapperIbcSettingsMessage(
  msg: any,
  context: { height: number; txHash: string | null; msgIndex: number },
): WrapperIbcSettingsNormalized {
  const nativeClientId = normalizeNonEmptyString(msg?.native_client_id);
  const counterpartyClientId = normalizeNonEmptyString(msg?.counterparty_client_id);
  const nativePort = normalizeNonEmptyString(msg?.native_port);
  const counterpartyPort = normalizeNonEmptyString(msg?.counterparty_port);
  const nativeChannel = normalizeNonEmptyString(msg?.native_channel);
  const counterpartyChannel = normalizeNonEmptyString(msg?.counterparty_channel);
  const denom = normalizeNonEmptyString(msg?.denom);
  const decimalDifference = normalizeNonNegativeInt(msg?.decimal_difference);
  const decimalRaw =
    msg?.decimal_difference === null || msg?.decimal_difference === undefined
      ? null
      : String(msg.decimal_difference).trim();

  const looksLegacyShifted =
    !denom &&
    isLikelyDenom(counterpartyPort) &&
    isLikelyChannelId(nativePort) &&
    isLikelyPortId(counterpartyClientId) &&
    !isLikelyClientId(counterpartyClientId);

  if (looksLegacyShifted) {
    log.warn(
      `[wrapper] legacy-shifted MsgUpdateIbcSettings remapped: height=${context.height} tx=${context.txHash ?? 'unknown'} msg_index=${context.msgIndex}`,
    );
    return {
      denom: counterpartyPort,
      native_client_id: nativeClientId,
      counterparty_client_id: null,
      native_port: counterpartyClientId,
      counterparty_port: null,
      native_channel: nativePort,
      counterparty_channel: null,
      decimal_difference: decimalDifference,
      legacy_shift_mapped: true,
    };
  }

  if (decimalRaw && decimalRaw.length > 0 && decimalDifference === null) {
    log.warn(
      `[wrapper] invalid decimal_difference ignored: height=${context.height} tx=${context.txHash ?? 'unknown'} msg_index=${context.msgIndex} raw=${decimalRaw}`,
    );
  }

  return {
    denom,
    native_client_id: nativeClientId,
    counterparty_client_id: counterpartyClientId,
    native_port: nativePort,
    counterparty_port: counterpartyPort,
    native_channel: nativeChannel,
    counterparty_channel: counterpartyChannel,
    decimal_difference: decimalDifference,
    legacy_shift_mapped: false,
  };
}

function buildFactoryDenom(creatorRaw: unknown, subDenomRaw: unknown): string | null {
  const creator = normalizeNonEmptyString(creatorRaw);
  const subDenom = normalizeNonEmptyString(subDenomRaw);
  if (!creator || !subDenom) return null;
  return `factory/${creator}/${subDenom}`;
}

function normalizeFactoryDenom(denomRaw: unknown): string | null {
  const denom = normalizeNonEmptyString(denomRaw);
  if (!denom) return null;
  const compact = denom.replace(/\s+/g, '');
  if (!compact.startsWith('factory/')) return null;
  const parts = compact.split('/');
  if (parts.length < 3 || !parts[1] || !parts.slice(2).join('/')) return null;
  return compact;
}

function splitFactoryDenom(denomRaw: unknown): { creatorAddress: string | null; subDenom: string | null } {
  const denom = normalizeFactoryDenom(denomRaw);
  if (!denom) return { creatorAddress: null, subDenom: null };
  const parts = denom.split('/');
  const creatorAddress = normalizeNonEmptyString(parts[1]);
  const subDenom = normalizeNonEmptyString(parts.slice(2).join('/'));
  return { creatorAddress, subDenom };
}

function normalizeFactoryUri(value: unknown): string | null {
  const raw = normalizeNonEmptyString(value);
  if (!raw) return null;

  let uri = raw;
  const gatewayMatch = uri.match(/^https?:\/\/[^/]+\/ipfs\/(.+)$/i);
  if (gatewayMatch?.[1]) {
    uri = `ipfs://${gatewayMatch[1]}`;
  }

  if (/^ipfs:\/\//i.test(uri)) {
    let rest = uri.replace(/^ipfs:\/\//i, '').trim();
    rest = rest.replace(/^\/+/, '');
    rest = rest.replace(/^ipfs\//i, '');
    rest = rest.replace(/^ipfs\.io\//i, '');
    const nestedGatewayMatch = rest.match(/^[^/]+\/ipfs\/(.+)$/i);
    if (nestedGatewayMatch?.[1]) rest = nestedGatewayMatch[1];
    uri = rest ? `ipfs://${rest}` : '';
  }

  const normalized = uri.trim();
  return normalized.length > 0 ? normalized : null;
}

function isValidFactoryUri(uri: string): boolean {
  if (uri.length > 2048) return false;
  if (/\s/.test(uri)) return false;
  return /^(ipfs|https?):\/\/.+$/i.test(uri);
}

function normalizeFactoryUriHash(value: unknown): string | null {
  const raw = normalizeNonEmptyString(value);
  if (!raw) return null;
  const stripped = raw.startsWith('0x') || raw.startsWith('0X') ? raw.slice(2) : raw;
  return /^[A-Fa-f0-9]{64}$/.test(stripped) ? stripped : null;
}

function sanitizeFactoryUriFields(
  uriRaw: unknown,
  uriHashRaw: unknown,
  context: { denom: string | null; txHash: string | null; msgType: string; height: number }
): { uri: string | null; uri_hash: string | null } {
  const rawUri = normalizeNonEmptyString(uriRaw);
  const rawUriHash = normalizeNonEmptyString(uriHashRaw);

  let uri = normalizeFactoryUri(uriRaw);
  if (uri && !isValidFactoryUri(uri)) {
    uri = null;
  }
  const uriHash = normalizeFactoryUriHash(uriHashRaw);

  const ctx = `[factory] ${context.msgType} denom=${context.denom ?? 'unknown'} height=${context.height} tx=${context.txHash ?? 'unknown'}`;
  if (rawUri && !uri) {
    log.warn(`${ctx} invalid URI format ignored: ${rawUri}`);
  }
  if (rawUriHash && !uriHash) {
    log.warn(`${ctx} invalid URI_hash format ignored`);
  }

  return { uri, uri_hash: uriHash };
}

function pickFirstNonEmptyAttr(
  attrsPairs: Array<{ key: string; value: string | null }>,
  keys: string[],
): string | null {
  for (const key of keys) {
    const value = normalizeNonEmptyString(findAttr(attrsPairs, key));
    if (value) return value;
  }
  return null;
}

function parseFeeCoins(fee: any): Array<{ denom: string; amount: string }> {
  if (!fee) return [];
  const raw = fee?.amount ?? fee?.Amount ?? fee;
  if (Array.isArray(raw)) {
    return raw
      .map((c: any) => ({ denom: String(c?.denom ?? '').trim(), amount: String(c?.amount ?? '').trim() }))
      .filter((c) => c.denom.length > 0 && /^\d+$/.test(c.amount));
  }
  if (raw && typeof raw === 'object') {
    const denom = String((raw as any)?.denom ?? '').trim();
    const amount = String((raw as any)?.amount ?? '').trim();
    return denom.length > 0 && /^\d+$/.test(amount) ? [{ denom, amount }] : [];
  }
  if (typeof raw === 'string') {
    return parseCoins(raw);
  }
  return [];
}

type TokenRegistryRpcMetadata = {
  symbol: string | null;
  base_denom: string | null;
  decimals: number | null;
  full_meta: Record<string, unknown> | null;
};

function normalizePlainObject(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const obj = value as Record<string, unknown>;
  return Object.keys(obj).length > 0 ? obj : null;
}

function extractDecimalsFromBankMetadata(meta: Record<string, unknown>): number | null {
  const display = normalizeNonEmptyString(meta['display']);
  const denomUnitsRaw = meta['denom_units'] ?? meta['denomUnits'];
  if (!Array.isArray(denomUnitsRaw) || denomUnitsRaw.length === 0) return null;

  let maxExponent: number | null = null;
  for (const unitRaw of denomUnitsRaw) {
    const unit = normalizePlainObject(unitRaw);
    if (!unit) continue;

    const unitDenom = normalizeNonEmptyString(unit['denom']);
    const exponent = normalizeNonNegativeInt(unit['exponent']);
    if (exponent === null) continue;

    if (display && unitDenom === display) {
      return exponent;
    }
    maxExponent = maxExponent === null ? exponent : Math.max(maxExponent, exponent);
  }

  return maxExponent;
}

function extractTokenRegistryMetadataFromBankResponse(decoded: any): TokenRegistryRpcMetadata | null {
  const metadataObj = normalizePlainObject(decoded?.metadata ?? decoded?.metadatas?.[0]);
  if (!metadataObj) return null;

  return {
    symbol: normalizeNonEmptyString(metadataObj['symbol']) ?? normalizeNonEmptyString(metadataObj['display']) ?? null,
    base_denom: normalizeNonEmptyString(metadataObj['base']),
    decimals: extractDecimalsFromBankMetadata(metadataObj),
    full_meta: metadataObj,
  };
}

export interface PostgresSinkConfig extends SinkConfig {
  pg: {
    connectionString?: string;
    host?: string;
    port?: number;
    user?: string;
    password?: string;
    database?: string;
    ssl?: boolean;
    poolSize?: number;
    progressId?: string;
  };
  /**
   * RPC URL needed for Reconciliation Engine
   */
  rpcUrl: string;
  mode?: PostgresMode;
  batchSizes?: {
    blocks?: number;
    txs?: number;
    msgs?: number;
    events?: number;
    attrs?: number;
  };
}

type BlockLine = any;

export class PostgresSink implements Sink {
  private cfg: PostgresSinkConfig;
  private mode: PostgresMode;
  private rpc: RpcClient | null = null;
  private protoRoot: Root | null = null;
  private wasmContractsCache = new Map<string, any>();
  private tokenRegistryMetadataCache = new Map<string, TokenRegistryRpcMetadata | null>();
  private isShuttingDown = false;

  // Standard Buffers
  private bufBlocks: any[] = [];
  private bufTxs: any[] = [];
  private bufMsgs: any[] = [];
  private bufEvents: any[] = [];
  private bufAttrs: any[] = [];
  private bufTransfers: any[] = [];
  private bufWasmExec: any[] = [];
  private bufWasmEvents: any[] = [];
  private bufWasmEventAttrs: any[] = [];

  // ✅ IBC Buffer
  private bufIbcPackets: any[] = [];
  private bufIbcChannels: any[] = [];
  private bufIbcTransfers: any[] = [];
  private bufIbcClients: any[] = [];
  private bufIbcDenoms: any[] = [];
  private bufIbcConnections: any[] = [];



  // ✅ ADDED: Missing Buffers
  private bufBalanceDeltas: any[] = [];
  private bufWasmCodes: any[] = [];
  private bufWasmContracts: any[] = [];
  private bufWasmMigrations: any[] = [];
  private bufWasmAdminChanges: any[] = [];
  private bufWasmInstantiateConfigs: any[] = [];
  private bufNetworkParams: any[] = [];

  // Zigchain Buffers
  private bufFactoryDenoms: any[] = [];
  private bufDexPools: any[] = [];
  private bufDexSwaps: any[] = [];
  private bufDexLiquidity: any[] = [];
  private bufWrapperSettings: any[] = [];
  private bufCw20Transfers: any[] = [];

  // ✅ WASM DEX Swap Analytics
  private bufWasmSwaps: any[] = [];

  private bufTokenRegistry: any[] = []; // ✅ NEW
  private bufWrapperEvents: any[] = [];

  // ✅ Unknown Messages Quarantine
  private bufUnknownMsgs: any[] = [];

  // ✅ Zigchain Factory Supply Tracking
  private bufFactorySupplyEvents: any[] = [];

  private batchSizes = {
    blocks: 1000,
    txs: 2000,
    msgs: 5000,
    events: 5000,
    attrs: 10000,
    transfers: 5000,
    wasmExec: 5000,
    wasmEvents: 5000,
    wasmEventAttrs: 5000,
    ibcPackets: 2000,
    ibcChannels: 500,

    ibcTransfers: 2000,
    zigchain: 2000,
    cw20Transfers: 5000
  };


  constructor(cfg: PostgresSinkConfig) {
    this.cfg = cfg;
    this.mode = cfg.mode ?? 'batch-insert';
    if (cfg.batchSizes) Object.assign(this.batchSizes, cfg.batchSizes);
  }

  async init(): Promise<void> {
    await createPgPool({ ...this.cfg.pg, applicationName: 'cosmos-indexer' });

    // ✅ Initialize RPC client and protoRoot for ABCI queries
    if (this.cfg.rpcUrl) {
      this.rpc = createRpcClientFromConfig({
        rpcUrl: this.cfg.rpcUrl,
        timeoutMs: 30000,
        retries: 2,
        backoffMs: 500,
        backoffJitter: 0.2,
        rps: 10,
      });
      const protoDir = process.env.PROTO_DIR || path.join(process.cwd(), 'protos');
      try {
        this.protoRoot = await loadProtoRoot(protoDir);
      } catch (err) {
        log.warn(`[postgres] Failed to load proto root from ${protoDir}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
  }

  async write(line: any): Promise<void> {
    let obj: BlockLine;
    if (typeof line === 'string') {
      try {
        obj = JSON.parse(line);
      } catch {
        return;
      }
    } else {
      obj = line;
    }
    if (obj?.error) return;

    if (this.mode === 'block-atomic') {
      await this.persistBlockAtomic(obj);
    } else {
      await this.persistBlockBuffered(obj);
    }
  }

  async flush(): Promise<void> {
    if (this.mode === 'batch-insert') {
      await this.flushAll();
    }
  }

  async close(): Promise<void> {
    if (this.isShuttingDown) return;
    this.isShuttingDown = true;
    await this.flush?.();
    await closePgPool();
  }

  async recordMissingBlock(height: number, error: string | null): Promise<void> {
    const pool = getPgPool();
    const client = await pool.connect();
    try {
      await recordMissingBlock(client, height, error);
    } finally {
      client.release();
    }
  }

  async resolveMissingBlock(height: number): Promise<void> {
    const pool = getPgPool();
    const client = await pool.connect();
    try {
      await resolveMissingBlock(client, height);
    } finally {
      client.release();
    }
  }

  private async fetchTokenRegistryMetadataViaAbci(denom: string): Promise<TokenRegistryRpcMetadata | null> {
    if (this.tokenRegistryMetadataCache.has(denom)) {
      return this.tokenRegistryMetadataCache.get(denom) ?? null;
    }
    if (!this.rpc || !this.protoRoot) {
      this.tokenRegistryMetadataCache.set(denom, null);
      return null;
    }

    const lower = denom.toLowerCase();
    // 1. IBC Denoms
    if (lower.startsWith('ibc/')) {
      return this.fetchTokenRegistryIbcMetadataViaAbci(denom);
    }
    // 2. CW20 Address (zig1... length 63)
    if (lower.startsWith('zig1') && (denom.length === 63 || denom.length === 39)) {
      return this.fetchTokenRegistryCw20MetadataViaAbci(denom);
    }
    // 3. Default (Bank)
    return this.fetchTokenRegistryBankMetadataViaAbci(denom);
  }

  private async fetchTokenRegistryBankMetadataViaAbci(denom: string): Promise<TokenRegistryRpcMetadata | null> {
    try {
      const ReqType = this.protoRoot!.lookupType('cosmos.bank.v1beta1.QueryDenomMetadataRequest');
      const ResType = 'cosmos.bank.v1beta1.QueryDenomMetadataResponse';
      const reqMsg = ReqType.create({ denom });
      const reqBytes = ReqType.encode(reqMsg).finish();
      const reqHex = '0x' + Buffer.from(reqBytes).toString('hex');
      const response = await this.rpc!.queryAbci('/cosmos.bank.v1beta1.Query/DenomMetadata', reqHex);

      if (!response || response.code !== 0 || !response.value) {
        this.tokenRegistryMetadataCache.set(denom, null);
        return null;
      }

      const decoded = decodeAnyWithRoot(ResType, Buffer.from(response.value, 'base64'), this.protoRoot!) as any;
      const parsed = extractTokenRegistryMetadataFromBankResponse(decoded);
      this.tokenRegistryMetadataCache.set(denom, parsed);
      return parsed;
    } catch (err: any) {
      this.tokenRegistryMetadataCache.set(denom, null);
      log.debug(`[token-registry] skip bank metadata fetch for ${denom}: ${err.message}`);
      return null;
    }
  }

  private async fetchTokenRegistryCw20MetadataViaAbci(address: string): Promise<TokenRegistryRpcMetadata | null> {
    try {
      const ReqType = this.protoRoot!.lookupType('cosmwasm.wasm.v1.QuerySmartContractStateRequest');
      const ResType = 'cosmwasm.wasm.v1.QuerySmartContractStateResponse';
      const queryMsg = JSON.stringify({ token_info: {} });
      const queryBytes = Buffer.from(queryMsg);
      const reqMsg = ReqType.create({ address, queryData: queryBytes });
      const reqBytes = ReqType.encode(reqMsg).finish();
      const reqHex = '0x' + Buffer.from(reqBytes).toString('hex');
      const response = await this.rpc!.queryAbci('/cosmwasm.wasm.v1.Query/SmartContractState', reqHex);

      if (!response || response.code !== 0 || !response.value) {
        this.tokenRegistryMetadataCache.set(address, null);
        return null;
      }

      const decoded = decodeAnyWithRoot(ResType, Buffer.from(response.value, 'base64'), this.protoRoot!) as any;
      const data = tryParseJson(Buffer.from(decoded.data, 'base64').toString());
      if (!data) return null;

      const parsed: TokenRegistryRpcMetadata = {
        symbol: normalizeNonEmptyString(data.symbol) ?? null,
        base_denom: address,
        decimals: normalizeNonNegativeInt(data.decimals),
        full_meta: normalizePlainObject(data),
      };
      this.tokenRegistryMetadataCache.set(address, parsed);
      return parsed;
    } catch (err: any) {
      this.tokenRegistryMetadataCache.set(address, null);
      log.debug(`[token-registry] skip cw20 metadata fetch for ${address}: ${err.message}`);
      return null;
    }
  }

  private async fetchTokenRegistryIbcMetadataViaAbci(denom: string): Promise<TokenRegistryRpcMetadata | null> {
    try {
      const hash = denom.slice(4); // Remove 'ibc/'
      const ReqType = this.protoRoot!.lookupType('ibc.applications.transfer.v1.QueryDenomTraceRequest');
      const ResType = 'ibc.applications.transfer.v1.QueryDenomTraceResponse';
      const reqMsg = ReqType.create({ hash });
      const reqBytes = ReqType.encode(reqMsg).finish();
      const reqHex = '0x' + Buffer.from(reqBytes).toString('hex');
      const response = await this.rpc!.queryAbci('/ibc.applications.transfer.v1.Query/DenomTrace', reqHex);

      if (!response || response.code !== 0 || !response.value) {
        this.tokenRegistryMetadataCache.set(denom, null);
        return null;
      }

      const decoded = decodeAnyWithRoot(ResType, Buffer.from(response.value, 'base64'), this.protoRoot!) as any;
      const trace = decoded?.denomTrace || decoded?.denom_trace;
      const baseDenom = normalizeNonEmptyString(trace?.base_denom ?? trace?.baseDenom);
      if (!baseDenom) return null;

      // Now fetch metadata for the base denom
      const meta = await this.fetchTokenRegistryMetadataViaAbci(baseDenom);
      if (meta) {
        this.tokenRegistryMetadataCache.set(denom, meta);
        return meta;
      }
      return null;
    } catch (err: any) {
      this.tokenRegistryMetadataCache.set(denom, null);
      log.debug(`[token-registry] skip ibc metadata fetch for ${denom}: ${err.message}`);
      return null;
    }
  }

  private applyTokenRegistryRpcMetadata(row: any, metadata: TokenRegistryRpcMetadata): void {
    if (metadata.decimals !== null) {
      row.decimals = metadata.decimals;
    }
    if (metadata.symbol) {
      row.symbol = metadata.symbol;
    }
    if (metadata.base_denom) {
      row.base_denom = metadata.base_denom;
    }
    if (metadata.full_meta) {
      const existing = normalizePlainObject(row?.metadata);
      row.metadata = existing
        ? { ...existing, ...metadata.full_meta }
        : { ...metadata.full_meta };
    }
  }

  private async enrichTokenRegistryRowsFromRpc(rows: any[]): Promise<void> {
    if (!rows?.length || !this.rpc || !this.protoRoot) return;

    const rowsByDenom = new Map<string, any[]>();
    for (const row of rows) {
      const denom = normalizeNonEmptyString(row?.denom);
      if (!denom) continue;
      const bucket = rowsByDenom.get(denom);
      if (bucket) {
        bucket.push(row);
      } else {
        rowsByDenom.set(denom, [row]);
      }
    }

    const denoms = Array.from(rowsByDenom.keys());
    if (denoms.length === 0) return;

    // 1. Determine which denoms need fetching (not in cache)
    const pendingDenoms = denoms.filter((denom) => !this.tokenRegistryMetadataCache.has(denom));
    if (pendingDenoms.length > 0) {
      const concurrency = Math.min(8, pendingDenoms.length);
      let cursor = 0;
      const workers = Array.from({ length: concurrency }, async () => {
        while (true) {
          const idx = cursor++;
          if (idx >= pendingDenoms.length) break;
          await this.fetchTokenRegistryMetadataViaAbci(pendingDenoms[idx]);
        }
      });
      await Promise.all(workers);
    }

    // 2. Share metadata between common aliases (Factory <-> Coin wrapper)
    for (const denom of denoms) {
      const meta = this.tokenRegistryMetadataCache.get(denom);
      const lower = denom.toLowerCase();
      let alias: string | null = null;
      if (lower.startsWith('factory/')) {
        const p = denom.split('/');
        if (p.length >= 3) alias = `coin.${p[1]}.${p.slice(2).join('/')}`;
      } else if (lower.startsWith('coin.')) {
        const p = denom.split('.');
        if (p.length >= 3) alias = `factory/${p[1]}/${p.slice(2).join('.')}`;
      }

      if (alias) {
        const aliasMeta = this.tokenRegistryMetadataCache.get(alias);
        if (meta && !aliasMeta) {
          this.tokenRegistryMetadataCache.set(alias, meta);
        } else if (!meta && aliasMeta) {
          this.tokenRegistryMetadataCache.set(denom, aliasMeta);
        }
      }
    }

    // 3. Apply metadata to the rows
    let enrichedRowCount = 0;
    for (const [denom, denomRows] of rowsByDenom.entries()) {
      const metadata = this.tokenRegistryMetadataCache.get(denom) ?? null;
      if (!metadata) continue;
      for (const row of denomRows) {
        this.applyTokenRegistryRpcMetadata(row, metadata);
        enrichedRowCount += 1;
      }
    }

    if (enrichedRowCount > 0) {
      log.debug(`[token-registry] RPC metadata enriched rows=${enrichedRowCount} denoms=${denoms.length}`);
    }
  }

  private extractRows(blockLine: BlockLine) {
    const height = Number(blockLine?.meta?.height);
    const time = new Date(blockLine?.meta?.time);

    // Block Extraction
    const b = blockLine.block;
    const blockRow = {
      height,
      block_hash: b?.block_id?.hash ?? null,
      time,
      proposer_address: b?.block?.header?.proposer_address ?? null,
      tx_count: Array.isArray(blockLine?.txs) ? blockLine.txs.length : 0,
      last_commit_hash: b?.block?.header?.last_commit_hash ?? b?.block?.last_commit?.block_id?.hash ?? null,
      data_hash: b?.block?.header?.data_hash ?? null,
      evidence_count: Array.isArray(b?.block?.evidence?.evidence) ? b.block.evidence.evidence.length : 0,
      app_hash: b?.block?.header?.app_hash ?? null,
    };

    // ✅ Define ALL row arrays at the top to fix scope issues
    const txRows: any[] = [];
    const msgRows: any[] = [];
    const evRows: any[] = [];
    const attrRows: any[] = [];
    const transfersRows: any[] = [];
    let droppedTransferRows = 0;

    // Zigchain
    const factoryDenomsRows: any[] = [];
    const dexPoolsRows: any[] = [];
    const dexSwapsRows: any[] = [];
    const dexLiquidityRows: any[] = [];
    const wrapperSettingsRows: any[] = [];

    // WASM
    const wasmExecRows: any[] = [];
    const wasmEventsRows: any[] = [];
    const wasmEventAttrsRows: any[] = [];

    // ✅ IBC
    const ibcPacketsRows: any[] = [];
    const ibcChannelsRows: any[] = [];
    const ibcTransfersRows: any[] = [];
    const ibcClientsRows: any[] = [];
    const ibcDenomsRows: any[] = [];
    const ibcConnectionsRows: any[] = [];



    // ✅ Tokens (CW20)
    const cw20TransfersRows: any[] = [];

    // ✅ ADDED: New Local Arrays
    const balanceDeltasRows: any[] = [];
    const wasmCodesRows: any[] = [];
    const wasmContractsRows: any[] = [];
    const wasmMigrationsRows: any[] = [];
    const wasmAdminChangesRows: any[] = [];
    const wasmInstantiateConfigsRows: any[] = [];
    const networkParamsRows: any[] = [];

    // ✅ WASM DEX Swaps Analytics
    const wasmSwapsRows: any[] = [];
    const tokenRegistryByDenom = new Map<string, any>();

    // ✅ Unknown Messages Quarantine
    const unknownMsgsRows: any[] = [];
    const factorySupplyEventsRows: any[] = [];
    const wrapperEventsRows: any[] = [];

    // 🟢 TOKEN REGISTRY HELPER 🟢
    function registerToken(
      denom: unknown,
      type?: TokenRegistryType,
      metadata: any = {},
      tx_h?: string | null,
      source: TokenRegistrySource = 'default',
    ): void {
      if (denom === null || denom === undefined) return;
      if (typeof denom !== 'string' && typeof denom !== 'number' && typeof denom !== 'bigint') return;
      const rawDenom = String(denom).trim();
      if (rawDenom.length === 0) return;

      const rType = type ?? inferTokenType(rawDenom);
      const normalizedDenom = rType === 'ibc' ? normalizeIbcDenom(rawDenom) : rawDenom;

      const row = buildTokenRegistryRow({
        denom: normalizedDenom,
        type: rType,
        source,
        height,
        txHash: tx_h ?? null,
        metadata,
      });

      if (!row) return;

      const upsert = (r: any) => {
        const existing = tokenRegistryByDenom.get(r.denom);
        if (!existing) {
          tokenRegistryByDenom.set(r.denom, r);
          return;
        }

        const hasExistingHeight = Number.isFinite(existing.first_seen_height);
        const hasIncomingHeight = Number.isFinite(r.first_seen_height);
        let firstSeenHeight = existing.first_seen_height;
        let firstSeenTx = existing.first_seen_tx ?? r.first_seen_tx ?? null;

        if (!hasExistingHeight && hasIncomingHeight) {
          firstSeenHeight = r.first_seen_height;
          firstSeenTx = r.first_seen_tx ?? existing.first_seen_tx ?? null;
        } else if (
          hasExistingHeight &&
          hasIncomingHeight &&
          Number(r.first_seen_height) < Number(existing.first_seen_height)
        ) {
          firstSeenHeight = r.first_seen_height;
          firstSeenTx = r.first_seen_tx ?? existing.first_seen_tx ?? null;
        } else if (
          hasExistingHeight &&
          hasIncomingHeight &&
          Number(r.first_seen_height) === Number(existing.first_seen_height)
        ) {
          firstSeenTx = existing.first_seen_tx ?? r.first_seen_tx ?? null;
        }

        const shouldPreferIncoming = (existing.type === 'native' && r.type !== 'native') ||
          (!!r.metadata && (!existing.metadata || Object.keys(r.metadata).length > Object.keys(existing.metadata).length));

        tokenRegistryByDenom.set(r.denom, {
          ...existing,
          type: existing.type === 'native' && r.type !== 'native' ? r.type : existing.type,
          base_denom: (shouldPreferIncoming ? r.base_denom : (r.base_denom ?? existing.base_denom)) ?? existing.base_denom,
          symbol: (shouldPreferIncoming ? r.symbol : (r.symbol ?? existing.symbol)) ?? existing.symbol,
          decimals: (shouldPreferIncoming ? r.decimals : (r.decimals ?? existing.decimals)) ?? existing.decimals,
          creator: (shouldPreferIncoming ? r.creator : (r.creator ?? existing.creator)) ?? existing.creator,
          first_seen_height: firstSeenHeight,
          first_seen_tx: firstSeenTx,
          is_primary: (r.is_primary !== undefined ? r.is_primary : existing.is_primary) ?? true,
          is_verified: existing.is_verified && r.is_verified,
          metadata:
            (existing.metadata || r.metadata)
              ? { ...(existing.metadata || {}), ...(r.metadata || {}) }
              : null,
        });
      };

      upsert(row);

      // ✅ ENHANCEMENT: Cross-Register Factory/Wrapper Denoms
      // If we see a factory/ denom, check if it's been wrapped.
      // If we see a coin.* denom, register the underlying factory/ denom.
      const lowerDenom = rawDenom.toLowerCase();
      if (row.type === 'factory') {
        if (lowerDenom.startsWith('factory/')) {
          const parts = rawDenom.split('/');
          if (parts.length >= 3) {
            const creator = parts[1];
            const subDenom = parts.slice(2).join('/');
            const wrapperDenom = `coin.${creator}.${subDenom}`;
            // Wrapper is Primary, Factory is Secondary
            upsert({ ...row, denom: wrapperDenom, metadata: { ...row.metadata, base: row.denom }, is_primary: true });
            upsert({ ...row, denom: row.denom, is_primary: false });
          }
        } else if (lowerDenom.startsWith('coin.')) {
          const parts = rawDenom.split('.');
          if (parts.length >= 3) {
            const creator = parts[1];
            const subDenom = parts.slice(2).join('.');
            const factoryDenom = `factory/${creator}/${subDenom}`;
            // Wrapper is Primary, Factory is Secondary
            upsert({ ...row, denom: row.denom, is_primary: true });
            upsert({ ...row, denom: factoryDenom, metadata: { ...row.metadata, base: row.denom }, is_primary: false });
          }
        }
      }
    }
    // ✅ NATIVE TOKEN REGISTRY (Always ensure Zigchain native token is registered)
    registerToken('uzig', 'native', { symbol: 'ZIG', base_denom: 'uzig', decimals: 6 }, null);

    // 🟢 BANK BALANCE DELTAS (Helper)
    const extractBalanceDeltas = (
      evType: string,
      attrsPairs: { key: string; value: string | null }[],
      tx_hash?: string,
      msg_index?: number,
      event_index?: number,
    ): number => {
      // ✅ FIX: Support multiple attribute keys
      const getReceiver = () =>
        findAttr(attrsPairs, 'receiver') ||
        findAttr(attrsPairs, 'recipient') ||
        findAttr(attrsPairs, 'to_address') ||
        findAttr(attrsPairs, 'to');

      const getSpender = () =>
        findAttr(attrsPairs, 'spender') ||
        findAttr(attrsPairs, 'sender') ||
        findAttr(attrsPairs, 'from_address') ||
        findAttr(attrsPairs, 'from');

      if (evType === 'coin_received' || evType === 'coin_spent') {
        const acc = evType === 'coin_received' ? getReceiver() : getSpender();

        // ✅ FIX: Strict Null Guard - Critical for preventing corrupt aggregates
        if (!acc || acc.trim() === '') {
          return 0;
        }

        const amountStr = findAttr(attrsPairs, 'amount');
        const coins = parseCoins(amountStr);
        let added = 0;
        for (const coin of coins) {
          balanceDeltasRows.push({
            height,
            account: acc,
            denom: coin.denom,
            delta: evType === 'coin_received' ? coin.amount : `-${coin.amount}`,
            // ✅ ADDED: Unique Constraint Keys for Intra-Block Deduplication
            tx_hash: (typeof tx_hash !== 'undefined') ? tx_hash : 'block_event',
            msg_index: (typeof msg_index !== 'undefined') ? msg_index : -1,
            event_index: (typeof event_index !== 'undefined') ? event_index : -1 // ✅ FIX: Use param, not 'ei'
          });
          registerToken(coin?.denom, undefined, {}, tx_hash ?? null);
          added++;
        }
        return added;
      }
      return 0;
    };

    const extractFactorySupplyFromEvent = (
      evType: string,
      attrsPairs: { key: string; value: string | null }[],
      txHashInput?: string,
      msgIndexInput?: number,
      eventIndexInput?: number,
    ): number => {
      const action = evType === 'coinbase' ? 'mint' : (evType === 'burn' ? 'burn' : null);
      if (!action) return 0;

      const amountStr = findAttr(attrsPairs, 'amount');
      const parsedCoins = parseCoins(amountStr);
      const fallbackDenom = normalizeNonEmptyString(
        findAttr(attrsPairs, 'denom') || findAttr(attrsPairs, 'token_denom'),
      );
      const fallbackAmount = normalizeUintAmount(amountStr);

      const aggregated = new Map<string, bigint>();
      const addCoin = (denomRaw: unknown, amountRaw: unknown) => {
        const denom = normalizeNonEmptyString(denomRaw);
        const amount = normalizeUintAmount(amountRaw);
        if (!denom || !amount) return;
        const prev = aggregated.get(denom) ?? 0n;
        aggregated.set(denom, prev + BigInt(amount));
      };

      for (const c of parsedCoins) {
        addCoin(c?.denom, c?.amount);
      }
      if (aggregated.size === 0 && fallbackDenom && fallbackAmount) {
        addCoin(fallbackDenom, fallbackAmount);
      }
      if (aggregated.size === 0) return 0;

      const txHash = normalizeNonEmptyString(txHashInput) ?? `block_supply_${height}`;
      const msgIndex = typeof msgIndexInput === 'number'
        ? msgIndexInput
        : -1000000 - Math.max(0, Number(eventIndexInput ?? 0));

      let added = 0;
      for (const [denom, amount] of aggregated.entries()) {
        if (amount <= 0n) continue;
        factorySupplyEventsRows.push({
          height,
          tx_hash: txHash,
          msg_index: msgIndex,
          event_index: (typeof eventIndexInput === 'number') ? eventIndexInput : -1,
          denom,
          action,
          amount: amount.toString(),
          sender: pickFirstNonEmptyAttr(attrsPairs, ['sender', 'spender', 'from', 'from_address', 'minter']),
          recipient: pickFirstNonEmptyAttr(attrsPairs, ['recipient', 'receiver', 'to', 'to_address']),
          metadata: null,
        });
        registerToken(denom, undefined, {}, txHash);
        added += 1;
      }
      return added;
    };

    // 🛡️ INITIAL SYNC: Fetch network parameters (kept)


    const txs = Array.isArray(blockLine?.txs) ? blockLine.txs : [];

    for (const tx of txs) {
      const tx_hash = tx.hash ?? tx.txhash ?? tx.tx_hash ?? null;
      const txIbcIntents: any[] = [];
      const txChannelIntents: { port: string; channel?: string; ordering: string }[] = [];
      const tx_index = Number(tx.index ?? tx.tx_index ?? tx?.tx_response?.index ?? 0);
      const code = Number(tx.code ?? tx?.tx_response?.code ?? 0);
      const isSuccess = code === 0;
      const gas_wanted = toNum(tx.gas_wanted ?? tx?.tx_response?.gas_wanted);
      const gas_used = toNum(tx.gas_used ?? tx?.tx_response?.gas_used);
      const fee = tx.fee ?? buildFeeFromDecodedFee(tx?.decoded?.auth_info?.fee);
      const memo = tx.memo ?? tx?.decoded?.body?.memo ?? null;
      let signers: string[] | null = Array.isArray(tx.signers) ? tx.signers : null;
      const raw_tx = tx.raw_tx ?? tx?.decoded ?? tx?.raw ?? null;
      const log_summary = tx.log_summary ?? tx?.tx_response?.raw_log ?? null;

      const msgs = pickMessages(tx);
      if (!signers || signers.length === 0) {
        const derived = collectSignersFromMessages(msgs);
        if (derived) signers = derived;
      }
      const firstSigner = Array.isArray(signers) && signers.length ? signers[0] : null;

      txRows.push({
        tx_hash, height, tx_index, code, gas_wanted, gas_used, fee, memo, signers, raw_tx, log_summary, time
      });

      const logs = pickLogs(tx);
      let txBankDeltaCount = 0;
      // 🚀 PERFORMANCE: Map-based log lookup (O(1) instead of O(N))
      const logMap = new Map<number, any>();
      for (const l of logs) {
        logMap.set(Number(l.msg_index), l);
      }

      // --- PROCESS MESSAGES ---
      for (let i = 0; i < msgs.length; i++) {
        const m = msgs[i];
        const type = m?.['@type'] ?? m?.type_url ?? '';

        // ✅ Detect unknown/undecoded messages (quarantine)
        const isUnknown = m?.['_raw'] || m?.['value_b64'];
        if (isUnknown) {
          unknownMsgsRows.push({
            tx_hash,
            msg_index: i,
            height,
            type_url: type,
            raw_value: m?.['_raw'] || m?.['value_b64'],
            signer: null  // Can't decode signer from unknown msg
          });
          log.warn(`[quarantine] Unknown message type: ${type} at height ${height} `);
          continue;  // Skip domain table processing for this message
        }

        msgRows.push({
          tx_hash, msg_index: i, height, type_url: type, value: m,
          signer: pickSigner(m),
        });

        const msgLog = logMap.get(i) || logMap.get(-1); // Fallback to flat log if per-msg missing



        // 🟢 WASM REGISTRY (STORE/INSTANTIATE) 🟢
        if (
          isSuccess &&
          (type.endsWith('.MsgStoreCode') ||
            type.endsWith('.MsgStoreAndInstantiateContract') ||
            type.endsWith('.MsgStoreAndMigrateContract'))
        ) {
          const event = msgLog?.events.find((e: any) => e.type === 'store_code');
          const attrs = attrsToPairs(event?.attributes);
          const codeId = findAttr(attrs, 'code_id');
          const checksum = findAttr(attrs, 'code_checksum') || findAttr(attrs, 'checksum') || m.checksum;

          if (codeId) {
            // Try to get instantiate_permission from message first, then fall back to event
            let instantiatePermission = m.instantiate_permission || m.instantiatePermission ||
              m.instantiate_config || m.instantiateConfig;

            // Fallback: extract from store_code event if not in message
            if (!instantiatePermission) {
              const permissionStr = findAttr(attrs, 'instantiate_permission') ||
                findAttr(attrs, 'access_config') ||
                findAttr(attrs, 'instantiate_config');
              if (permissionStr) {
                instantiatePermission = tryParseJson(permissionStr);
              }
            }

            wasmCodesRows.push({
              code_id: codeId,
              checksum: checksum || '', // Ensure not null
              creator: m.sender || m.authority || m.signer || firstSigner,
              instantiate_permission: instantiatePermission || { permission: 'Everybody' },
              store_tx_hash: tx_hash,
              store_height: height
            });
            if (!instantiatePermission) {
              log.debug(`[wasm - store] instantiate_permission is missing in ${tx_hash}. Keys present: ${Object.keys(m).join(', ')} `);
            }
          }

          // If it's a "StoreAndInstantiate", we also capture the contract creation if it succeeded
          if (type.endsWith('.MsgStoreAndInstantiateContract') && isSuccess) {
            const instEvent = msgLog?.events.find((e: any) => e.type === 'instantiate');
            const addr = findAttr(attrsToPairs(instEvent?.attributes), '_contract_address') || findAttr(attrsToPairs(instEvent?.attributes), 'contract_address');
            if (addr) {
              wasmContractsRows.push({
                address: addr,
                code_id: codeId,
                creator: m.authority || m.sender || firstSigner,
                admin: m.admin,
                label: m.label,
                created_height: height,
                created_tx_hash: tx_hash
              });
            }
          }
        }

        if (type.endsWith('.MsgUpdateInstantiateConfig') && isSuccess) {
          wasmInstantiateConfigsRows.push({
            code_id: m.code_id,
            instantiate_permission: m.new_instantiate_permission || m.newInstantiatePermission || m.instantiate_permission || m.instantiatePermission,
            height,
            tx_hash
          });
        }

        if (
          isSuccess &&
          (type.endsWith('.MsgInstantiateContract') ||
            type.endsWith('.MsgInstantiateContract2'))
        ) {
          const event = msgLog?.events.find((e: any) => e.type === 'instantiate');
          const addr = findAttr(attrsToPairs(event?.attributes), '_contract_address') || findAttr(attrsToPairs(event?.attributes), 'contract_address');
          if (addr) {
            wasmContractsRows.push({
              address: addr,
              code_id: m.code_id,
              creator: m.sender,
              admin: m.admin,
              label: m.label,
              created_height: height,
              created_tx_hash: tx_hash
            });
          }
        }

        if (type.endsWith('.MsgMigrateContract') && isSuccess) {
          // Extract from_code_id from migrate event (contains old code_id)
          const migrateEvent = msgLog?.events.find((e: any) => e.type === 'migrate');
          const migrateAttrs = attrsToPairs(migrateEvent?.attributes);
          const fromCodeId = findAttr(migrateAttrs, 'code_id') || findAttr(migrateAttrs, 'old_code_id');

          wasmMigrationsRows.push({
            contract: m.contract,
            from_code_id: fromCodeId ? toBigIntStr(fromCodeId) : null,
            to_code_id: m.code_id,
            height,
            tx_hash
          });
        }

        // 🟢 ZIGCHAIN LOGIC 🟢
        if (type.endsWith('.MsgCreateDenom') && isSuccess) {
          const event = msgLog?.events.find((e: any) => e.type === 'create_denom');
          const eventDenom = event ? normalizeFactoryDenom(findAttr(attrsToPairs(event.attributes), 'denom')) : null;
          const fallbackDenom = buildFactoryDenom(m.creator, m.sub_denom);
          const finalDenom = eventDenom || fallbackDenom;

          if (!finalDenom || !tx_hash) {
            log.warn(`[factory] MsgCreateDenom skipped (invalid denom or tx hash) at height ${height}`);
          } else {
            const parts = splitFactoryDenom(finalDenom);
            const creatorAddress = parts.creatorAddress || normalizeNonEmptyString(m.creator);
            if (!creatorAddress) {
              log.warn(`[factory] MsgCreateDenom skipped (missing creator) denom=${finalDenom} tx=${tx_hash}`);
            } else {
              const { uri, uri_hash } = sanitizeFactoryUriFields(
                m.URI ?? m.uri ?? m._u_r_i ?? null,
                m.URI_hash ?? m.URIHash ?? m.uri_hash ?? m._u_r_i_hash ?? null,
                { denom: finalDenom, txHash: tx_hash, msgType: 'MsgCreateDenom', height },
              );

              factoryDenomsRows.push({
                denom: finalDenom,
                creator_address: creatorAddress,
                sub_denom: parts.subDenom || normalizeNonEmptyString(m.sub_denom),
                minting_cap: normalizeUintAmount(m.minting_cap),
                uri,
                uri_hash,
                description: normalizeNonEmptyString(m.description),
                creation_tx_hash: tx_hash,
                block_height: height
              });

              // ✅ Register in Token Registry
              registerToken(finalDenom, 'factory', { creator: creatorAddress }, tx_hash as string | null);
            }
          }
        }

        if (type.endsWith('.MsgUpdateDenomURI') && isSuccess) {
          const denom = normalizeFactoryDenom(m.denom);
          if (!denom || !tx_hash) {
            log.warn(`[factory] MsgUpdateDenomURI skipped (invalid denom or tx hash) at height ${height}`);
          } else {
            const parts = splitFactoryDenom(denom);
            const creatorAddress = parts.creatorAddress;
            if (!creatorAddress) {
              log.warn(`[factory] MsgUpdateDenomURI skipped (cannot parse creator) denom=${denom} tx=${tx_hash}`);
            } else {
              const { uri, uri_hash } = sanitizeFactoryUriFields(
                m.URI ?? m.uri ?? m._u_r_i ?? null,
                m.URI_hash ?? m.URIHash ?? m.uri_hash ?? m._u_r_i_hash ?? null,
                { denom, txHash: tx_hash, msgType: 'MsgUpdateDenomURI', height },
              );

              if (!uri && !uri_hash) {
                log.warn(`[factory] MsgUpdateDenomURI ignored (no valid URI fields) denom=${denom} tx=${tx_hash}`);
              } else {
                // Upsert row to update URI/URI hash; inserter keeps existing non-null fields.
                factoryDenomsRows.push({
                  denom,
                  creator_address: creatorAddress,
                  sub_denom: parts.subDenom,
                  minting_cap: null,
                  uri,
                  uri_hash,
                  description: null,
                  creation_tx_hash: tx_hash,
                  block_height: height
                });
              }

              registerToken(denom, 'factory', { creator: creatorAddress }, tx_hash as string | null);
            }
          }
        }

        if (isSuccess && (type.endsWith('.MsgMintAndSendTokens') || type.endsWith('.MsgBurnTokens'))) {
          const denom = normalizeNonEmptyString(m.token?.denom || m.amount?.denom || m.denom);
          const amount = normalizeUintAmount(m.token?.amount || m.amount?.amount || m.amount);
          if (denom && amount && tx_hash) {
            registerToken(denom, undefined, {}, tx_hash);
            factorySupplyEventsRows.push({
              height,
              tx_hash,
              msg_index: i,
              event_index: -1,
              denom,
              action: type.endsWith('.MsgMintAndSendTokens') ? 'mint' : 'burn',
              amount,
              sender: m.signer || m.sender || m.creator,
              recipient: m.recipient || null,
              metadata: null
            });
          }
        }

        if (type.endsWith('.MsgSetDenomMetadata') && isSuccess) {
          if (m.metadata?.base) {
            registerToken(m.metadata.base, 'factory', { full_meta: m.metadata }, tx_hash);
            factorySupplyEventsRows.push({
              height,
              tx_hash,
              msg_index: i,
              event_index: -1,
              denom: m.metadata.base,
              action: 'set_metadata',
              amount: null,
              sender: m.signer || m.sender || m.creator,
              recipient: null,
              metadata: m.metadata
            });
          }
        }

        if (isSuccess && (type.endsWith('.MsgFundModuleWallet') || type.endsWith('.MsgWithdrawFromModuleWallet'))) {
          const coins = Array.isArray(m.amount) ? m.amount : (m.amount ? [m.amount] : []);
          const wrapperSender = normalizeNonEmptyString(m.signer || m.sender || firstSigner) || 'unknown';
          for (let wi = 0; wi < coins.length; wi++) {
            const coin = coins[wi];
            wrapperEventsRows.push({
              height,
              tx_hash,
              msg_index: i,
              event_index: wi,
              sender: wrapperSender,
              action: type.endsWith('.MsgFundModuleWallet') ? 'fund_module' : 'withdraw_module',
              amount: coin?.amount || null,
              denom: coin?.denom || null,
              metadata: null
            });
          }
        }

        if (type.endsWith('.MsgUpdateIbcSettings') && isSuccess) {
          const normalizedWrapper = normalizeWrapperIbcSettingsMessage(m, {
            height,
            txHash: tx_hash,
            msgIndex: i,
          });
          wrapperEventsRows.push({
            height,
            tx_hash,
            msg_index: i,
            event_index: -1,
            sender: normalizeNonEmptyString(m.sender || m.signer || firstSigner) || 'unknown',
            action: 'update_ibc_settings',
            amount: null,
            denom: normalizedWrapper.denom,
            metadata: {
              native_client_id: normalizedWrapper.native_client_id,
              counterparty_client_id: normalizedWrapper.counterparty_client_id,
              native_port: normalizedWrapper.native_port,
              counterparty_port: normalizedWrapper.counterparty_port,
              native_channel: normalizedWrapper.native_channel,
              counterparty_channel: normalizedWrapper.counterparty_channel,
              decimal_difference: normalizedWrapper.decimal_difference,
              legacy_shift_mapped: normalizedWrapper.legacy_shift_mapped,
            }
          });

          if (!normalizedWrapper.denom) {
            log.warn(
              `[wrapper] MsgUpdateIbcSettings skipped for wrapper_settings (missing denom): height=${height} tx=${tx_hash ?? 'unknown'} msg_index=${i}`,
            );
          } else {
            wrapperSettingsRows.push({
              denom: normalizedWrapper.denom,
              native_client_id: normalizedWrapper.native_client_id,
              counterparty_client_id: normalizedWrapper.counterparty_client_id,
              native_port: normalizedWrapper.native_port,
              counterparty_port: normalizedWrapper.counterparty_port,
              native_channel: normalizedWrapper.native_channel,
              counterparty_channel: normalizedWrapper.counterparty_channel,
              decimal_difference: normalizedWrapper.decimal_difference,
              updated_at_height: height
            });
            registerToken(normalizedWrapper.denom, undefined, {}, tx_hash, 'wrapper_settings');
          }
        }

        if (type.endsWith('.MsgCreatePool') && isSuccess) {
          let poolId = null;
          let lpToken = null;
          let pairId = null;
          let baseReserve = null;
          let quoteReserve = null;

          if (msgLog) {
            for (const e of msgLog.events) {
              const pairs = attrsToPairs(e.attributes);
              const pid = findAttr(pairs, 'pool_id');
              if (pid) poolId = pid;
              const lpt = findAttr(pairs, 'lp_token') || findAttr(pairs, 'lptoken_denom');
              if (lpt) lpToken = lpt;
              // ✅ Extract pair_id if present
              const pId = findAttr(pairs, 'pair_id');
              if (pId) pairId = pId;
            }
          }

          // Initial reserves from the message (create pool deposits initial liquidity)
          baseReserve = m.base?.amount || '0';
          quoteReserve = m.quote?.amount || '0';

          if (poolId) {
            // Derive pair_id from base/quote denoms if not found in events
            if (!pairId && m.base?.denom && m.quote?.denom) {
              const sorted = [String(m.base.denom), String(m.quote.denom)].sort();
              pairId = sorted.join('-');
            }

            // Derive lp_token_denom from pool_id if not found in events
            if (!lpToken && poolId) {
              lpToken = poolId;
            }

            dexPoolsRows.push({
              pool_id: poolId,
              creator_address: m.creator,
              pair_id: pairId,
              base_denom: m.base?.denom,
              quote_denom: m.quote?.denom,
              lp_token_denom: lpToken,
              base_reserve: baseReserve,
              quote_reserve: quoteReserve,
              block_height: height,
              tx_hash: tx_hash
            });
            registerToken(m.base?.denom, undefined, {}, tx_hash);
            registerToken(m.quote?.denom, undefined, {}, tx_hash);
            registerToken(lpToken, undefined, {}, tx_hash);
          }
        }

        // ✅ REDUNDANT: Native MsgSwap is now handled via 'token_swapped' events in the event loop for better accuracy (analytics parity).
        /*
        if (type.endsWith('.MsgSwapExactIn') || type.endsWith('.MsgSwapExactOut') || type.endsWith('.MsgSwap')) {
          let priceImpact = null;
          // ... (moved to event-based indexing)
        }
        */

        if (isSuccess && (type.endsWith('.MsgAddLiquidity') || type.endsWith('.MsgRemoveLiquidity'))) {
          let poolId = m.pool_id;
          let evLpAmount: string | null = null;
          let evBaseAmount: string | null = null;
          let evQuoteAmount: string | null = null;

          if (msgLog) {
            for (const e of msgLog.events) {
              const pairs = attrsToPairs(e.attributes);
              const pid = findAttr(pairs, 'pool_id');
              if (pid) poolId = poolId || pid;

              // Extract LP token amount from events (covers both add/remove)
              const lp = findAttr(pairs, 'lp_token') || findAttr(pairs, 'lptoken') || findAttr(pairs, 'lptoken_amount');
              if (lp) evLpAmount = evLpAmount || lp;

              // Extract base/quote amounts from response events (for remove liquidity)
              const ba = findAttr(pairs, 'base_amount') || findAttr(pairs, 'actual_base') || findAttr(pairs, 'amount_0');
              if (ba) evBaseAmount = evBaseAmount || ba;
              const qa = findAttr(pairs, 'quote_amount') || findAttr(pairs, 'actual_quote') || findAttr(pairs, 'amount_1');
              if (qa) evQuoteAmount = evQuoteAmount || qa;
            }
          }

          // For RemoveLiquidity, pool_id is not in the request msg — derive from lptoken denom
          if (!poolId && m.lptoken?.denom) {
            // LP token denom IS the pool_id on zigchain
            poolId = String(m.lptoken.denom);
          }

          if (poolId) {
            const isAdd = type.includes('Add');
            dexLiquidityRows.push({
              tx_hash,
              msg_index: i,
              pool_id: poolId,
              sender_address: m.creator || m.signer || firstSigner,
              action_type: isAdd ? 'ADD' : 'REMOVE',
              amount_0: m.base?.amount || evBaseAmount || null,
              amount_1: m.quote?.amount || evQuoteAmount || null,
              shares_minted_burned: m.lptoken?.amount || evLpAmount || null,
              block_height: height
            });
            registerToken(m.base?.denom, undefined, {}, tx_hash);
            registerToken(m.quote?.denom, undefined, {}, tx_hash);
            registerToken(m.lptoken?.denom || poolId, undefined, {}, tx_hash);
          }
        }

        // 🟢 WASM LOGIC 🟢
        if (type.endsWith('.MsgExecuteContract')) {
          wasmExecRows.push({
            tx_hash, msg_index: i, contract: m?.contract ?? m?.contract_address, caller: m?.sender,
            funds: m?.funds, msg: tryParseJson(m?.msg), success: code === 0, error: code === 0 ? null : (log_summary),
            gas_used, height
          });
        }

        // 🟢 WASM ADMIN CHANGES (Security Auditing) 🟢
        if (type.endsWith('.MsgUpdateAdmin') && code === 0) {
          wasmAdminChangesRows.push({
            contract: m.contract,
            height,
            tx_hash,
            msg_index: i,
            old_admin: null, // Would need state query to get old admin
            new_admin: m.new_admin,
            action: 'update'
          });
        }

        if (type.endsWith('.MsgClearAdmin') && code === 0) {
          wasmAdminChangesRows.push({
            contract: m.contract,
            height,
            tx_hash,
            msg_index: i,
            old_admin: null, // Would need state query to get old admin
            new_admin: null,
            action: 'clear'
          });
        }

        // 🟢 WASM SUDO (Governance-initiated execution) 🟢
        if (type.endsWith('.MsgSudoContract')) {
          wasmExecRows.push({
            tx_hash, msg_index: i, contract: m?.contract ?? m?.contract_address,
            caller: 'governance', // Sudo is always governance-initiated
            funds: null, msg: tryParseJson(m?.msg), success: code === 0,
            error: code === 0 ? null : (log_summary),
            gas_used, height
          });
        }

        // 🟢 IBC MSG_TRANSFER INTENT (OUTGOING) 🟢
        if (type === '/ibc.applications.transfer.v1.MsgTransfer' && isSuccess) {
          txIbcIntents.push({
            msg_index: i,
            port: m.source_port,
            channel: m.source_channel,
            sender: m.sender,
            receiver: m.receiver,
            denom: m.token?.denom,
            amount: m.token?.amount,
            memo: m.memo ?? null
          });
        }

        // 🟢 IBC CHANNEL OPEN INTENT — Extract ordering from protobuf message body 🟢
        if (
          (type === '/ibc.core.channel.v1.MsgChannelOpenInit' || type === '/ibc.core.channel.v1.MsgChannelOpenTry') &&
          isSuccess
        ) {
          const ch = m.channel ?? m;
          const rawOrdering = ch?.ordering ?? ch?.order;
          let ordering: string | null = null;
          if (typeof rawOrdering === 'string') {
            // Normalize proto enum strings: "ORDER_ORDERED" → "ORDER_ORDERED"
            ordering = rawOrdering;
          } else if (typeof rawOrdering === 'number') {
            // Proto enum: 0=NONE, 1=UNORDERED, 2=ORDERED
            const orderMap: Record<number, string> = { 0: 'ORDER_NONE_UNSPECIFIED', 1: 'ORDER_UNORDERED', 2: 'ORDER_ORDERED' };
            ordering = orderMap[rawOrdering] ?? `ORDER_${rawOrdering}`;
          }
          if (ordering) {
            txChannelIntents.push({
              port: m.port_id ?? m.portId ?? ch?.port_id ?? '',
              channel: undefined, // channel_id assigned after channel_open_init event
              ordering
            });
          }
        }

        // 🟢 IBC CLIENT CREATION - Extract chain_id from client_state 🟢
        if (type === '/ibc.core.client.v1.MsgCreateClient' && isSuccess) {
          const clientState = m.client_state || m.clientState;
          let chainId = clientState?.chain_id || clientState?.chainId;

          // If client_state is encoded (has 'value' field with base64), try to extract chain_id
          if (!chainId && clientState?.value && typeof clientState.value === 'string') {
            try {
              // Decode base64 and extract chain_id (first field in tendermint ClientState protobuf)
              const decoded = Buffer.from(clientState.value, 'base64');
              // Chain_id is typically the first string field in protobuf (field 1, wire type 2)
              // The format is: 0x0a (tag for field 1, length-delimited), length, string bytes
              if (decoded[0] === 0x0a && decoded.length > 2) {
                const strLen = decoded[1];
                if (strLen > 0 && strLen < decoded.length - 2) {
                  chainId = decoded.slice(2, 2 + strLen).toString('utf-8');
                }
              }
            } catch {
              // Ignore decoding errors
            }
          }

          // Get client_id from events
          const createClientEvent = msgLog?.events.find((e: any) => e.type === 'create_client');
          const clientId = createClientEvent
            ? findAttr(attrsToPairs(createClientEvent.attributes), 'client_id')
            : null;

          if (clientId) {
            ibcClientsRows.push({
              client_id: clientId,
              chain_id: chainId,
              client_type: clientState?.type_url?.split('.').pop()?.toLowerCase() ||
                clientState?.['@type']?.split('.').pop()?.toLowerCase() || 'tendermint',
              updated_at_height: height,
              updated_at_time: !isNaN(time.getTime()) ? time : new Date()
            });
          }
        }

        // 🟢 MSG_UPDATE_PARAMS (GENERIC) 🟢
        if (type.endsWith('.MsgUpdateParams') && isSuccess) {
          // Extract the actual module name (e.g. 'staking' from '/cosmos.staking.v1beta1.MsgUpdateParams')
          const moduleMatch = type.match(/\.([^.]+)\.v\d/);
          const module = moduleMatch ? moduleMatch[1] : 'unknown';
          const params = m.params || {};

          networkParamsRows.push({
            height,
            time,
            module,
            param_key: '_all', // Modern SDK updates usually replace the whole params object
            old_value: null,
            new_value: params
          });
        }
      }

      // --- PROCESS LOGS (Events) ---
      for (const l of logs) {
        const msg_index = Number(l?.msg_index ?? -1);
        const events = normArray<any>(l?.events);

        // 🔄 Pre-scan message events for IBC metadata and general sender
        let msgIbcMeta: any = {};
        let msgSender: string | null = null;
        for (const ev of events) {
          const ap = attrsToPairs(ev.attributes);
          if (ev.type === 'message') {
            msgSender = findAttr(ap, 'sender');
          }
          if (ev.type === 'fungible_token_packet') {
            msgIbcMeta.sender = findAttr(ap, 'sender');
            msgIbcMeta.receiver = findAttr(ap, 'receiver');
            msgIbcMeta.amount = findAttr(ap, 'amount');
            msgIbcMeta.denom = findAttr(ap, 'denom');
          }
        }

        for (let ei = 0; ei < events.length; ei++) {
          const ev = events[ei];
          const event_type = String(ev?.type ?? 'unknown');
          const attrsPairs = attrsToPairs(ev?.attributes);

          evRows.push({
            tx_hash, msg_index, event_index: ei, event_type,
            attributes: attrsPairs, height
          });

          const contract = findAttr(attrsPairs, 'contract_address') || findAttr(attrsPairs, '_contract_address');
          const action = findAttr(attrsPairs, 'action');
          const method = findAttr(attrsPairs, 'method');

          if (event_type === 'wasm') {
            if (contract) {
              wasmEventsRows.push({
                contract, height, tx_hash, msg_index, event_index: ei, event_type, attributes: attrsPairs
              });
              for (let ai = 0; ai < attrsPairs.length; ai++) {
                const attr = attrsPairs[ai];
                if (!attr) continue;
                wasmEventAttrsRows.push({
                  contract,
                  height,
                  tx_hash,
                  msg_index,
                  event_index: ei,
                  attr_index: ai,
                  key: attr.key,
                  value: attr.value
                });
              }

              // ✅ ENHANCED CW20 DETECTION (Multiple patterns)
              // Pattern 1: action=transfer/send (standard CW20)
              // Pattern 2: method=transfer/send (some CW20 variants)
              // Pattern 3: Heuristic - from/to/amount without action (non-standard)
              const hasFromTo = findAttr(attrsPairs, 'from') && findAttr(attrsPairs, 'to');
              const isCw20Transfer =
                action === 'transfer' || action === 'send' ||
                action === 'transfer_from' || action === 'send_from' ||
                method === 'transfer' || method === 'send' ||
                (!action && !method && hasFromTo && findAttr(attrsPairs, 'amount'));


              if (isSuccess && isCw20Transfer) {
                const fromAddr =
                  findAttr(attrsPairs, 'sender') ||
                  findAttr(attrsPairs, 'from') ||
                  findAttr(attrsPairs, 'from_address') ||
                  findAttr(attrsPairs, 'owner'); // For transfer_from
                const toAddr =
                  findAttr(attrsPairs, 'recipient') ||
                  findAttr(attrsPairs, 'to') ||
                  findAttr(attrsPairs, 'to_address');
                const amtRaw = findAttr(attrsPairs, 'amount');
                const amtCoin = amtRaw ? parseCoin(amtRaw) : null;
                const amount = (amtRaw && /^\d+$/.test(amtRaw)) ? amtRaw : amtCoin?.amount ?? null;
                if (fromAddr && toAddr && amount) {
                  cw20TransfersRows.push({
                    contract,
                    from_addr: fromAddr,
                    to_addr: toAddr,
                    amount,
                    height,
                    tx_hash
                  });
                  // Register CW20 token contracts once they are observed emitting transfer semantics.
                  if (contract) {
                    registerToken(contract, 'cw20', {}, tx_hash);
                  }
                }
              }

            }
          }

          // ✅ WASM & NATIVE DEX SWAP EXTRACTION
          const isWasmSwap = event_type === 'wasm' && (action === 'swap' || method === 'swap' || (method && method.startsWith('swap_')));
          const isNativeSwap = event_type === 'token_swapped';

          if (isWasmSwap || isNativeSwap) {
            let sender = findAttr(attrsPairs, 'sender');
            let receiver = findAttr(attrsPairs, 'receiver') || sender;
            let offerAsset = findAttr(attrsPairs, 'offer_asset');
            let askAsset = findAttr(attrsPairs, 'ask_asset');
            let offerAmount = findAttr(attrsPairs, 'offer_amount');
            let returnAmount = findAttr(attrsPairs, 'return_amount');
            let spreadAmount = findAttr(attrsPairs, 'spread_amount');
            let commissionAmount = findAttr(attrsPairs, 'commission_amount');
            let makerFeeAmount = findAttr(attrsPairs, 'maker_fee_amount');
            let feeShareAmount = findAttr(attrsPairs, 'fee_share_amount');
            let reservesRaw = findAttr(attrsPairs, 'reserves') || findAttr(attrsPairs, 'pool_snapshot');
            let contractAddr = contract; // Default to the event's contract

            // Native Zigchain token_swapped event mapping
            if (isNativeSwap) {
              contractAddr = findAttr(attrsPairs, 'pool_id') || 'dex';
              const tIn = findAttr(attrsPairs, 'token_in');
              const tOut = findAttr(attrsPairs, 'token_out');
              const tFee = findAttr(attrsPairs, 'swap_fee');

              // Extract from combined formats (e.g. "100uzig")
              if (tIn) {
                const c = parseCoin(tIn);
                offerAsset = c?.denom || offerAsset;
                offerAmount = c?.amount || offerAmount;
              }
              if (tOut) {
                const c = parseCoin(tOut);
                askAsset = c?.denom || askAsset;
                returnAmount = c?.amount || returnAmount;
              }
              if (tFee) {
                const c = parseCoin(tFee);
                commissionAmount = c?.amount || commissionAmount;
              }
            }

            let reserves = null;
            if (reservesRaw) {
              // Handle both "amountDenom" and "denom:amount" formats
              const obj: Record<string, string> = {};
              const parts = reservesRaw.split(',');
              for (const p of parts) {
                const trimmed = p.trim();
                if (!trimmed) continue;
                // Try denom:amount
                const colonIdx = trimmed.lastIndexOf(':');
                if (colonIdx > 0) {
                  const d = trimmed.slice(0, colonIdx);
                  const a = trimmed.slice(colonIdx + 1).replace(/\D/g, ''); // sanitize amount
                  if (d && a) obj[d] = a;
                } else {
                  // fallback to standard parseCoin
                  const c = parseCoin(trimmed);
                  if (c) obj[c.denom] = c.amount;
                }
              }
              reserves = Object.keys(obj).length > 0 ? JSON.stringify(obj) : JSON.stringify({ raw: reservesRaw });
            }

            if (sender && (offerAmount || isNativeSwap)) {

              // ✅ Compute analytics columns with NaN validation
              const offerNum = parseFloat(offerAmount || '0');
              const returnNum = parseFloat(returnAmount || '0');
              const spreadNum = parseFloat(spreadAmount || '0');
              const commissionNum = parseFloat(commissionAmount || '0');
              const makerFeeNum = parseFloat(makerFeeAmount || '0');
              const feeShareNum = parseFloat(feeShareAmount || '0');

              // Generate sorted pair_id for consistent pair identification
              const pairId = offerAsset && askAsset
                ? [offerAsset, askAsset].sort().join('-')
                : null;

              // Effective price: return_amount / offer_amount (with NaN check)
              const effectivePrice = offerNum > 0 && !isNaN(returnNum)
                ? returnNum / offerNum
                : null;

              // Price impact (slippage %): (spread_amount / offer_amount) * 100 (with NaN check)
              const priceImpact = offerNum > 0 && !isNaN(spreadNum)
                ? (spreadNum / offerNum) * 100
                : null;

              // Total fee: sum of all fees (with NaN check)
              const feeSum = (isNaN(commissionNum) ? 0 : commissionNum) +
                (isNaN(makerFeeNum) ? 0 : makerFeeNum) +
                (isNaN(feeShareNum) ? 0 : feeShareNum);
              const totalFee = feeSum > 0 ? feeSum.toString() : '0';

              const swapRow = {
                tx_hash,
                msg_index,
                event_index: ei,
                contract: contractAddr,
                sender,
                receiver,
                offer_asset: offerAsset,
                ask_asset: askAsset,
                offer_amount: toBigIntStr(offerAmount),
                return_amount: toBigIntStr(returnAmount),
                spread_amount: toBigIntStr(spreadAmount),
                commission_amount: toBigIntStr(commissionAmount),
                maker_fee_amount: toBigIntStr(makerFeeAmount),
                fee_share_amount: toBigIntStr(feeShareAmount),
                reserves,
                pair_id: pairId,
                effective_price: effectivePrice,
                price_impact: priceImpact,
                total_fee: totalFee,
                block_height: height,
                timestamp: time
              };

              if (isNativeSwap) {
                // Route Native Zigchain Swaps to zigchain schema
                dexSwapsRows.push({
                  tx_hash: swapRow.tx_hash,
                  msg_index: swapRow.msg_index,
                  event_index: swapRow.event_index,
                  pool_id: swapRow.contract, // For native, contractAddr is the pool_id
                  sender_address: swapRow.sender,
                  token_in_denom: swapRow.offer_asset,
                  token_in_amount: swapRow.offer_amount,
                  token_out_denom: swapRow.ask_asset,
                  token_out_amount: swapRow.return_amount,
                  pair_id: swapRow.pair_id,
                  effective_price: swapRow.effective_price,
                  total_fee: swapRow.total_fee,
                  price_impact: swapRow.price_impact ? swapRow.price_impact.toString() : null,
                  block_height: swapRow.block_height,
                  timestamp: swapRow.timestamp
                });
              } else {
                // Route Smart Contract Swaps to wasm schema
                wasmSwapsRows.push(swapRow);
              }
            }


            // ✅ Register assets discovered in swaps in Universal Token Registry
            for (const denom of [offerAsset, askAsset]) {
              if (denom) {
                const cleanDenom = String(denom).trim();
                const lowerDenom = cleanDenom.toLowerCase();
                const isFactory = lowerDenom.startsWith('factory/') || lowerDenom.startsWith('coin.zig');
                const isCw20 = cleanDenom.startsWith('zig1') && !cleanDenom.includes('/');

                registerToken(cleanDenom, undefined, {
                  creator: isCw20 ? cleanDenom : (isFactory ? (cleanDenom.includes('/') ? cleanDenom.split('/')[1] : cleanDenom.split('.')[1]) : null)
                }, tx_hash);
              }
            }
          }


          if (event_type === 'instantiate' || event_type === '_instantiate') {
            const addr = findAttr(attrsPairs, '_contract_address') || findAttr(attrsPairs, 'contract_address');
            const codeId = findAttr(attrsPairs, 'code_id');
            if (addr && codeId) {
              // ✅ ENHANCEMENT: Fallbacks for creator, admin, label (vital for internal calls)
              const creator = findAttr(attrsPairs, 'creator') || findAttr(attrsPairs, '_contract_creator') || findAttr(attrsPairs, '_creator') || msgSender;
              const admin = findAttr(attrsPairs, 'admin') || findAttr(attrsPairs, '_contract_admin') || findAttr(attrsPairs, '_admin');
              const label = findAttr(attrsPairs, 'label') || findAttr(attrsPairs, '_contract_label') || findAttr(attrsPairs, '_label');

              wasmContractsRows.push({
                address: addr,
                code_id: toBigIntStr(codeId),
                creator,
                admin,
                label,
                created_height: height,
                created_tx_hash: tx_hash
              });
            }
          }

          if (isSuccess && event_type === 'transfer') {
            const sender = pickFirstNonEmptyAttr(attrsPairs, ['sender', 'from', 'from_address']);
            const recipient = pickFirstNonEmptyAttr(attrsPairs, ['recipient', 'receiver', 'to', 'to_address']);
            const amountStr = findAttr(attrsPairs, 'amount');
            const coins = parseCoins(amountStr);
            if (!sender || !recipient) {
              droppedTransferRows += coins.length > 0 ? coins.length : 1;
              continue;
            }
            for (const coin of coins) {
              if (!normalizeNonEmptyString(coin?.denom) || !/^\d+$/.test(String(coin?.amount ?? ''))) {
                droppedTransferRows += 1;
                continue;
              }
              registerToken(coin.denom, undefined, {}, tx_hash);
              transfersRows.push({
                tx_hash, msg_index, event_index: ei, from_addr: sender, to_addr: recipient,
                denom: coin.denom, amount: coin.amount, height
              });
            }
          }

          // 🟢 IBC LOGIC 🟢
          if (['send_packet', 'recv_packet', 'acknowledge_packet', 'timeout_packet'].includes(event_type)) {
            const sequenceStr = findAttr(attrsPairs, 'packet_sequence');
            if (!sequenceStr || !/^\d+$/.test(sequenceStr)) {
              continue;
            }
            const sequence = toBigIntStr(sequenceStr);
            const srcPort = findAttr(attrsPairs, 'packet_src_port');
            const srcChan = findAttr(attrsPairs, 'packet_src_channel');
            let dstPort = findAttr(attrsPairs, 'packet_dst_port');
            let dstChan = findAttr(attrsPairs, 'packet_dst_channel');

            // 🛡️ Fallback: If dst missing, try to find it in previous packets for this channel? No, stateless.
            // Just warn if it's a send_packet
            if (event_type === 'send_packet' && (!dstPort || !dstChan)) {
              // Try identifying from raw packet_data if possible, but for now just warn
              log.warn(`[ibc] send_packet missing dst info: ${srcPort}/${srcChan} seq=${sequence}`);
            }

            // ✅ FIX: Extract timeout fields
            const timeoutHeightStr = findAttr(attrsPairs, 'packet_timeout_height');
            const timeoutTsStr = findAttr(attrsPairs, 'packet_timeout_timestamp');

            if (sequence && srcChan) {
              // ✅ FIX: Try packet_data first, then packet_data_hex (hex-encoded)
              const dataJson = findAttr(attrsPairs, 'packet_data');
              const dataHex = findAttr(attrsPairs, 'packet_data_hex');
              let amount: string | null = null;
              let denom: string | null = null;
              let relayer: string | null = null;
              let memo: string | null = null;
              let sender: string | null = null;
              let receiver: string | null = null;

              try {
                // Try packet_data first (plain JSON), then packet_data_hex (hex-encoded)
                const d = dataJson ? tryParseJson(dataJson) : (dataHex ? decodeHexToJson(dataHex) : null);
                if (d) {
                  amount = d.amount ?? d.amt ?? msgIbcMeta.amount ?? null;
                  denom = d.denom ?? d.den ?? msgIbcMeta.denom ?? null;
                  sender = d.sender ?? d.snd ?? msgIbcMeta.sender ?? null;
                  receiver = d.receiver ?? d.rcv ?? msgIbcMeta.receiver ?? null;
                  memo = d.memo ?? null;
                }
              } catch { /* ignore parse error */ }

              // Fallback to fungible_token_packet event data if not extracted
              if (!sender) sender = msgIbcMeta.sender ?? null;
              if (!receiver) receiver = msgIbcMeta.receiver ?? null;
              if (!amount) amount = msgIbcMeta.amount ?? null;
              if (!denom) denom = msgIbcMeta.denom ?? null;

              // 🛡️ ENHANCEMENT: Link with MsgTransfer intents if fields are missing (Outgoing)
              if (event_type === 'send_packet') {
                const intent = txIbcIntents.find((it: any) => it.port === srcPort && it.channel === srcChan && !it.linked);
                if (intent) {
                  intent.linked = true; // Avoid double-link for multi-msg tx
                  amount = amount || intent.amount;
                  denom = denom || intent.denom;
                  sender = sender || intent.sender;
                  receiver = receiver || intent.receiver;
                  memo = memo || intent.memo;
                }
              }

              if (!relayer) {
                const ackRelayer = findAttr(attrsPairs, 'packet_ack_relayer') || findAttr(attrsPairs, 'relayer');
                if (event_type === 'send_packet') {
                  relayer = sender || findAttr(attrsPairs, 'packet_sender') || findAttr(attrsPairs, 'sender');
                } else {
                  // For recv, ack, timeout: the transaction signer is the relayer
                  relayer = firstSigner || ackRelayer;
                }
              }

              const statusMap: Record<string, string> = {
                send_packet: 'sent',
                recv_packet: 'received',
                acknowledge_packet: 'acknowledged',
                timeout_packet: 'timeout'
              };
              const status = statusMap[event_type] || 'failed';

              const safeTime = !isNaN(time.getTime()) ? time : new Date();

              // ✅ FIX: Improved Ack Parsing (Handle non-JSON strings correctly)
              let ackSuccess: boolean | null = null;
              let ackError: string | null = null;
              if (event_type === 'acknowledge_packet') {
                const ackHex = findAttr(attrsPairs, 'packet_ack');
                if (ackHex) {
                  const ackJson = tryParseJson(ackHex);
                  if (ackJson && typeof ackJson === 'object' && !Array.isArray(ackJson)) {
                    ackSuccess = !!ackJson.result;
                    ackError = ackJson.error || null;
                  } else {
                    // Fallback for strings (e.g. "error: code 123")
                    // tryParseJson returns the string itself if it fails to parse as JSON
                    const ackStr = typeof ackJson === 'string' ? ackJson : ackHex;
                    if (ackStr.includes('result')) ackSuccess = true;
                    else if (ackStr.includes('error') || ackStr.includes('failed')) {
                      ackSuccess = false;
                      ackError = ackStr;
                    }
                  }
                }
              }

              // Build packet row with event-specific columns
              const packetRow: any = {
                port_id_src: srcPort,
                channel_id_src: srcChan,
                sequence: sequence, // ✅ FIX: Use string/BigInt directly, avoid Number() precision loss
                port_id_dst: dstPort,
                channel_id_dst: dstChan,
                timeout_height: timeoutHeightStr ?? null,
                timeout_ts: timeoutTsStr ?? null,
                status,
                denom,
                amount,
                sender,
                receiver,
                memo,
                // Event-specific fields
                tx_hash_send: null, height_send: null, time_send: null, relayer_send: null,
                tx_hash_recv: null, height_recv: null, time_recv: null, relayer_recv: null,
                tx_hash_ack: null, height_ack: null, time_ack: null, relayer_ack: null, ack_success: null, ack_error: null,
                tx_hash_timeout: null, height_timeout: null, time_timeout: null
              };

              // Populate event-specific fields
              if (event_type === 'send_packet') {
                packetRow.tx_hash_send = tx_hash;
                packetRow.height_send = height;
                packetRow.time_send = safeTime; // ✅ FIX: Use safeTime
                packetRow.relayer_send = relayer;
              } else if (event_type === 'recv_packet') {
                packetRow.tx_hash_recv = tx_hash;
                packetRow.height_recv = height;
                packetRow.time_recv = safeTime; // ✅ FIX: Use safeTime
                packetRow.relayer_recv = relayer;
              } else if (event_type === 'acknowledge_packet') {
                packetRow.tx_hash_ack = tx_hash;
                packetRow.height_ack = height;
                packetRow.time_ack = safeTime; // ✅ FIX: Use safeTime
                packetRow.relayer_ack = relayer;
                packetRow.ack_success = ackSuccess;
                packetRow.ack_error = ackError;
              } else if (event_type === 'timeout_packet') {
                packetRow.tx_hash_timeout = tx_hash;
                packetRow.height_timeout = height;
                packetRow.time_timeout = safeTime; // ✅ FIX: Use safeTime
              }

              ibcPacketsRows.push(packetRow);

              // ✅ Register in Token Registry
              if (denom) {
                registerToken(denom, 'ibc', {}, tx_hash);
              }

              // ✅ FIX: Always record transfers for ALL lifecycle events (not just when denom/amount present)
              // This ensures ack/timeout statuses are properly recorded
              const transferRow: any = {
                port_id_src: srcPort,
                channel_id_src: srcChan,
                sequence: sequence, // ✅ FIX: Use string/BigInt directly
                port_id_dst: dstPort,
                channel_id_dst: dstChan,
                sender,
                receiver,
                denom,
                amount,
                memo,
                timeout_height: timeoutHeightStr ?? null,
                timeout_ts: timeoutTsStr ?? null,
                status,
                tx_hash_send: null, height_send: null, time_send: null, relayer_send: null,
                tx_hash_recv: null, height_recv: null, time_recv: null, relayer_recv: null,
                tx_hash_ack: null, height_ack: null, time_ack: null, relayer_ack: null, ack_success: null, ack_error: null,
                tx_hash_timeout: null, height_timeout: null, time_timeout: null
              };

              if (event_type === 'send_packet') {
                transferRow.tx_hash_send = tx_hash;
                transferRow.height_send = height;
                transferRow.time_send = safeTime; // ✅ FIX
                transferRow.relayer_send = relayer;
              } else if (event_type === 'recv_packet') {
                transferRow.tx_hash_recv = tx_hash;
                transferRow.height_recv = height;
                transferRow.time_recv = safeTime; // ✅ FIX
                transferRow.relayer_recv = relayer;
              } else if (event_type === 'acknowledge_packet') {
                transferRow.tx_hash_ack = tx_hash;
                transferRow.height_ack = height;
                transferRow.time_ack = safeTime; // ✅ FIX
                transferRow.relayer_ack = relayer;
                transferRow.ack_success = ackSuccess;
                transferRow.ack_error = ackError;
              } else if (event_type === 'timeout_packet') {
                transferRow.tx_hash_timeout = tx_hash;
                transferRow.height_timeout = height;
                transferRow.time_timeout = safeTime; // ✅ FIX
              }

              ibcTransfersRows.push(transferRow);
            }
          }

          if (event_type.startsWith('channel_open_') || event_type.startsWith('channel_close_')) {
            const portId = findAttr(attrsPairs, 'port_id') || findAttr(attrsPairs, 'packet_src_port');
            const channelId = findAttr(attrsPairs, 'channel_id') || findAttr(attrsPairs, 'packet_src_channel');
            if (portId && channelId) {
              const connectionId = findAttr(attrsPairs, 'connection_id');
              ibcChannelsRows.push({
                port_id: portId,
                channel_id: channelId,
                state: event_type,
                ordering: findAttr(attrsPairs, 'channel_ordering') || findAttr(attrsPairs, 'ordering')
                  || txChannelIntents.find(ci => ci.port === portId)?.ordering
                  || null,
                connection_hops: connectionId ? [connectionId] : null,
                counterparty_port: findAttr(attrsPairs, 'counterparty_port_id') || findAttr(attrsPairs, 'counterparty_port'),
                counterparty_channel: findAttr(attrsPairs, 'counterparty_channel_id') || findAttr(attrsPairs, 'counterparty_channel'),
                version: findAttr(attrsPairs, 'version')
              });
            }
          }

          // 🟢 IBC CONNECTIONS 🟢
          if (event_type.startsWith('connection_open_')) {
            const connId = findAttr(attrsPairs, 'connection_id');
            const clientId = findAttr(attrsPairs, 'client_id');
            if (connId && clientId) {
              ibcConnectionsRows.push({
                connection_id: connId,
                client_id: clientId,
                counterparty_connection_id: findAttr(attrsPairs, 'counterparty_connection_id'),
                counterparty_client_id: findAttr(attrsPairs, 'counterparty_client_id'),
                state: event_type
              });
            }
          }


          // 🟢 IBC DISCOVERY (CLIENTS & DENOMS) 🟢
          // ✅ FIX: Handle both denom_trace and denomination events
          if (event_type === 'denom_trace' || event_type === 'denomination') {
            // Try denom_trace format first (hash + denom_trace path)
            let hash = findAttr(attrsPairs, 'hash') || findAttr(attrsPairs, 'denom_hash');
            let fullPath: string | null = null;
            let baseDenom: string | null = null;

            const denomTraceAttr = findAttr(attrsPairs, 'denom_trace');
            const denomAttr = findAttr(attrsPairs, 'denom');

            if (denomTraceAttr) {
              // Old format: denom_trace = "transfer/channel-0/uatom"
              fullPath = denomTraceAttr;
              baseDenom = denomTraceAttr.split('/').pop() || denomTraceAttr;
            } else if (denomAttr) {
              // New format: denom = {"base":"uaxl","trace":[{"port_id":"transfer","channel_id":"channel-0"}]}
              try {
                const d = tryParseJson(denomAttr);
                if (d && d.base) {
                  baseDenom = d.base;
                  // Reconstruct full path from trace array
                  if (Array.isArray(d.trace) && d.trace.length > 0) {
                    const tracePath = d.trace.map((t: any) => `${t.port_id}/${t.channel_id}`).join('/');
                    fullPath = `${tracePath}/${d.base}`;
                  } else {
                    fullPath = d.base;
                  }
                }
              } catch { /* ignore parse error */ }
            }

            if (hash && fullPath && baseDenom) {
              // Prepend ibc/ to hash if not already present
              const ibcHash = hash.startsWith('ibc/') ? hash : `ibc/${hash}`;
              ibcDenomsRows.push({ hash: ibcHash, full_path: fullPath, base_denom: baseDenom });
              registerToken(ibcHash, 'ibc', { base_denom: baseDenom, symbol: baseDenom }, tx_hash);
              registerToken(fullPath, 'ibc', { base_denom: baseDenom, symbol: baseDenom }, tx_hash);
            }
          }

          if (event_type === 'create_client' || event_type === 'update_client') {
            const clientId = findAttr(attrsPairs, 'client_id');
            if (clientId) {
              ibcClientsRows.push({
                client_id: clientId,
                chain_id: findAttr(attrsPairs, 'chain_id'),
                client_type: findAttr(attrsPairs, 'client_type'),
                updated_at_height: height,
                updated_at_time: time
              });
            }
          }


          // 🟢 BANK BALANCE DELTAS 🟢
          // Process for ALL txs (including failed) — SDK emits coin_spent/coin_received
          // for ante-handler fee deduction even on failed txs. Message-level events are
          // not emitted on failure, so this is safe and captures the actual on-chain fee.
          txBankDeltaCount += extractBalanceDeltas(event_type, attrsPairs, tx_hash, msg_index, ei);
          // 🟢 SUPPLY TRACKING FROM BANK/MODULE EVENTS 🟢
          if (isSuccess) {
            extractFactorySupplyFromEvent(event_type, attrsPairs, tx_hash ?? undefined, msg_index, ei);
          }


          // 🟢 NETWORK PARAMS 🟢
          if (event_type === 'param_change') {
            const module = findAttr(attrsPairs, 'module');
            const key = findAttr(attrsPairs, 'key');
            const value = findAttr(attrsPairs, 'value');
            if (module && key) {
              networkParamsRows.push({
                height,
                time,
                module,
                param_key: key,
                old_value: null, // Event typically only has new value
                new_value: tryParseJson(value)
              });
            }
          }

          // Specialized WASM Analytics for Oracles/Tokens removed (not needed for Zigchain)

          for (let ai = 0; ai < attrsPairs.length; ai++) {
            const attr = attrsPairs[ai];
            if (!attr) continue;
            const { key, value } = attr;
            attrRows.push({ tx_hash, msg_index, event_index: ei, attr_index: ai, key, value, height });
          }
        }
      }

      // NOTE: Failed-tx fee fallback removed. Bank events from failed txs are now
      // captured directly via extractBalanceDeltas above (isSuccess guard removed).
      // The old fallback subtracted declared fees blindly, causing phantom negatives
      // on ante-handler failures where no fee was actually charged.
    }

    // ✅ FIX: Process Begin/End Block Events for Balances (e.g. Gov Refunds, Minting)
    const allBlockEvents = [
      ...(blockLine.block_results?.begin_block_events ?? []),
      ...(blockLine.block_results?.end_block_events ?? [])
    ];

    for (let i = 0; i < allBlockEvents.length; i++) {
      const ev = allBlockEvents[i];
      const evType = ev.type;
      const attrsPairs = attrsToPairs(ev.attributes);
      // ✅ FIX: Pass 'i' as event_index to prevent dedup collisions (defaults to -1 otherwise)
      // tx_hash and msg_index are undefined for block events
      extractBalanceDeltas(evType, attrsPairs, undefined, undefined, i);
      extractFactorySupplyFromEvent(evType, attrsPairs, `block_${height}_events`, undefined, i);

      // 🟢 NETWORK PARAMS from block-level events (FinalizeBlock / EndBlock)
      if (evType === 'param_change') {
        const module = findAttr(attrsPairs, 'module');
        const key = findAttr(attrsPairs, 'key');
        const value = findAttr(attrsPairs, 'value');
        if (module && key) {
          networkParamsRows.push({
            height,
            time,
            module,
            param_key: key,
            old_value: null,
            new_value: tryParseJson(value)
          });
        }
      }
    }

    // Prefer event-derived mint/burn rows over message-derived fallback rows for the same tx/msg/denom/action.
    if (factorySupplyEventsRows.length > 1) {
      const preferredKeys = new Set<string>();
      for (const row of factorySupplyEventsRows) {
        if (row?.action !== 'mint' && row?.action !== 'burn') continue;
        if (typeof row?.event_index !== 'number' || row.event_index < 0) continue;
        preferredKeys.add(`${String(row.tx_hash)}|${String(row.msg_index)}|${String(row.denom)}|${String(row.action)}`);
      }
      if (preferredKeys.size > 0) {
        let dropped = 0;
        const filtered: any[] = [];
        for (const row of factorySupplyEventsRows) {
          const isFallback = (row?.action === 'mint' || row?.action === 'burn')
            && (typeof row?.event_index !== 'number' || row.event_index < 0);
          if (isFallback) {
            const key = `${String(row.tx_hash)}|${String(row.msg_index)}|${String(row.denom)}|${String(row.action)}`;
            if (preferredKeys.has(key)) {
              dropped += 1;
              continue;
            }
          }
          filtered.push(row);
        }
        if (dropped > 0) {
          factorySupplyEventsRows.length = 0;
          factorySupplyEventsRows.push(...filtered);
          log.debug(`[factory_supply] dropped ${dropped} fallback mint/burn row(s) at height=${height}`);
        }
      }
    }

    if (droppedTransferRows > 0) {
      log.warn(`[transfer] dropped ${droppedTransferRows} malformed transfer row(s) at height=${height}`);
    }

    const tokenRegistryRows = Array.from(tokenRegistryByDenom.values());

    return {
      blockRow, txRows, msgRows, evRows, attrRows,
      transfersRows, wasmExecRows, wasmEventsRows,
      ibcPacketsRows, // 👈 Returning IBC Data
      ibcChannelsRows,
      ibcTransfersRows,
      ibcClientsRows,
      ibcDenomsRows,
      ibcConnectionsRows,
      cw20TransfersRows,
      factoryDenomsRows, dexPoolsRows, dexSwapsRows, dexLiquidityRows, wrapperSettingsRows, wrapperEventsRows,
      balanceDeltasRows, wasmCodesRows, wasmContractsRows, wasmMigrationsRows, wasmAdminChangesRows, networkParamsRows,
      wasmEventAttrsRows,
      wasmSwapsRows, // ✅ WASM DEX Swaps Analytics
      tokenRegistryRows, // ✅ Universal Token Registry
      unknownMsgsRows, // ✅ Unknown Messages Quarantine
      factorySupplyEventsRows,
      wasmInstantiateConfigsRows,
      height
    };
  }

  private async persistBlockBuffered(blockLine: BlockLine): Promise<void> {
    const data = this.extractRows(blockLine);

    this.bufBlocks.push(data.blockRow);
    this.bufTxs.push(...data.txRows);
    this.bufMsgs.push(...data.msgRows);
    this.bufEvents.push(...data.evRows);
    this.bufAttrs.push(...data.attrRows);
    this.bufTransfers.push(...data.transfersRows);

    // Zigchain
    this.bufFactoryDenoms.push(...data.factoryDenomsRows);
    this.bufDexPools.push(...data.dexPoolsRows);
    this.bufDexSwaps.push(...data.dexSwapsRows);
    this.bufDexLiquidity.push(...data.dexLiquidityRows);
    this.bufWrapperSettings.push(...data.wrapperSettingsRows);
    this.bufWrapperEvents.push(...data.wrapperEventsRows);
    this.bufWasmEventAttrs.push(...data.wasmEventAttrsRows);

    // WASM Data
    this.bufWasmEvents.push(...data.wasmEventsRows);
    this.bufWasmExec.push(...data.wasmExecRows);

    // ✅ ADDED: New Buffers Persistence
    this.bufBalanceDeltas.push(...data.balanceDeltasRows);
    this.bufWasmCodes.push(...data.wasmCodesRows);
    this.bufWasmContracts.push(...data.wasmContractsRows);
    this.bufWasmMigrations.push(...data.wasmMigrationsRows);
    this.bufWasmAdminChanges.push(...data.wasmAdminChangesRows);
    this.bufWasmInstantiateConfigs.push(...data.wasmInstantiateConfigsRows);
    this.bufNetworkParams.push(...data.networkParamsRows);

    // ✅ IBC (Pushing to Buffer)
    this.bufIbcPackets.push(...data.ibcPacketsRows);
    this.bufIbcChannels.push(...data.ibcChannelsRows);
    this.bufIbcTransfers.push(...data.ibcTransfersRows);
    this.bufIbcClients.push(...data.ibcClientsRows);
    this.bufIbcDenoms.push(...data.ibcDenomsRows);
    this.bufIbcConnections.push(...data.ibcConnectionsRows);

    // ✅ Tokens (CW20) (Pushing to Buffer)
    this.bufCw20Transfers.push(...data.cw20TransfersRows);

    // ✅ WASM DEX Swaps Analytics (Pushing to Buffer)
    this.bufWasmSwaps.push(...data.wasmSwapsRows);
    this.bufTokenRegistry.push(...data.tokenRegistryRows); // ✅ NEW

    // ✅ Unknown Messages Quarantine (Pushing to Buffer)
    this.bufUnknownMsgs.push(...data.unknownMsgsRows);
    this.bufFactorySupplyEvents.push(...data.factorySupplyEventsRows);

    const zCount = this.bufFactoryDenoms.length + this.bufDexPools.length + this.bufDexSwaps.length + this.bufDexLiquidity.length;

    const needFlush =
      this.bufBlocks.length >= this.batchSizes.blocks ||
      this.bufTxs.length >= this.batchSizes.txs ||
      zCount >= this.batchSizes.zigchain;

    if (needFlush) {
      log.debug(`flush trigger: blocks=${this.bufBlocks.length}`);
      await this.flushAll();
    }
  }

  private async flushAll(): Promise<void> {
    if (this.bufBlocks.length === 0 && this.bufTxs.length === 0) return;

    // ✅ SNAPSHOT & SWAP BUFFERS
    // We move all current data to a local "snapshot" and clear the main buffers.
    // This allows incoming writes to continue safely while we are awaiting the DB.
    const snapshot = {
      blocks: this.bufBlocks,
      txs: this.bufTxs,
      msgs: this.bufMsgs,
      events: this.bufEvents,
      attrs: this.bufAttrs,
      transfers: this.bufTransfers,
      factoryDenoms: this.bufFactoryDenoms,
      dexPools: this.bufDexPools,
      dexSwaps: this.bufDexSwaps,
      dexLiquidity: this.bufDexLiquidity,
      wrapperSettings: this.bufWrapperSettings,
      wrapperEvents: this.bufWrapperEvents,
      wasmEventAttrs: this.bufWasmEventAttrs,
      wasmExec: this.bufWasmExec,
      wasmEvents: this.bufWasmEvents,
      balanceDeltas: this.bufBalanceDeltas,
      wasmCodes: this.bufWasmCodes,
      wasmContracts: this.bufWasmContracts,
      wasmMigrations: this.bufWasmMigrations,
      wasmAdminChanges: this.bufWasmAdminChanges,
      wasmInstantiateConfigs: this.bufWasmInstantiateConfigs,
      networkParams: this.bufNetworkParams,
      ibcPackets: this.bufIbcPackets,
      ibcChannels: this.bufIbcChannels,
      ibcTransfers: this.bufIbcTransfers,
      ibcClients: this.bufIbcClients,
      ibcDenoms: this.bufIbcDenoms,
      ibcConnections: this.bufIbcConnections,
      cw20Transfers: this.bufCw20Transfers,
      wasmSwaps: this.bufWasmSwaps,
      tokenRegistry: this.bufTokenRegistry, // ✅ NEW
      unknownMsgs: this.bufUnknownMsgs,
      factorySupplyEvents: this.bufFactorySupplyEvents,
    };

    // Reset all main buffers
    this.bufBlocks = []; this.bufTxs = []; this.bufMsgs = []; this.bufEvents = []; this.bufAttrs = []; this.bufTransfers = [];
    this.bufFactoryDenoms = []; this.bufDexPools = []; this.bufDexSwaps = []; this.bufDexLiquidity = [];
    this.bufWrapperSettings = []; this.bufWrapperEvents = [];
    this.bufWasmExec = []; this.bufWasmEvents = []; this.bufWasmEventAttrs = [];
    this.bufBalanceDeltas = []; this.bufWasmCodes = []; this.bufWasmContracts = []; this.bufWasmMigrations = [];
    this.bufWasmAdminChanges = []; this.bufWasmInstantiateConfigs = []; this.bufNetworkParams = [];
    this.bufIbcPackets = []; this.bufIbcChannels = []; this.bufIbcTransfers = [];
    this.bufIbcClients = []; this.bufIbcDenoms = []; this.bufIbcConnections = [];
    this.bufCw20Transfers = [];
    this.bufWasmSwaps = []; this.bufTokenRegistry = []; this.bufUnknownMsgs = [];
    this.bufFactorySupplyEvents = [];

    // Enrich token registry rows once per denom from chain metadata (cached per process).
    await this.enrichTokenRegistryRowsFromRpc(snapshot.tokenRegistry);

    const pool = getPgPool();
    const client = await pool.connect();
    let transactionStarted = false;

    try {
      let maxH: number | null = null;
      if (snapshot.blocks.length === 0) {
        // If we are flushing but have no blocks (e.g. metadata only),
        // skip partition check or use a fallback if absolutely needed.
        await client.query('BEGIN');
        transactionStarted = true;
      } else {
        const heights = snapshot.blocks.map(r => r.height);
        const minH = Math.min(...heights);
        maxH = Math.max(...heights);

        await ensureCorePartitions(client, minH, maxH);
        await client.query('BEGIN');
        transactionStarted = true;
      }

      // 1. Standard
      await flushBlocks(client, snapshot.blocks);
      await flushTxs(client, snapshot.txs);
      await flushMsgs(client, snapshot.msgs);
      await flushEvents(client, snapshot.events);
      await flushAttrs(client, snapshot.attrs);
      await flushTransfers(client, snapshot.transfers);

      // 2. Modules (WASM)

      await flushCw20Transfers(client, snapshot.cw20Transfers);

      // IBC Data
      if (snapshot.ibcPackets.length > 0 || snapshot.ibcTransfers.length > 0) {
        const seqs = [...snapshot.ibcPackets, ...snapshot.ibcTransfers]
          .map((r: any) => Number(r.sequence))
          .filter((n: number) => Number.isFinite(n));
        if (seqs.length > 0) {
          const minSeq = Math.min(...seqs);
          const maxSeq = Math.max(...seqs);
          await ensureIbcPartitions(client, minSeq, maxSeq);
        }
      }
      await flushIbcPackets(client, snapshot.ibcPackets);
      await flushIbcChannels(client, snapshot.ibcChannels);
      await flushIbcTransfers(client, snapshot.ibcTransfers);
      await flushIbcClients(client, snapshot.ibcClients);
      await flushIbcDenoms(client, snapshot.ibcDenoms);
      await flushIbcConnections(client, snapshot.ibcConnections);

      // WASM Registry and Enrichment (RPC Fallback)
      // ✅ ENHANCEMENT: Enrich missing contract metadata via ABCI fallback
      if (snapshot.wasmContracts.length > 0 && this.rpc && this.protoRoot) {
        for (const row of snapshot.wasmContracts) {
          if (!row.admin || !row.label) {
            try {
              let info = this.wasmContractsCache.get(row.address);
              if (!info) {
                info = await fetchContractInfoViaAbci(this.rpc, this.protoRoot, row.address);
                if (info) this.wasmContractsCache.set(row.address, info);
              }

              if (info) {
                if (!row.admin) row.admin = info.admin;
                if (!row.label) row.label = info.label;
                if (!row.creator && info.creator) row.creator = info.creator;
              }
            } catch (err: any) {
              log.debug(`[flush] Metadata enrichment failed for ${row.address}: ${err.message}`);
            }
          }
        }
      }

      await flushWasmRegistry(client, {
        codes: snapshot.wasmCodes,
        contracts: snapshot.wasmContracts,
        migrations: snapshot.wasmMigrations,
        configs: snapshot.wasmInstantiateConfigs
      });

      if (snapshot.wasmAdminChanges.length > 0) {
        await flushWasmAdminChanges(client, snapshot.wasmAdminChanges);
      }

      if (snapshot.balanceDeltas.length > 0) {
        await flushBalanceDeltas(client, snapshot.balanceDeltas);
      }

      // Core WASM Data
      await flushWasmExec(client, snapshot.wasmExec);
      await flushWasmEvents(client, snapshot.wasmEvents);
      await flushWasmEventAttrs(client, snapshot.wasmEventAttrs);
      if (snapshot.networkParams.length > 0) {
        await flushNetworkParams(client, snapshot.networkParams);
      }

      if (snapshot.unknownMsgs.length > 0) {
        await flushUnknownMessages(client, snapshot.unknownMsgs);
      }

      if (snapshot.factorySupplyEvents.length > 0) {
        await flushFactorySupplyEvents(client, snapshot.factorySupplyEvents);
      }

      // 3. Zigchain
      await flushWasmSwaps(client, snapshot.wasmSwaps); // ✅ FIX: Flush WASM Swaps
      await flushTokenRegistry(client, snapshot.tokenRegistry); // ✅ NEW

      await flushZigchainData(client, {
        factoryDenoms: snapshot.factoryDenoms,
        dexPools: snapshot.dexPools,
        dexSwaps: snapshot.dexSwaps,
        dexLiquidity: snapshot.dexLiquidity,
        wrapperSettings: snapshot.wrapperSettings,
        wrapperEvents: snapshot.wrapperEvents
      });

      if (maxH != null) {
        await upsertProgress(client, this.cfg.pg?.progressId ?? 'default', maxH);
      }
      await client.query('COMMIT');
      transactionStarted = false;
    } catch (e) {
      log.error(`[flush-error] rollback initiated: ${String(e)}`);
      if (transactionStarted) {
        await client.query('ROLLBACK');
        transactionStarted = false;
      } else {
        log.warn('[flush-error] rollback skipped: transaction was not started');
      }

      // ❌ RESTORE BUFFERS ON FAILURE
      // We prepend the snapshot data to the current buffers so they are included in the next retry.
      this.bufBlocks = [...snapshot.blocks, ...this.bufBlocks];
      this.bufTxs = [...snapshot.txs, ...this.bufTxs];
      this.bufMsgs = [...snapshot.msgs, ...this.bufMsgs];
      this.bufEvents = [...snapshot.events, ...this.bufEvents];
      this.bufAttrs = [...snapshot.attrs, ...this.bufAttrs];
      this.bufTransfers = [...snapshot.transfers, ...this.bufTransfers];
      this.bufFactoryDenoms = [...snapshot.factoryDenoms, ...this.bufFactoryDenoms];

      this.bufDexPools = [...snapshot.dexPools, ...this.bufDexPools];
      this.bufDexSwaps = [...snapshot.dexSwaps, ...this.bufDexSwaps];
      this.bufWasmSwaps = [...snapshot.wasmSwaps, ...this.bufWasmSwaps]; // ✅ FIX: Restore Buffer
      this.bufDexLiquidity = [...snapshot.dexLiquidity, ...this.bufDexLiquidity];
      this.bufWrapperSettings = [...snapshot.wrapperSettings, ...this.bufWrapperSettings];
      this.bufWrapperEvents = [...snapshot.wrapperEvents, ...this.bufWrapperEvents];
      this.bufWasmEventAttrs = [...snapshot.wasmEventAttrs, ...this.bufWasmEventAttrs];
      this.bufWasmExec = [...snapshot.wasmExec, ...this.bufWasmExec];
      this.bufWasmEvents = [...snapshot.wasmEvents, ...this.bufWasmEvents];
      this.bufBalanceDeltas = [...snapshot.balanceDeltas, ...this.bufBalanceDeltas];
      this.bufWasmCodes = [...snapshot.wasmCodes, ...this.bufWasmCodes];
      this.bufWasmContracts = [...snapshot.wasmContracts, ...this.bufWasmContracts];
      this.bufWasmMigrations = [...snapshot.wasmMigrations, ...this.bufWasmMigrations];
      this.bufWasmAdminChanges = [...snapshot.wasmAdminChanges, ...this.bufWasmAdminChanges];
      this.bufWasmInstantiateConfigs = [...snapshot.wasmInstantiateConfigs, ...this.bufWasmInstantiateConfigs];
      this.bufNetworkParams = [...snapshot.networkParams, ...this.bufNetworkParams];
      this.bufIbcPackets = [...snapshot.ibcPackets, ...this.bufIbcPackets];
      this.bufIbcChannels = [...snapshot.ibcChannels, ...this.bufIbcChannels];
      this.bufIbcTransfers = [...snapshot.ibcTransfers, ...this.bufIbcTransfers];
      this.bufIbcClients = [...snapshot.ibcClients, ...this.bufIbcClients];
      this.bufIbcDenoms = [...snapshot.ibcDenoms, ...this.bufIbcDenoms];
      this.bufIbcConnections = [...snapshot.ibcConnections, ...this.bufIbcConnections];

      this.bufCw20Transfers = [...snapshot.cw20Transfers, ...this.bufCw20Transfers];
      this.bufTokenRegistry = [...snapshot.tokenRegistry, ...this.bufTokenRegistry];

      this.bufUnknownMsgs = [...snapshot.unknownMsgs, ...this.bufUnknownMsgs];
      this.bufFactorySupplyEvents = [...snapshot.factorySupplyEvents, ...this.bufFactorySupplyEvents];

      throw e;
    } finally {
      client.release();
    }
  }

  // Atomic mode: write one block and flush immediately in a single DB transaction.
  private async persistBlockAtomic(blockLine: BlockLine): Promise<void> {
    await this.persistBlockBuffered(blockLine);
    await this.flushAll();
  }
}
