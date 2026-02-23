/**
 * Helper for fetching staking validator metadata via ABCI query.
 */
import { RpcClient } from '../../../rpc/client.js';
import { Root } from 'protobufjs';
import { decodeAnyWithRoot } from '../../../decode/dynamicProto.js';
import { getLogger } from '../../../utils/logger.js';
import { deriveConsensusAddress, normalizePubkey } from '../../../utils/crypto.js';
import { parseDec } from '../parsing.js';

const log = getLogger('staking/abci');

export interface ValidatorMetadata {
    operator_address: string;
    consensus_address: string | null;
    consensus_pubkey: string | null;
    moniker: string | null;
    website: string | null;
    details: string | null;
    commission_rate: string | null;
    max_commission_rate: string | null;
    max_change_rate: string | null;
    min_self_delegation: string | null;
    status: string | null;
}

/**
 * Extracts the raw pubkey bytes as a base64 string from a validator's
 * consensus pubkey field, handling all known naming and encoding variations.
 */
function extractPubkeyBase64(v: any, protoRoot: Root): string | null {
    const pubAny = v.consensus_pubkey || v.consensusPubkey || v.consensusPubKey || v.consensus_pub_key;
    if (!pubAny?.value) return null;

    try {
        // Step 1: Get raw bytes from the value field
        let rawVal: Uint8Array;
        if (typeof pubAny.value === 'string') {
            rawVal = Buffer.from(pubAny.value, 'base64');
        } else if (typeof pubAny.value === 'object' && pubAny.value !== null) {
            rawVal = new Uint8Array(Object.values(pubAny.value));
        } else {
            return null;
        }

        // Step 2: Try to decode via protobuf (strips the Any wrapper)
        const typeUrl = pubAny['@type'] || pubAny.type_url || pubAny.typeUrl;
        if (typeUrl) {
            try {
                const decoded = decodeAnyWithRoot(typeUrl, rawVal, protoRoot) as any;
                const rawKey = decoded?.key || decoded?.value;
                if (rawKey) {
                    return typeof rawKey === 'string'
                        ? rawKey
                        : Buffer.from(Object.values(rawKey) as any).toString('base64');
                }
            } catch {
                // Protobuf decode failed; fall through to raw value
            }
        }

        // Step 3: Fallback — return the raw value as base64
        const finalKey = typeof pubAny.value === 'string'
            ? pubAny.value
            : Buffer.from(rawVal).toString('base64');
        return normalizePubkey(finalKey);
    } catch {
        return null;
    }
}

/**
 * Normalizes a Cosmos SDK Dec string (18-decimal integer) into a human-readable decimal.
 */
function toDecimal(val: string | number | null | undefined): string | null {
    if (val == null) return null;
    try {
        const str = String(val);
        if (str.includes('.') && parseFloat(str) < 100) return str;
        const num = BigInt(str);
        const divisor = BigInt('1000000000000000000');
        const intPart = num / divisor;
        const fracPart = num % divisor;
        const fracStr = fracPart.toString().padStart(18, '0');
        return `${intPart}.${fracStr}`;
    } catch { return null; }
}

/**
 * Fetches all validators from the chain via ABCI query with pagination.
 * @param rpc - RPC client
 * @param protoRoot - Protobuf Root for encoding/decoding
 * @returns Array of validator metadata
 */
export async function fetchAllValidatorsViaAbci(
    rpc: RpcClient,
    protoRoot: Root
): Promise<ValidatorMetadata[]> {
    try {
        const ReqType = protoRoot.lookupType('cosmos.staking.v1beta1.QueryValidatorsRequest');
        const ResType = 'cosmos.staking.v1beta1.QueryValidatorsResponse';
        const path = '/cosmos.staking.v1beta1.Query/Validators';

        const out: ValidatorMetadata[] = [];
        const seen = new Set<string>();
        let nextKey: Uint8Array | null = null;
        const maxPages = 200;

        for (let page = 0; page < maxPages; page++) {
            const payload: any = {
                status: '', // All statuses
                pagination: { limit: 500 },
            };
            if (nextKey) payload.pagination.key = nextKey;

            const reqMsg = ReqType.create(payload);
            const reqBytes = ReqType.encode(reqMsg).finish();
            const reqHex = '0x' + Buffer.from(reqBytes).toString('hex');
            const response = await rpc.queryAbci(path, reqHex);

            if (!response || response.code !== 0 || !response.value) break;

            const decoded = decodeAnyWithRoot(ResType, Buffer.from(response.value, 'base64'), protoRoot) as any;
            const validators = Array.isArray(decoded?.validators) ? decoded.validators : [];

            for (const v of validators) {
                const opAddr = String(v.operatorAddress || v.operator_address || v.address || v.operator_addr || '');
                if (!opAddr || seen.has(opAddr)) continue;
                seen.add(opAddr);

                const pubkeyB64 = extractPubkeyBase64(v, protoRoot);
                const consAddr = pubkeyB64 ? deriveConsensusAddress(pubkeyB64) : null;

                const commRates = v.commission?.commissionRates || v.commission?.commission_rates;

                out.push({
                    operator_address: opAddr,
                    consensus_address: consAddr,
                    consensus_pubkey: pubkeyB64,
                    moniker: v.description?.moniker || null,
                    website: v.description?.website || null,
                    details: v.description?.details || null,
                    commission_rate: toDecimal(commRates?.rate || commRates?.Rate) ?? parseDec(commRates?.rate),
                    max_commission_rate: toDecimal(commRates?.maxRate || commRates?.max_rate || commRates?.MaxRate) ?? parseDec(commRates?.maxRate || commRates?.max_rate),
                    max_change_rate: toDecimal(commRates?.maxChangeRate || commRates?.max_change_rate || commRates?.MaxChangeRate) ?? parseDec(commRates?.maxChangeRate || commRates?.max_change_rate),
                    min_self_delegation: v.minSelfDelegation || v.min_self_delegation || null,
                    status: v.status || null,
                });
            }

            // Pagination
            const nk = decoded?.pagination?.next_key ?? decoded?.pagination?.nextKey ?? null;
            if (nk && ((typeof nk === 'string' && nk.length > 0) || (nk instanceof Uint8Array && nk.length > 0))) {
                nextKey = typeof nk === 'string' ? Buffer.from(nk, 'base64') : nk;
            } else {
                break;
            }
        }

        return out;
    } catch (err: any) {
        log.warn(`[staking] Failed to fetch all validators via ABCI: ${err.message}`);
        return [];
    }
}
