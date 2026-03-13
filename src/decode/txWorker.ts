/**
 * Worker thread that listens for 'init' and 'decode' messages,
 * loads protobuf root if needed, and decodes Cosmos transactions.
 */
import { parentPort } from 'node:worker_threads';
import { Buffer } from 'node:buffer';
import { loadProtoRootWithProgress } from './dynamicProto.ts';
import { getLogger } from '../utils/logger.ts';
import type {
  InitMsg,
  DecodeMsg,
  DecodeGenericMsg,
  InMsg,
  ProgressMsg,
  ReadyMsg,
  WorkerOk,
  WorkerErr,
  OutMsg,
} from './txWorker.types.ts';
import { setProtoRoot, clearProtoRoot, getProtoRoot } from './decoders/context.ts';
import { decodeTxBase64 } from './decoders/tx.ts';

const log = getLogger('decode/txWorker');

/**
 * Handles the 'init' message: loads proto definitions if a directory is provided,
 * sends progress and ready messages.
 * @param msg - The initialization message containing optional protoDir.
 * @returns Promise<void>
 */
async function onInit(msg: InitMsg) {
  if (!msg.protoDir) {
    clearProtoRoot();
    parentPort!.postMessage({ type: 'ready', ok: true } as ReadyMsg);
    log.warn('[txWorker] no protoDir provided — dynamic decode disabled');
    return;
  }

  try {
    const root = await loadProtoRootWithProgress(
      msg.protoDir,
      (loaded, total) => parentPort!.postMessage({ type: 'progress', loaded, total } as ProgressMsg),
      200,
    );
    setProtoRoot(root);
    parentPort!.postMessage({ type: 'ready', ok: true } as ReadyMsg);
    log.info(`[txWorker] loaded proto root from: ${msg.protoDir}`);
  } catch (e: any) {
    clearProtoRoot();
    parentPort!.postMessage({ type: 'ready', ok: false, detail: String(e?.message ?? e) } as ReadyMsg);
    log.warn(`[txWorker] failed to load proto root: ${String(e?.message ?? e)}`);
  }
}

/**
 * Handles the 'decode' message: decodes a base64 transaction and sends the result back.
 * @param msg - The decode message containing the transaction base64 string and id.
 */
function onDecode(msg: DecodeMsg) {
  try {
    let decoded = decodeTxBase64(msg.txBase64);

    // ✨ ENHANCEMENT: Also make transaction messages friendly
    // This ensures MsgUpdateParams and other messages are snake_case.
    decoded = makeFriendly(decoded);

    const out: WorkerOk = { id: msg.id, ok: true, decoded };
    parentPort!.postMessage(out as OutMsg);
  } catch (e: any) {
    const out: WorkerErr = { id: msg.id, ok: false, error: String(e?.message ?? e) };
    parentPort!.postMessage(out as OutMsg);
  }
}

/**
 * Handles the 'decode-generic' message: decodes any protobuf type using the loaded root.
 */
function onDecodeGeneric(msg: DecodeGenericMsg) {
  try {
    const root = getProtoRoot();
    if (!root) throw new Error('Proto root not loaded');

    // Remove leading / if present
    const typePath = msg.typeUrl.replace(/^\/+/, '');
    const Type = root.lookupType(typePath);
    const buffer = Buffer.from(msg.base64, 'base64');
    const decoded = Type.decode(buffer);
    let obj = Type.toObject(decoded, { longs: String, enums: String, defaults: true });

    // ✨ ENHANCEMENT: Make the output look like the LCD (REST API) version
    obj = makeFriendly(obj);

    const out: WorkerOk = { id: msg.id, ok: true, decoded: obj };
    parentPort!.postMessage(out as OutMsg);
  } catch (e: any) {
    const out: WorkerErr = { id: msg.id, ok: false, error: String(e?.message ?? e) };
    parentPort!.postMessage(out as OutMsg);
  }
}

/**
 * Transforms raw RPC Protobuf objects into a more user-friendly format (matching LCD).
 * Also handles Uint8Array/Buffer binary data by converting to base64 or printable strings.
 */
function makeFriendly(val: any): any {
  if (val === null || typeof val !== 'object') return val;

  // 0. Handle TypedArrays/Buffers directly
  if (val instanceof Uint8Array || Buffer.isBuffer(val)) {
    const buf = Buffer.from(val);
    const str = buf.toString('utf8');
    // If it's short, printable ASCII, and has no nulls, it's likely a "friendly" format (like sdk.Dec)
    if (buf.length > 0 && buf.length < 256 && !str.includes('\x00') && /^[\x20-\x7E]*$/.test(str)) {
      return str;
    }
    // Otherwise, it's raw binary (e.g., WASM bytecode)
    return buf.toString('base64');
  }

  if (Array.isArray(val)) {
    return val.map((item) => makeFriendly(item));
  }

  // 1. Duration objects: { seconds: "600", nanos: 0 } -> "600s"
  if (val.seconds !== undefined && val.nanos !== undefined && Object.keys(val).length === 2) {
    return `${val.seconds}s`;
  }

  // 2. Objects with numeric keys (often appear for Uint8Array in some decoding contexts)
  const keys = Object.keys(val);
  if (keys.length > 0 && keys.length < 256 && keys.every((k) => !isNaN(Number(k)))) {
    try {
      const bytes = Object.values(val) as number[];
      const buf = Buffer.from(bytes);
      const str = buf.toString('utf8');
      // Only convert to string if it looks like a printable ASCII "friendly" value
      if (buf.length > 0 && !str.includes('\x00') && /^[\x20-\x7E]*$/.test(str)) {
        return str;
      }
    } catch { /* ignore and continue */ }
  }

  const out: any = {};
  for (const [key, v] of Object.entries(val)) {
    // 3. camelCase -> snake_case
    const snake = key.replace(/[A-Z]/g, (m) => `_${m.toLowerCase()}`);
    out[snake] = makeFriendly(v);
  }
  return out;
}

/**
 * Routes incoming worker messages to appropriate handlers.
 */
parentPort!.on('message', (msg: InMsg) => {
  if (msg.type === 'init') return void onInit(msg);
  if (msg.type === 'decode') return void onDecode(msg);
  if (msg.type === 'decode-generic') return void onDecodeGeneric(msg);
});
