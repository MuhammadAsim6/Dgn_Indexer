// src/sink/pg/parsing.ts
import { Buffer } from 'node:buffer';

/**
 * Normalized representation of a tx log entry used by the sink.
 * @property {number} msg_index - Index of the message within the transaction (0-based). Use -1 when logs are not per-message.
 * @property {{ type: string; attributes: any }[]} events - Array of events with a type and raw attributes as returned by the node.
 */
export type NormalizedLog = {
  msg_index: number;
  events: Array<{ type: string; attributes: any }>;
};

/**
 * Returns the input value if it is an array, otherwise returns an empty array.
 * @template T
 * @param {*} x - Any value to normalize into an array.
 * @returns {T[]} The original array or an empty array if input was not an array.
 */
export function normArray<T>(x: any): T[] {
  return Array.isArray(x) ? x : [];
}

/**
 * Extracts the messages array from various transaction shapes (raw, decoded, or SDK-like).
 * Tries common paths: `tx.msgs`, `tx.messages`, `tx.body.messages`, `tx.decoded.body.messages`.
 * @param {*} tx - Transaction object in any supported shape.
 * @returns {any[]} Array of messages or an empty array if none found.
 */
export function pickMessages(tx: any): any[] {
  if (Array.isArray(tx?.msgs)) return tx.msgs;
  if (Array.isArray(tx?.messages)) return tx.messages;
  if (Array.isArray(tx?.body?.messages)) return tx.body.messages;
  if (Array.isArray(tx?.decoded?.body?.messages)) return tx.decoded?.body?.messages;
  return [];
}

/**
 * Extracts and normalizes logs from a transaction object.
 * Prefers `logsNormalized` if present; otherwise converts `tx.tx_response.logs` to {@link NormalizedLog}.
 * If only a flat `events` array exists, wraps it into a single {@link NormalizedLog} with `msg_index = -1`.
 * @param {*} tx - Transaction object in any supported shape.
 * @returns {NormalizedLog[]} Normalized logs array; empty array if none present.
 */
export function pickLogs(tx: any): NormalizedLog[] {
  if (Array.isArray(tx?.logsNormalized)) {
    return tx.logsNormalized as NormalizedLog[];
  }
  if (Array.isArray(tx?.tx_response?.logs) && tx.tx_response.logs.length > 0) {
    return tx.tx_response.logs.map((l: any) => ({
      msg_index: Number(l?.msg_index ?? -1),
      events: normArray(l?.events).map((ev: any) => ({
        type: String(ev?.type ?? 'unknown'),
        attributes: ev?.attributes ?? [],
      })),
    }));
  }
  const flat =
    (Array.isArray(tx?.eventsNormalized) && tx.eventsNormalized) || (Array.isArray(tx?.events) && tx.events) || null;

  if (Array.isArray(flat)) {
    return [
      {
        msg_index: -1,
        events: flat.map((ev: any) => ({
          type: String(ev?.type ?? 'unknown'),
          attributes: ev?.attributes ?? [],
        })),
      },
    ];
  }
  return [];
}

/**
 * Converts attributes into a uniform array of `{ key, value }` pairs.
 * Accepts either an array of `{key,value}` or a plain object map.
 * @param {*} attrs - Raw attributes.
 * @returns {{ key: string, value: (string|null) }[]} Normalized key/value pairs.
 */
export function attrsToPairs(attrs: any): Array<{ key: string; value: string | null }> {
  if (Array.isArray(attrs)) {
    return attrs.map((a) => ({
      key: String(a?.key ?? ''),
      value: a?.value != null ? String(a.value) : null,
    }));
  }
  if (attrs && typeof attrs === 'object') {
    return Object.entries(attrs).map(([k, v]) => ({
      key: String(k),
      value: v != null ? String(v as any) : null,
    }));
  }
  return [];
}

/**
 * Converts a value to a finite number or returns `null` if not convertible.
 * @param {*} x - Value to convert.
 * @returns {(number|null)} Finite number or `null`.
 */
export function toNum(x: any): number | null {
  if (x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

/**
 * Builds a compact fee object from a decoded fee structure, copying only known fields.
 * Returned object may contain: `amount`, `gas_limit`, `payer`, `granter`.
 * @param {*} fee - Decoded fee object (e.g., from Tx.auth_info.fee).
 * @returns {(object|null)} Minimal fee object or `null` if nothing to copy.
 */
export function buildFeeFromDecodedFee(fee: any): any | null {
  if (!fee) return null;
  const out: any = {};
  if (fee.amount !== undefined) out.amount = fee.amount;
  if (fee.gas_limit !== undefined) out.gas_limit = fee.gas_limit;
  if (fee.payer !== undefined) out.payer = fee.payer;
  if (fee.granter !== undefined) out.granter = fee.granter;
  return Object.keys(out).length ? out : null;
}

/**
 * Collects potential signer addresses from a list of decoded messages.
 * Scans common fields and then falls back to a deep scan for Bech32 strings.
 * @param {any[]} msgs - Array of decoded messages.
 * @returns {(string[]|null)} Unique list of candidate addresses or `null` if none found.
 */
export function collectSignersFromMessages(msgs: any[]): string[] | null {
  const s = new Set<string>();
  for (const m of msgs) {
    if (!m) continue;

    // 1. High-priority candidates
    const candidates = [
      m.signer, m.from_address, m.delegator_address, m.voter, m.sender,
      m.admin, m.proposer, m.creator, m.granter, m.depositor,
      m.proposer_address, m.proposerAddress, m.sender_address, m.authority
    ];
    for (const c of candidates) {
      if (typeof c === 'string' && isLikelyAddress(c)) s.add(c);
    }

    // 2. Recursive fallback scanner (finds ANY address in the message)
    const deep = scanForAddresses(m);
    for (const d of deep) s.add(d);
  }
  return s.size ? Array.from(s) : null;
}

/**
 * Picks a single best candidate for 'signer' from a decoded message.
 * @param {any} m - Decoded message.
 * @returns {string|null} The best signer candidate.
 */
export function pickSigner(m: any): string | null {
  if (!m || typeof m !== 'object') return null;

  // 1. Try common fields first
  const candidates = [
    m.signer, m.from_address, m.delegator_address, m.voter, m.sender,
    m.admin, m.proposer, m.creator, m.granter, m.depositor,
    m.proposer_address, m.sender_address, m.validator_address, m.authority
  ];
  for (const c of candidates) {
    if (typeof c === 'string' && isLikelyAddress(c)) return c;
  }

  // 2. Fallback to first address-like string found in the object
  const deep = scanForAddresses(m);
  return deep.length > 0 ? deep[0]! : null;
}

/**
 * Helper to check if a string looks like a Bech32 address (e.g. zig1..., cosmos1...).
 */
function isLikelyAddress(s: string): boolean {
  if (!s || typeof s !== 'string') return false;
  // Standard Bech32 format: prefix + '1' + 38-58 chars
  return /^[a-z]+1[a-z0-9]{38,58}$/.test(s);
}

/**
 * Recursively scans an object for any string that looks like an address.
 */
function scanForAddresses(obj: any, depth = 0): string[] {
  const found: string[] = [];
  if (!obj || depth > 5) return found;

  if (typeof obj === 'string') {
    if (isLikelyAddress(obj)) found.push(obj);
  } else if (Array.isArray(obj)) {
    for (const item of obj) {
      found.push(...scanForAddresses(item, depth + 1));
    }
  } else if (typeof obj === 'object') {
    for (const val of Object.values(obj)) {
      found.push(...scanForAddresses(val, depth + 1));
    }
  }
  return found;
}

/**
 * Parses a Cosmos SDK coin string of the form `${amount}${denom}` (e.g., "123uatom").
 * If the string contains multiple coins (e.g., "123uatom,456usdt"), it returns the first one.
 * @param {(string|null|undefined)} amt - Coin string to parse.
 * @returns {{ denom: string, amount: string } | null} Parsed coin parts or `null` if input is invalid.
 */
export function parseCoin(amt: string | null | undefined): { denom: string; amount: string } | null {
  if (!amt) return null;
  const str = String(amt).trim();
  if (!str) return null;

  // 1. Try JSON Array/Object parsing first
  // e.g. [{"denom":"uzig","amount":"100"}] or {"amount":"100","denom":"uzig"}
  if (str.startsWith('[') || str.startsWith('{')) {
    try {
      const parsed = JSON.parse(str);
      const obj = Array.isArray(parsed) ? parsed[0] : parsed;
      if (obj && obj.amount && obj.denom) {
        return { amount: String(obj.amount), denom: String(obj.denom) };
      }
    } catch {
      // Ignore JSON parse errors, fall through to string parsing
    }
  }

  // 2. Comma-separated (take first)
  const parts = str.split(',');
  const first = (parts[0] || '').trim();
  if (!first) return null;

  // 3. Regex Parsing
  // Matches: "100uzig", "100 uzig" (space support!), "100.5uzig" (legacy), "100" (no denom?)
  // Pattern:
  // ^(\d+)         -> Amount (digits)
  // \s*            -> Optional whitespace
  // ([a-zA-Z/][\w/:\-.]*)? -> Denom (Optional? No, must have denom for valid coin)
  //
  // Revised Pattern for robustness:
  // ^(\d+)\s*([a-zA-Z/][\w/:\-.]*)$
  const m = first.match(/^(\d+)\s*([a-zA-Z/][\w/:\-.]*)$/);

  if (m && m.length >= 3) {
    return { amount: m[1]!, denom: m[2]! };
  }

  return null;
}

/**
 * Parses a multi-coin string (e.g., "100uatom,200usdt") into an array of coin objects.
 * Handles mixed formats: "100uatom, 200usdt", JSON arrays, etc.
 * @param {(string|null|undefined)} amt - Multi-coin string to parse.
 * @returns {{ denom: string, amount: string }[]} Array of parsed coins.
 */
export function parseCoins(amt: string | null | undefined): { denom: string; amount: string }[] {
  if (!amt) return [];
  const str = String(amt).trim();
  if (!str) return [];

  // 1. JSON Array
  if (str.startsWith('[')) {
    try {
      const parsed = JSON.parse(str);
      if (Array.isArray(parsed)) {
        return parsed
          .map(p => ({ amount: String(p?.amount), denom: String(p?.denom) }))
          .filter(c => c.amount && c.denom && /^\d+$/.test(c.amount)); // Validate numeric amount
      }
    } catch {
      // Fallback
    }
  }

  // 2. Comma-Separated String
  return str
    .split(',')
    .map((s) => parseCoin(s.trim()))
    .filter((c): c is { denom: string; amount: string } => c !== null);
}

/**
 * Finds an attribute value by key in an array of `{key,value}` pairs.
 * @param {{ key: string, value: (string|null) }[]} attrs - Attributes to search.
 * @param {string} key - Attribute key to find.
 * @returns {(string|null)} The attribute value or `null` if not present.
 */
export function findAttr(attrs: Array<{ key: string; value: string | null }>, key: string): string | null {
  const a = attrs.find((x) => x.key === key);
  return a ? (a.value ?? null) : null;
}
/**
 * Parses a Cosmos SDK "Dec" string (18 decimal places, no point) into a decimal string for Postgres.
 * e.g. "100000000000000000" -> "0.1", "1000000000000000000" -> "1"
 * @param {any} val - Value to parse.
 * @returns {string|null} Decimal string or null.
 */
export function parseDec(val: any): string | null {
  if (val === undefined || val === null || val === '') return null;
  const s = String(val).trim();
  if (s.includes('.')) return s; // Already decimal

  // ✅ FIX: Unified logic for all lengths
  // Pad to at least 18 chars, then split at -18 position
  const padded = s.padStart(18, '0');
  const integerPart = padded.length > 18 ? padded.slice(0, padded.length - 18) : '0';
  const fractionalPart = padded.slice(-18);

  // Combine and strip trailing zeros
  const result = `${integerPart}.${fractionalPart}`.replace(/\.?0+$/, '');
  return result || '0';
}

export function toDateFromTimestamp(val: any): Date | null {
  if (!val) return null;
  if (val instanceof Date) return val;
  if (typeof val === 'string') {
    const d = new Date(val);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  if (typeof val === 'number') {
    const ms = val > 1e12 ? val : val * 1000;
    const d = new Date(ms);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  const seconds = val?.seconds ?? val?.secs ?? val?.sec;
  if (seconds !== undefined) {
    const nanos = val?.nanos ?? val?.nanoseconds ?? 0;
    const ms = Number(seconds) * 1000 + Math.floor(Number(nanos) / 1e6);
    if (Number.isFinite(ms)) {
      const d = new Date(ms);
      return Number.isNaN(d.getTime()) ? null : d;
    }
  }
  return null;
}

/**
 * Tries to parse a value which could be a base64-encoded JSON or already an object.
 * Useful for WASM contract messages.
 * @param val - The value to parse.
 * @returns Normalized object or the original value if parsing fails.
 */
export function tryParseJson(val: any): any {
  if (!val) return val;
  const bytes = toByteArray(val);
  if (bytes) {
    const decoded = Buffer.from(bytes).toString('utf8');
    const trimmed = decoded.trim();
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
      try {
        return JSON.parse(trimmed);
      } catch {
        return decoded;
      }
    }
    return decoded;
  }
  if (typeof val !== 'string') return val;

  // Try direct parse first
  try {
    return JSON.parse(val);
  } catch {
    // Try base64 decode then parse
    if (/^[A-Za-z0-9+/]*={0,2}$/.test(val)) {
      try {
        const decoded = Buffer.from(val, 'base64').toString('utf8');
        if (decoded.trim().startsWith('{') || decoded.trim().startsWith('[')) {
          return JSON.parse(decoded);
        }
      } catch {
        /* ignore */
      }
    }
  }
  return val;
}

function toByteArray(val: any): Uint8Array | null {
  if (val instanceof Uint8Array) return val;
  if (Buffer.isBuffer(val)) return new Uint8Array(val);
  if (Array.isArray(val) && val.every((v) => Number.isInteger(v) && v >= 0 && v <= 255)) {
    return new Uint8Array(val);
  }
  if (val && typeof val === 'object') {
    const keys = Object.keys(val);
    if (!keys.length) return null;
    const numericKeys = keys.filter((k) => String(Number(k)) === k);
    if (numericKeys.length !== keys.length) return null;
    const max = Math.max(...numericKeys.map((k) => Number(k)));
    const out = new Uint8Array(max + 1);
    for (const k of numericKeys) {
      const idx = Number(k);
      const v = val[k];
      if (!Number.isInteger(v) || v < 0 || v > 255) return null;
      out[idx] = v;
    }
    return out;
  }
  return null;
}

/**
 * Ensures a string value is purely numeric for BigInt columns.
 * Removes any non-digit characters. Returns "0" if empty or invalid.
 * @param val - The value to sanitize.
 * @returns Numeric string safely parsable by Postgres BIGINT.
 */
export function toBigIntStr(val: any): string {
  if (val === undefined || val === null) return '0';
  const s = String(val).replace(/\D/g, '');
  return s.length > 0 ? s : '0';
}

/**
 * Decodes a hex-encoded string to a JSON object.
 * Used for IBC packet_data_hex which contains hex-encoded FungibleTokenPacketData.
 * @param hex - The hex string to decode.
 * @returns Parsed JSON object or null if decoding fails.
 */
export function decodeHexToJson(hex: string | null | undefined): any | null {
  if (!hex || typeof hex !== 'string') return null;
  try {
    // Remove 0x prefix if present
    const cleanHex = hex.startsWith('0x') ? hex.slice(2) : hex;
    // Validate hex string
    if (!/^[a-fA-F0-9]*$/.test(cleanHex)) return null;
    // Decode hex to UTF-8 string
    const decoded = Buffer.from(cleanHex, 'hex').toString('utf8');
    // Try to parse as JSON
    const trimmed = decoded.trim();
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
      return JSON.parse(trimmed);
    }
    return null;
  } catch {
    return null;
  }
}

// ✅ Attribute Serialization & Sanitization Helpers (Moved from inserters/events.ts)
export const MAX_ATTR_VALUE_SIZE = 1_000_000;
export const MAX_TOTAL_JSON_SIZE = 20_000_000;
export const MAX_ATTR_COUNT = 10000;
const PREVIEW_SIZE = 4096;

/**
 * Truncates large attribute values to prevent PostgreSQL index size errors.
 */
export function sanitizeAttributes(attributes: any[]): any[] {
  if (!Array.isArray(attributes)) return attributes;

  const limited = attributes.length > MAX_ATTR_COUNT
    ? attributes.slice(0, MAX_ATTR_COUNT)
    : attributes;

  return limited.map((attr: any) => {
    if (typeof attr?.value === 'string' && attr.value.length > MAX_ATTR_VALUE_SIZE) {
      return {
        key: attr.key,
        value: attr.value.substring(0, MAX_ATTR_VALUE_SIZE) + '...[TRUNCATED]'
      };
    }
    return attr;
  });
}

/**
 * Safely serializes attributes with a total size cap.
 */
export function safeSerializeAttributes(attributes: any): string {
  if (attributes == null) return '[]';

  if (typeof attributes === 'string') {
    let parsed: unknown;
    try {
      parsed = JSON.parse(attributes);
    } catch {
      const safeRaw = attributes.length > PREVIEW_SIZE ? attributes.slice(0, PREVIEW_SIZE) : attributes;
      return JSON.stringify({
        _non_json: true,
        _raw: safeRaw,
        _raw_length: attributes.length,
      });
    }
    attributes = parsed;
  }

  if (Array.isArray(attributes) && attributes.length > MAX_ATTR_COUNT) {
    const truncated = attributes.slice(0, MAX_ATTR_COUNT);
    truncated.push({ key: '_truncated', value: `${attributes.length - MAX_ATTR_COUNT} more items...` });
    const sanitized = sanitizeAttributes(truncated);
    try {
      return JSON.stringify(sanitized);
    } catch {
      return '{"error": "failed_to_serialize"}';
    }
  }

  const sanitized = sanitizeAttributes(attributes);
  let json: string;
  try {
    json = JSON.stringify(sanitized);
  } catch {
    return '{"error": "failed_to_serialize"}';
  }

  if (json.length > MAX_TOTAL_JSON_SIZE) {
    return JSON.stringify({
      _truncated: true,
      _reason: 'max_total_size',
      _original_length: json.length,
      _preview: json.slice(0, PREVIEW_SIZE),
    });
  }

  return json;
}
