import crypto from 'node:crypto';

export type TokenRegistryType = 'native' | 'factory' | 'cw20' | 'ibc';
export type TokenRegistrySource = 'default' | 'wrapper_settings';

export type TokenRegistryRowInput = {
  denom: string;
  height: number;
  txHash?: string | null;
  type?: TokenRegistryType;
  source?: TokenRegistrySource;
  is_primary?: boolean;
  metadata?: {
    symbol?: string | null;
    base_denom?: string | null;
    decimals?: number | null;
    creator?: string | null;
    full_meta?: Record<string, unknown> | null;
  };
};

/**
 * Normalizes an IBC denom path to its ibc/HASH equivalent.
 * If the denom is already an ibc/HASH or not a path, it returns it as-is.
 */
export function normalizeIbcDenom(denom: string): string {
  const clean = denom.trim();
  if (!clean.includes('/')) return clean;
  if (clean.toLowerCase().startsWith('ibc/')) return clean;

  // Compute SHA256 of the path
  const hash = crypto.createHash('sha256').update(clean).digest('hex').toUpperCase();
  return `ibc/${hash}`;
}

function normalizeNonEmptyString(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function normalizeDecimals(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  if (!Number.isInteger(n) || n < 0 || n > 2147483647) return null;
  return n;
}

function normalizeHeight(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n < 0) return null;
  return n;
}

function normalizeMetadata(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const obj = value as Record<string, unknown>;
  return Object.keys(obj).length > 0 ? obj : null;
}

function deriveDecimalsFromFullMeta(value: unknown): number | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const meta = value as Record<string, unknown>;
  const denomUnits = (meta['denom_units'] ?? meta['denomUnits']) as unknown;
  if (!Array.isArray(denomUnits) || denomUnits.length === 0) return null;

  let maxExponent: number | null = null;
  for (const unit of denomUnits) {
    if (!unit || typeof unit !== 'object' || Array.isArray(unit)) continue;
    const exponent = normalizeDecimals((unit as Record<string, unknown>)['exponent']);
    if (exponent === null) continue;
    maxExponent = maxExponent === null ? exponent : Math.max(maxExponent, exponent);
  }
  return maxExponent;
}

export function inferTokenType(denom: string): TokenRegistryType {
  const clean = denom.trim();
  const lower = clean.toLowerCase();

  if (lower.startsWith('factory/') || lower.startsWith('coin.zig')) {
    return 'factory';
  }
  if (lower.startsWith('ibc/') || lower.startsWith('transfer/')) {
    return 'ibc';
  }
  if (clean.startsWith('zig1') && !clean.includes('/')) {
    return 'cw20';
  }
  return 'native';
}

export function deriveTokenMeta(denom: string, type: TokenRegistryType): {
  base_denom: string | null;
  symbol: string | null;
  creator: string | null;
} {
  const clean = denom.trim();
  const lower = clean.toLowerCase();

  if (type === 'factory') {
    if (lower.startsWith('factory/')) {
      const parts = clean.split('/');
      const creator = normalizeNonEmptyString(parts[1]);
      const subDenom = normalizeNonEmptyString(parts.slice(2).join('/'));
      return { base_denom: subDenom, symbol: subDenom, creator };
    }

    if (lower.startsWith('coin.zig')) {
      const parts = clean.split('.');
      const creator = normalizeNonEmptyString(parts[1]);
      const subDenom = normalizeNonEmptyString(parts.slice(2).join('.'));
      // For coin.zig* denoms, we often want the underlying sub-denom as symbol
      return { base_denom: subDenom, symbol: subDenom, creator };
    }

    const fallback = normalizeNonEmptyString(clean.replace(/\s/g, '').split(/[/.]/).pop() ?? '');
    return { base_denom: fallback, symbol: fallback, creator: null };
  }

  if (type === 'ibc') {
    if (lower.startsWith('transfer/')) {
      const leaf = normalizeNonEmptyString(clean.split('/').pop() ?? '');
      return { base_denom: leaf ?? clean, symbol: leaf ?? clean, creator: null };
    }
    if (lower.startsWith('ibc/')) {
      const hash = clean.slice(4);
      const symbol = hash ? `ibc:${hash.slice(0, 8)}` : clean;
      return { base_denom: clean, symbol, creator: null };
    }
    const truncated = clean.slice(0, 24);
    return { base_denom: clean, symbol: truncated, creator: null };
  }

  if (type === 'native') {
    // Check for DEX pool tokens (e.g., zp1, zp2)
    if (/^zp\d+$/i.test(clean)) {
      return { base_denom: clean, symbol: clean.toUpperCase(), creator: 'dex_module' };
    }

    // Strip micro-prefix only for pure micro denoms (e.g., uzig -> ZIG).
    const symbol = /^u[a-z0-9]+$/i.test(clean) && clean.length > 1
      ? clean.slice(1).toUpperCase()
      : clean.toUpperCase();
    return { base_denom: clean, symbol, creator: null };
  }

  return { base_denom: clean, symbol: clean, creator: null };
}

export function buildTokenRegistryRow(input: TokenRegistryRowInput): {
  denom: string;
  type: TokenRegistryType;
  base_denom: string;
  symbol: string;
  decimals: number | null;
  creator: string | null;
  first_seen_height: number | null;
  first_seen_tx: string | null;
  is_primary: boolean;
  is_verified: boolean;
  metadata: Record<string, unknown> | null;
} | null {
  const cleanDenom = normalizeNonEmptyString(input.denom);
  if (!cleanDenom) return null;

  const source = input.source ?? 'default';
  const inferredType = input.type ?? inferTokenType(cleanDenom);
  const type: TokenRegistryType =
    input.type === undefined && source === 'wrapper_settings' && inferredType === 'native'
      ? 'ibc'
      : inferredType;
  const derived = deriveTokenMeta(cleanDenom, type);
  const metadata = input.metadata ?? {};
  const symbol = normalizeNonEmptyString(metadata.symbol) ?? derived.symbol ?? cleanDenom;
  const baseDenom = normalizeNonEmptyString(metadata.base_denom) ?? derived.base_denom ?? symbol;
  const creator = normalizeNonEmptyString(metadata.creator) ?? derived.creator ?? null;

  // ⚠️ CRITICAL CHANGE: Default to null instead of 6 if unknown.
  // We default to 6 for 'native' tokens that:
  // 1. Start with 'u' (standard micro denoms).
  // 2. Are DEX pool tokens (zp...).
  let fallbackDecimals: number | null = null;
  const lowerDenom = cleanDenom.toLowerCase();
  if (type === 'native') {
    if (/^u[a-z]/.test(lowerDenom) || /^zp\d+$/.test(lowerDenom)) {
      fallbackDecimals = 6;
    }
  }

  const decimals = normalizeDecimals(metadata.decimals) ?? deriveDecimalsFromFullMeta(metadata.full_meta) ?? fallbackDecimals;

  const firstSeenHeight = normalizeHeight(input.height);
  const firstSeenTx = normalizeNonEmptyString(input.txHash ?? null);

  return {
    denom: cleanDenom,
    type,
    base_denom: baseDenom,
    symbol,
    decimals,
    creator,
    first_seen_height: firstSeenHeight,
    first_seen_tx: firstSeenTx,
    is_primary: input.is_primary ?? true,
    is_verified: true, // Default to true, inserter will adjust if collisions found
    metadata: normalizeMetadata(metadata.full_meta),
  };
}
