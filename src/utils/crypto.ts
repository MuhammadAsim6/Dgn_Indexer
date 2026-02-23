import crypto from 'node:crypto';
import { base64ToBytes, bytesToHex } from './bytes.js';

/**
 * Normalizes an Ed25519 public key by stripping the Protobuf wrapper (0x0A 0x20)
 * if present. Returns the raw 32-byte key as a base64 string.
 *
 * @param pubkeyBase64 - The public key in base64 format.
 * @returns The normalized raw public key as a base64 string.
 */
export function normalizePubkey(pubkeyBase64: string): string {
    if (!pubkeyBase64) return pubkeyBase64;
    try {
        const bytes = base64ToBytes(pubkeyBase64);
        // Protobuf-encoded Ed25519 PubKey has a 2-byte prefix: 0x0a (field 1, wire type 2) + 0x20 (length 32).
        if (bytes.length === 34 && bytes[0] === 0x0a && bytes[1] === 0x20) {
            return Buffer.from(bytes.slice(2)).toString('base64');
        }
    } catch {
        /* ignore */
    }
    return pubkeyBase64;
}

/**
 * Derives a Tendermint consensus address (Hex) from a base64-encoded Ed25519 public key.
 * 
 * Logic: 
 * 1. Decode base64 pubkey.
 * 2. Compute SHA-256 hash.
 * 3. Take the first 20 bytes (160 bits).
 * 4. Return as uppercase Hex string.
 * 
 * @param pubkeyBase64 - The Ed25519 public key in base64 format.
 * @returns The derived consensus address as an uppercase Hex string.
 */
export function deriveConsensusAddress(pubkeyBase64: string): string | null {
    if (!pubkeyBase64) return null;
    try {
        const normalized = normalizePubkey(pubkeyBase64);
        const bytes = base64ToBytes(normalized);

        const hash = crypto.createHash('sha256').update(bytes).digest();
        const truncated = hash.slice(0, 20);
        return bytesToHex(truncated).toUpperCase();
    } catch (e) {
        return null;
    }
}
