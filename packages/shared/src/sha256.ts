import * as crypto from './crypto.js';

/**
 * Returns a hex-encoded SHA-256 hash of the given string.
 */
export async function sha256OfString(s: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(s);
  const hash = await crypto.subtle.digest('SHA-256', data);
  return hexStringFromBuffer(hash);
}

function hexStringFromBuffer(hash: ArrayBuffer): string {
  let s = '';
  for (const byte of new Uint8Array(hash)) {
    s += byte < 10 ? '0' : '' + byte.toString(16);
  }
  return s;
}
