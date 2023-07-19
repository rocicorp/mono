import {declaredParams} from 'firebase-functions/params';
import assert from 'node:assert';

assert(process.env.NODE_ENV === 'test', 'Only import this file in tests');

export function mockCloudflareStringParam() {
  for (const p of declaredParams) {
    if (p.name === 'CLOUDFLARE_ACCOUNT_ID') {
      p.value = p.toString = () => 'default-cloudflare-id';
    }
  }
}

/**
 * Jest environment does not have crypto defined.
 */
export async function installCrypto() {
  assert(
    typeof globalThis.crypto === 'undefined',
    'Only do this if Jest is still broken',
  );
  globalThis.crypto = (await import('crypto')).webcrypto as Crypto;
}
