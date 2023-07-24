import assert from 'node:assert';

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
