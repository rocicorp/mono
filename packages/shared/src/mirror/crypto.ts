// Add more as needed

// Firebase and Jest do not correctly setup the global crypto object.

const localCrypto =
  typeof crypto !== 'undefined'
    ? crypto
    : ((await import('crypto')).webcrypto as Crypto);

export function getRandomValues<T extends ArrayBufferView | null>(array: T): T {
  return localCrypto.getRandomValues(array);
}

export const {subtle} = localCrypto;
