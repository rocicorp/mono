// Add more as needed

// Firebase and Jest do not correctly setup the global crypto object.

const localCrypto =
  typeof crypto !== 'undefined'
    ? crypto
    : // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore Allow non resolved import so this compiles in non node environments
      ((await import('node:crypto')).webcrypto as Crypto);

export function getRandomValues<T extends ArrayBufferView | null>(array: T): T {
  return localCrypto.getRandomValues(array);
}

export const {subtle} = localCrypto;
