// Add more as needed

// Firebase and Jest do not correctly setup the global crypto object.

const localCrypto =
  typeof crypto !== 'undefined'
    ? crypto
    : // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore Fallback for Node etc
      ((await import('crypto')).webcrypto as Crypto);

export function getRandomValues<T extends ArrayBufferView | null>(array: T): T {
  return localCrypto.getRandomValues(array);
}

// rollup does not like `export const {subtle} = ...
// eslint-disable-next-line prefer-destructuring
export const subtle = localCrypto.subtle;
