// Add more as needed

// Firebase and Jest do not correctly setup the global crypto object.

// Don't inline this. It is done this way to prevent TS from type checking the
// module as well as to prevent esbuild (actually web-dev-server which does not
// support external) from trying to resolve the module.
const cryptoNodeModuleName = 'crypto';

const localCrypto =
  typeof crypto !== 'undefined'
    ? crypto
    : ((await import(cryptoNodeModuleName)).webcrypto as Crypto);

export function getRandomValues<T extends ArrayBufferView | null>(array: T): T {
  return localCrypto.getRandomValues(array);
}

// rollup does not like `export const {subtle} = ...
// eslint-disable-next-line prefer-destructuring
export const subtle = localCrypto.subtle;
