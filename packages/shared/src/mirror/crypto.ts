// Add more as needed

// Firebase and Jest do not correctly setup the global crypto object.

export const {getRandomValues, subtle} =
  typeof crypto !== 'undefined'
    ? crypto
    : ((await import('crypto')).webcrypto as Crypto);
