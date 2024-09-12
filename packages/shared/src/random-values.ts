export function getNonCryptoRandomValues(array: Uint8Array) {
  if (array === null) {
    throw new TypeError('array cannot be null');
  }

  // Fill the array with random values
  for (let i = 0; i < array.length; i++) {
    array[i] = Math.floor(Math.random() * 256); // Random byte (0-255)
  }

  return array;
}
