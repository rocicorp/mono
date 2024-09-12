// This is taken from https://github.com/ai/nanoid/blob/main/index.browser.js We
// copy this because we want to use `--platform=neutral` which doesn't work with
// the npm package.
// Also we changed the random number generator to use Math.random() for compat
// with React Native.

import {getNonCryptoRandomValues} from 'shared/src/random-values.js';

export function nanoid(size = 21): string {
  // Use our custom getRandomValues function to fill a Uint8Array with random values.
  const randomBytes = getNonCryptoRandomValues(new Uint8Array(size));

  return randomBytes.reduce((id, byte) => {
    // It is incorrect to use bytes exceeding the alphabet size.
    // The following mask reduces the random byte in the 0-255 value
    // range to the 0-63 value range. Therefore, adding hacks, such
    // as empty string fallback or magic numbers, is unneccessary because
    // the bitmask trims bytes down to the alphabet size.
    byte &= 63;
    if (byte < 36) {
      // `0-9a-z`
      id += byte.toString(36);
    } else if (byte < 62) {
      // `A-Z`
      id += (byte - 26).toString(36).toUpperCase();
    } else if (byte > 62) {
      id += '-';
    } else {
      id += '_';
    }
    return id;
  }, '');
}
