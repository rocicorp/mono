import {Base64Decoder, Base64Encoder, urlAlphabet} from './base64.js';

const buffer = new ArrayBuffer(8);
const view = new DataView(buffer);
const u8 = new Uint8Array(buffer);

/**
 * This is the URL safe base64 encoding but sorted lexicographically.
 */
const alphabet = urlAlphabet.split('').sort().join('');

const encoder = new Base64Encoder(alphabet);
const decoder = new Base64Decoder(alphabet);

export function encodeFloat64AsString(n: number) {
  view.setFloat64(0, n);

  const high = view.getUint32(0);
  const low = view.getUint32(4);

  // The sign bit is 1 for negative numbers
  // We flip the sign bit so that positive numbers are ordered before negative numbers

  // If negative we flip all the bits so that larger absolute numbers are treated smaller
  if (n < 0 || Object.is(n, -0)) {
    view.setUint32(0, high ^ 0xffffffff);
    view.setUint32(4, low ^ 0xffffffff);
  } else {
    // we only flip the sign
    view.setUint32(0, high ^ (1 << 31));
  }

  return encoder.encode(u8);
}

export function decodeFloat64AsString(s: string): number {
  decoder.decodeInto(s, u8);

  const high = view.getUint32(0);
  const low = view.getUint32(4);
  const sign = high >> 31;

  // Positive
  if (sign) {
    // we only flip the sign
    view.setUint32(0, high ^ (1 << 31));
  } else {
    // If negative we flipped all the bits so that larger absolute numbers are treated smaller
    view.setUint32(0, high ^ 0xffffffff);
    view.setUint32(4, low ^ 0xffffffff);
  }

  return view.getFloat64(0);
}
