import {assert} from './asserts.js';
import {parseBigInt} from './parse-big-int.js';

const view = new DataView(new ArrayBuffer(8));

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

  const bigint = view.getBigUint64(0);
  return bigint.toString(36).padStart(13, '0');
}

export function decodeFloat64AsString(s: string): number {
  assert(s.length === 13, `Invalid encoded float64: ${s}`);
  const bigint = parseBigInt(s, 36);
  view.setBigUint64(0, bigint);

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
