import {assert} from './asserts.js';

export const alphabet =
  'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
export const urlAlphabet =
  'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';

function encodeBase64(data: Uint8Array, alphabet: string): string {
  let s = '';
  let i;
  const {length} = data;
  for (i = 2; i < length; i += 3) {
    s += alphabet[data[i - 2] >> 2];
    s += alphabet[((data[i - 2] & 0x03) << 4) | (data[i - 1] >> 4)];
    s += alphabet[((data[i - 1] & 0x0f) << 2) | (data[i] >> 6)];
    s += alphabet[data[i] & 0x3f];
  }
  if (i === length + 1) {
    // 1 octet yet to write
    s += alphabet[data[i - 2] >> 2];
    s += alphabet[(data[i - 2] & 0x03) << 4];
    s += '==';
  }
  if (i === length) {
    // 2 octets yet to write
    s +=
      alphabet[data[i - 2] >> 2] +
      alphabet[((data[i - 2] & 0x03) << 4) | (data[i - 1] >> 4)] +
      alphabet[(data[i - 1] & 0x0f) << 2] +
      '=';
  }
  return s;
}

function getBufferLength(base64: string): number {
  let bufferLength = base64.length * 0.75;
  if (base64[base64.length - 1] === '=') {
    bufferLength--;
    if (base64[base64.length - 2] === '=') {
      bufferLength--;
    }
  }
  return bufferLength;
}

function decodeBase64Into(
  base64: string,
  bytes: Uint8Array,
  lookup: Uint8Array,
): void {
  const bufferLength = getBufferLength(base64);
  assert(bytes.length === bufferLength);

  const {length} = base64;
  let p = 0;
  let encoded1: number;
  let encoded2: number;
  let encoded3: number;
  let encoded4: number;

  for (let i = 0; i < length; i += 4) {
    encoded1 = lookup[base64.charCodeAt(i)];
    encoded2 = lookup[base64.charCodeAt(i + 1)];
    encoded3 = lookup[base64.charCodeAt(i + 2)];
    encoded4 = lookup[base64.charCodeAt(i + 3)];

    bytes[p++] = (encoded1 << 2) | (encoded2 >> 4);
    bytes[p++] = ((encoded2 & 15) << 4) | (encoded3 >> 2);
    bytes[p++] = ((encoded3 & 3) << 6) | (encoded4 & 63);
  }
}

function decodeBase64(base64: string, lookup: Uint8Array): Uint8Array {
  const bufferLength = base64.length * 0.75;
  const bytes = new Uint8Array(bufferLength);
  decodeBase64Into(base64, bytes, lookup);
  return bytes;
}

export class Base64Encoder {
  readonly alphabet: string;

  constructor(alphabet: string) {
    assert(alphabet.length === 64);
    this.alphabet = alphabet;
  }

  encode(data: Uint8Array): string {
    return encodeBase64(data, this.alphabet);
  }
}

export class Base64Decoder {
  readonly alphabet: string;
  readonly #lookup = new Uint8Array(256);

  constructor(alphabet: string) {
    assert(alphabet.length === 64);
    this.alphabet = alphabet;

    for (let i = 0; i < alphabet.length; i++) {
      this.#lookup[alphabet[i].charCodeAt(0)] = i;
    }
  }

  decode(base64: string): Uint8Array {
    return decodeBase64(base64, this.#lookup);
  }

  decodeInto(base64: string, bytes: Uint8Array): void {
    decodeBase64Into(base64, bytes, this.#lookup);
  }
}
