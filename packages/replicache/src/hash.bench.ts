import {createSHA512} from 'hash-wasm';
import {makeRandomStrings} from '../../shared/src/test-data.ts';
import {bench, describe} from '../../shared/src/bench.ts';
import {beforeAll} from 'vitest';

const NUM_STRINGS = 100;
const STRING_LENGTH = 100_000;

const encoder = new TextEncoder();

function stringToUint8Array(s: string): Uint8Array<ArrayBuffer> {
  return encoder.encode(s);
}

function stringToUint16Array(s: string): Uint16Array<ArrayBuffer> {
  const u = new Uint16Array(s.length);
  for (let i = 0; i < s.length; i++) {
    u[i] = s.charCodeAt(i);
  }
  return u;
}

describe('hash', () => {
  const randomStrings = makeRandomStrings(NUM_STRINGS, STRING_LENGTH);

  describe('text encoder', () => {
    const results: (Uint8Array<ArrayBuffer> | Uint16Array<ArrayBuffer>)[] = [];

    bench('text encoder utf8', () => {
      for (let i = 0; i < randomStrings.length; i++) {
        results.push(stringToUint8Array(randomStrings[i]));
      }
    });

    bench('text encoder utf16', () => {
      for (let i = 0; i < randomStrings.length; i++) {
        results.push(stringToUint16Array(randomStrings[i]));
      }
    });
  });

  describe('sha512 wasm utf8', () => {
    let calculateHash: (sum: Uint8Array<ArrayBuffer>) => Uint8Array;
    const results: (Uint8Array | ArrayBuffer)[] = [];

    beforeAll(async () => {
      const hasher = await createSHA512();
      calculateHash = sum => hasher.init().update(sum).digest('binary');
    });

    bench('sha512 wasm from string utf8', async () => {
      for (let i = 0; i < randomStrings.length; i++) {
        const sum = stringToUint8Array(randomStrings[i]);
        results.push(calculateHash(sum));
      }
    });
  });

  describe('sha512 wasm utf16', () => {
    let calculateHash: (sum: Uint16Array<ArrayBuffer>) => Uint8Array;
    const results: (Uint8Array | ArrayBuffer)[] = [];

    beforeAll(async () => {
      const hasher = await createSHA512();
      calculateHash = sum =>
        hasher
          .init()
          .update(new Uint8Array(sum.buffer, sum.byteOffset, sum.byteLength))
          .digest('binary');
    });

    bench('sha512 wasm from string utf16', async () => {
      for (let i = 0; i < randomStrings.length; i++) {
        const sum = stringToUint16Array(randomStrings[i]);
        results.push(calculateHash(sum));
      }
    });
  });

  describe('sha512 native utf8', () => {
    const results: (Uint8Array | ArrayBuffer)[] = [];

    bench('sha512 native from string utf8', async () => {
      for (let i = 0; i < randomStrings.length; i++) {
        const sum = stringToUint8Array(randomStrings[i]);
        const buf = await crypto.subtle.digest('SHA-512', sum);
        results.push(buf);
      }
    });
  });

  describe('sha512 native utf16', () => {
    const results: (Uint8Array | ArrayBuffer)[] = [];

    bench('sha512 native from string utf16', async () => {
      for (let i = 0; i < randomStrings.length; i++) {
        const sum = stringToUint16Array(randomStrings[i]);
        const buf = await crypto.subtle.digest('SHA-512', sum);
        results.push(buf);
      }
    });
  });
});
