import {createSHA512} from 'hash-wasm';
import type {Benchmark} from '../benchmark.ts';
import {makeRandomStrings} from '../data.ts';

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

export function benchmarks(): Array<Benchmark> {
  return [
    textEncoderUtf8(),
    textEncoderUtf16(),
    sha512({wasm: true, utf8: true}),
    sha512({wasm: true, utf8: false}),
    sha512({wasm: false, utf8: true}),
    sha512({wasm: false, utf8: false}),
  ];
}

const NUM_STRINGS = 100;
const STRING_LENGTH = 100_000;

function textEncoderUtf8(): Benchmark {
  let randomStrings: string[];
  const results = [];
  return {
    name: 'text encoder utf8',
    group: 'hash',
    setup() {
      randomStrings = makeRandomStrings(NUM_STRINGS, STRING_LENGTH);
    },
    run() {
      for (let i = 0; i < randomStrings.length; i++) {
        results.push(stringToUint8Array(randomStrings[i]));
      }
    },
  };
}

function textEncoderUtf16(): Benchmark {
  let randomStrings: string[];
  const results = [];
  return {
    name: 'text encoder utf16',
    group: 'hash',
    setup() {
      randomStrings = makeRandomStrings(NUM_STRINGS, STRING_LENGTH);
    },
    run() {
      for (let i = 0; i < randomStrings.length; i++) {
        results.push(stringToUint16Array(randomStrings[i]));
      }
    },
  };
}

function sha512({wasm, utf8}: {wasm: boolean; utf8: boolean}): Benchmark {
  let randomStrings: string[];
  const results = [];
  let calculateHash: (
    sum: Uint8Array<ArrayBuffer> | Uint16Array<ArrayBuffer>,
  ) => Uint8Array | Promise<ArrayBuffer>;
  const toSum = utf8 ? stringToUint8Array : stringToUint16Array;

  return {
    name: `sha512 ${wasm ? 'wasm' : 'native'} from string ${
      utf8 ? 'utf8' : 'utf16'
    }`,
    group: 'hash',
    async setup() {
      randomStrings = makeRandomStrings(NUM_STRINGS, STRING_LENGTH);
      if (wasm) {
        const hasher = await createSHA512();
        calculateHash = sum => hasher.init().update(sum).digest('binary');
      } else {
        calculateHash = sum => crypto.subtle.digest('SHA-512', sum);
      }
    },
    async run() {
      for (let i = 0; i < randomStrings.length; i++) {
        const sum = toSum(randomStrings[i]);
        const buf = await calculateHash(sum);
        results.push(buf);
      }
    },
  };
}
