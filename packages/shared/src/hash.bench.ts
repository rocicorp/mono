import {xxHash32} from 'js-xxhash';
import {bench, describe, use} from './bench.ts';
import {h128, h32, h64} from './hash.ts';

/**
 * The implementation `hash.ts` replaced: one `xxHash32` pass per word, each
 * pass UTF-8 encoding the string again.
 */
function multiPass(str: string, words: number): bigint {
  let hash = 0n;
  for (let i = 0; i < words; i++) {
    hash = (hash << 32n) + BigInt(xxHash32(str, i));
  }
  return hash;
}

// Sizes chosen around what the callers hash: a row key, a normalized query AST,
// a view-syncer row-set signature.
const SIZES = [64, 256, 1024, 8192];

function makeInput(len: number): string {
  let s = '';
  for (let i = 0; s.length < len; i++) {
    s += `{"table":"issue${i}","column":"created${i}","value":${i}},`;
  }
  return s.slice(0, len);
}

describe('h64', () => {
  for (const size of SIZES) {
    const s = makeInput(size);
    bench(`${size} chars | multi-pass (before)`, () => {
      use(multiPass(s, 2));
    });
    bench(`${size} chars | single-pass (after)`, () => {
      use(h64(s));
    });
  }
});

describe('h128', () => {
  for (const size of SIZES) {
    const s = makeInput(size);
    bench(`${size} chars | multi-pass (before)`, () => {
      use(multiPass(s, 4));
    });
    bench(`${size} chars | single-pass (after)`, () => {
      use(h128(s));
    });
  }
});

describe('h32', () => {
  for (const size of SIZES) {
    const s = makeInput(size);
    bench(`${size} chars | js-xxhash (before)`, () => {
      use(xxHash32(s, 0));
    });
    bench(`${size} chars | single-pass (after)`, () => {
      use(h32(s));
    });
  }
});
