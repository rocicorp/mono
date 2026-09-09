import {xxHash32} from 'js-xxhash';
import {bench, describe, use} from './bench.ts';
import {h128, h32, h64} from './hash.ts';

// A caveat about reading these numbers, and any vitest benchmark that compares
// code across a module boundary: vite rewrites an imported binding into a
// property lookup on a module namespace object, so an imported function or
// constant is re-loaded on every use and never inlined or constant-folded.
// That is invisible in the source and can dominate a tight loop -- moving this
// file's primitives into their own module appeared to cost 4x under vitest and
// exactly nothing (0.98-1.01x) once bundled.
//
// So these benchmarks are trustworthy for "same code, different algorithm" and
// misleading for "same algorithm, different module layout". `multiPass` below
// calls the imported `xxHash32` in a loop while `h64`/`h128` are local, which
// flatters them somewhat; bundled, the real margins are 1.9x for h64 and 3.1x
// for h128. To measure a layout change, bundle with esbuild and run it under node.

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
