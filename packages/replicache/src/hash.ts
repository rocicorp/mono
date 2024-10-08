import {assert} from '../../shared/src/asserts.js';
import {randomUint64} from '../../shared/src/random-uint64.js';
import * as valita from '../../shared/src/valita.js';

export const STRING_LENGTH = 22;

// We use an opaque type so that we can make sure that a hash is always a hash.
// TypeScript does not have direct support but we can use a trick described
// here:
//
// https://evertpot.com/opaque-ts-types/
//
// The basic idea is to declare a type that cannot be created. We then use
// functions that cast a string to this type.
//

// By using declare we tell the type system that there is a unique symbol.
// However, there is no such symbol but the type system does not care.
declare const hashTag: unique symbol;

/**
 * Opaque type representing a hash. The only way to create one is using `parse`
 * or `hashOf` (except for static unsafe cast of course).
 */
export type Hash = string & {[hashTag]: true};

// We are no longer using hashes but due to legacy reason we still refer to
// them as hashes. We use UUID and counters instead.
const hashRe = /^[0-9a-v-]+$/;

export function parse(s: string): Hash {
  assertHash(s);
  return s;
}

const emptyUUID = '0'.repeat(STRING_LENGTH);
export const emptyHash = emptyUUID as unknown as Hash;

/**
 * Creates a function that generates random hashes.
 */
export const newRandomHash = makeNewRandomHashFunctionInternal();

/**
 * Creates a function that generates UUID hashes for tests.
 */
export function makeNewFakeHashFunction(hashPrefix = 'fake'): () => Hash {
  assert(
    /^[0-9a-v]{0,8}$/.test(hashPrefix),
    `Invalid hash prefix: ${hashPrefix}`,
  );
  let i = 0;
  return () => {
    const count = String(i++);
    return (hashPrefix +
      '0'.repeat(STRING_LENGTH - hashPrefix.length - count.length) +
      count) as Hash;
  };
}

function toStringAndSlice(n: number | bigint, len: number): string {
  return n.toString(32).slice(-len).padStart(len, '0');
}

/**
 * This creates an ID that looks like `<RANDOM><COUNTER>`. The random part is
 * a random number encoded with base 32 and the length is 12 characters. The
 * is 10 characters long and encoded as base 32. The total length is 22 characters.
 *
 * Do the math: https://devina.io/collision-calculator
 */
function makeNewRandomHashFunctionInternal(): () => Hash {
  let base = '';
  let i = 0;

  return () => {
    if (!base) {
      // This needs to be lazy because the cloudflare worker environment will
      // throw an error if crypto.getRandomValues is used statically.  Specifically:
      // Error: Some functionality, such as asynchronous I/O, timeouts, and
      // generating random values, can only be performed while handling a
      // request.
      base = toStringAndSlice(randomUint64(), 12);
    }
    const tail = toStringAndSlice(i++, 10);
    return (base + tail) as Hash;
  };
}

/**
 * Generates a fake hash useful for testing.
 */
export function fakeHash(word: string | number): Hash {
  if (typeof word === 'number') {
    word = String(word);
  }
  return ('fake' + '0'.repeat(STRING_LENGTH - 4 - word.length) + word) as Hash;
}

export function isHash(value: unknown): value is Hash {
  return typeof value === 'string' && hashRe.test(value);
}

export function assertHash(value: unknown): asserts value is Hash {
  valita.assert(value, hashSchema);
}

export const hashSchema = valita.string().assert(isHash, 'Invalid hash');
