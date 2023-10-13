import {assert} from 'shared/src/asserts.js';
import * as valita from 'shared/src/valita.js';
import {uuid} from './uuid.js';

export const STRING_LENGTH = 44;

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

const emptyUUID = '00000000-0000-4000-8000-000000000000';
export const emptyHash = emptyUUID as unknown as Hash;

/**
 * Creates a new "Hash" that is a UUID.
 */
export const newUUIDHash = makeNewUUIDHashFunctionInternal('', uuid);

/**
 * Creates a function that generates UUID hashes for tests.
 */
export function makeNewFakeHashFunction(hashPrefix = 'face'): () => Hash {
  assert(
    /^[0-9a-f]{0,8}$/.test(hashPrefix),
    `Invalid hash prefix: ${hashPrefix}`,
  );
  return makeNewUUIDHashFunctionInternal(hashPrefix, () => emptyUUID);
}

/**
 * Creates a new fake hash function.
 * @param hashPrefix The prefix of the hash. If the prefix starts with 't/' it
 * is considered a temp hash.
 */
function makeNewUUIDHashFunctionInternal(
  hashPrefix: string,
  makeUUID: () => string,
): () => Hash {
  let base: string | undefined;
  let tempHashCounter = 0;
  return () => {
    if (!base) {
      // This needs to be lazy because the cloudflare worker environment will
      // throw an error if crypto.randomUUID is used statically.  Specifically:
      // Error: Some functionality, such as asynchronous I/O, timeouts, and
      // generating random values, can only be performed while handling a
      // request.
      base = makeBase(hashPrefix, makeUUID());
    }
    const tail = String(tempHashCounter++);
    return makeHash(base, tail);
  };
}

function makeBase(hashPrefix: string, uuid: string): string {
  return hashPrefix + uuid.replaceAll('-', '').slice(hashPrefix.length);
}

function makeHash(base: string, tail: string): Hash {
  assert(tail.length <= 12);
  return (base + tail.padStart(12, '0')) as unknown as Hash;
}

/**
 * Generates a fake hash useful for testing.
 */
export function fakeHash(word: string): Hash {
  assert(/^[0-9a-f]{0,12}$/.test(word), `Invalid word for fakeHash: ${word}`);
  const fake = 'face';
  const base = makeBase(fake, emptyUUID);
  return makeHash(base, word);
}

export function isHash(value: unknown): value is Hash {
  return typeof value === 'string' && hashRe.test(value);
}

export function assertHash(value: unknown): asserts value is Hash {
  valita.assert(value, hashSchema);
}

export const hashSchema = valita.string().assert(isHash, 'Invalid hash');
