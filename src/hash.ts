import {assert} from './asserts';
import {uuid} from './uuid.js';

export const STRING_LENGTH = 36;

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
export type Hash = {[hashTag]: true};

// We are no longer using hashes but due to legacy reason we still refer to
// them as hashes. We use UUID and counters instead.
const oldHashRe = /^[0-9a-v]{32}$/;
const uuidRe = /^[0-9a-f-]{36}$/;

export function parse(s: string): Hash {
  assertHash(s);
  return s;
}

const emptyUUID = '00000000-0000-4000-8000-000000000000';
export const emptyHash = emptyUUID as unknown as Hash;

/**
 * Creates a new "Hash" that is a UUID.
 */
export const newUUIDHash = createUUIDHashFn();

function createUUIDHashFn() {
  const uuidPrefix = uuid().substring(0, 24);
  let counter = 0;
  return () => {
    const c = String(counter++);
    return uuidPrefix + '000000000000'.slice(0, -c.length) + c;
  };
}

/**
 * Creates a function that generates UUID hashes for tests.
 */
export function makeNewFakeHashFunction(hashPrefix = 'face'): () => Hash {
  assert(
    /^[0-9a-f]{0,8}$/.test(hashPrefix),
    `Invalid hash prefix: ${hashPrefix}`,
  );
  return makeNewFakeHashFunctionInternal(hashPrefix, emptyUUID);
}

/**
 * Creates a new fake hash function.
 * @param hashPrefix The prefix of the hash. If the prefix starts with 't/' it
 * is considered a temp hash.
 */
function makeNewFakeHashFunctionInternal(
  hashPrefix: string,
  template: string,
): () => Hash {
  const s = hashPrefix + template.slice(hashPrefix.length);
  let tempHashCounter = 0;
  return () => {
    const tail = String(tempHashCounter++);
    assert(tail.length <= 12);
    return (s.slice(0, -tail.length) + tail) as unknown as Hash;
  };
}

/**
 * Generates a fake hash useful for testing.
 */
export function fakeHash(word: string): Hash {
  assert(/^[0-9a-f]{0,12}$/.test(word), `Invalid word for fakeHash: ${word}`);
  const fake = 'face';
  return (fake +
    emptyUUID.slice(4, emptyUUID.length - word.length) +
    word) as unknown as Hash;
}

export function isHash(v: unknown): v is Hash {
  return typeof v === 'string' && (uuidRe.test(v) || oldHashRe.test(v));
}

export function isUUIDHash(v: unknown): v is Hash {
  return typeof v === 'string' && uuidRe.test(v);
}

export function assertHash(v: unknown): asserts v is Hash {
  if (!isHash(v)) {
    throw new Error(`Invalid hash: '${v}'`);
  }
}
