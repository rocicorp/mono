import {assert} from './asserts';
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
export type Hash = {[hashTag]: true};

// We are no longer using hashes but due to legacy reason we still refer to
// them as hashes. We use UUID and counters instead.
const oldHashRe = /^[0-9a-v]{32}$/;
const oldUUIDRe = /^[0-9a-f-]{36}$/;
const uuidRe = /^[0-9a-f]{44}$/;

export function parse(s: string): Hash {
  assertHash(s);
  return s;
}

const emptyUUID = '00000000-0000-4000-8000-000000000000';
export const emptyHash = emptyUUID as unknown as Hash;

/**
 * Creates a new "Hash" that is a UUID.
 */
export const newUUIDHash = makeNewUUIDHashFunctionInternal('', uuid());

/**
 * Creates a function that generates UUID hashes for tests.
 */
export function makeNewFakeHashFunction(hashPrefix = 'face'): () => Hash {
  assert(
    /^[0-9a-f]{0,8}$/.test(hashPrefix),
    `Invalid hash prefix: ${hashPrefix}`,
  );
  return makeNewUUIDHashFunctionInternal(hashPrefix, emptyUUID);
}

/**
 * Creates a new fake hash function.
 * @param hashPrefix The prefix of the hash. If the prefix starts with 't/' it
 * is considered a temp hash.
 */
function makeNewUUIDHashFunctionInternal(
  hashPrefix: string,
  uuid: string,
): () => Hash {
  const base = makeBase(hashPrefix, uuid);
  let tempHashCounter = 0;
  return () => {
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

export function isHash(v: unknown): v is Hash {
  return (
    typeof v === 'string' &&
    (uuidRe.test(v) || oldUUIDRe.test(v) || oldHashRe.test(v))
  );
}

export function assertHash(v: unknown): asserts v is Hash {
  if (!isHash(v)) {
    throw new Error(`Invalid hash: '${v}'`);
  }
}
