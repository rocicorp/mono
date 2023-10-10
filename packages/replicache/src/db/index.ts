import type {LogContext} from '@rocicorp/logger';
import type {BTreeRead} from '../btree/read.js';
import type {BTreeWrite} from '../btree/write.js';
import type {Hash} from '../hash.js';
import type {FrozenJSONObject, FrozenJSONValue} from '../json.js';
import type {IndexRecord} from './commit.js';

export class IndexRead<BTree = BTreeRead> {
  readonly meta: IndexRecord;
  readonly map: BTree;

  constructor(meta: IndexRecord, map: BTree) {
    this.meta = meta;
    this.map = map;
  }
}

export class IndexWrite extends IndexRead<BTreeWrite> {
  // Note: does not update self.meta.valueHash (doesn't need to at this point as flush
  // is only called during commit.)
  flush(): Promise<Hash> {
    return this.map.flush();
  }

  clear(): Promise<void> {
    return this.map.clear();
  }
}

// Index or de-index a single primary entry.
export async function indexValue(
  lc: LogContext,
  index: BTreeWrite,
  op: IndexOperation,
  key: string,
  val: FrozenJSONValue,
  jsonPointer: string,
  allowEmpty: boolean,
): Promise<void> {
  try {
    for (const entry of getIndexKeys(key, val, jsonPointer, allowEmpty)) {
      switch (op) {
        case IndexOperation.Add:
          await index.put(entry, val);
          break;
        case IndexOperation.Remove:
          await index.del(entry);
          break;
      }
    }
  } catch (e) {
    // Right now all the errors that index_value() returns are customers dev
    // problems: either the value is not json, the pointer is into nowhere, etc.
    // So we ignore them.
    lc.info?.('Not indexing value', val, ':', e);
  }
}

// Gets the set of index keys for a given primary key and value.
export function getIndexKeys(
  primary: string,
  value: FrozenJSONValue,
  jsonPointer: string,
  allowEmpty: boolean,
): string[] {
  const target = evaluateJSONPointer(value, jsonPointer);
  if (target === undefined) {
    if (allowEmpty) {
      return [];
    }
    throw new Error(`No value at path: ${jsonPointer}`);
  }

  const values = Array.isArray(target) ? target : [target];

  const indexKeys: string[] = [];
  for (const value of values) {
    if (typeof value === 'string') {
      indexKeys.push(encodeIndexKey([value, primary]));
    } else {
      throw new Error('Unsupported target type');
    }
  }

  return indexKeys;
}

export const KEY_VERSION_0 = '\u0000';
export const KEY_SEPARATOR = '\u0000';

/**
 * When using indexes the key is a tuple of the secondary key and the primary
 * key.
 */
export type IndexKey = readonly [secondary: string, primary: string];

// An index key is encoded to vec of bytes in the following order:
//   - key version byte(s), followed by
//   - the secondary key bytes (which for now is a UTF8 encoded string), followed by
//   - the key separator, a null byte, followed by
//   - the primary key bytes
//
// The null separator byte ensures that if a secondary key A is longer than B then
// A always sorts after B. Appending the primary key ensures index keys with
// identical secondary keys sort in primary key order. Secondary keys must not
// contain a zero (null) byte.
export function encodeIndexKey(indexKey: IndexKey): string {
  const secondary = indexKey[0];
  const primary = indexKey[1];

  if (secondary.includes('\u0000')) {
    throw new Error('Secondary key cannot contain null byte');
  }
  return KEY_VERSION_0 + secondary + KEY_SEPARATOR + primary;
}

// Returns bytes that can be used to scan for the given secondary index value.
//
// Consider a scan for start_secondary_key="a" (97). We want to scan with scan
// key [0, 97]. We could also scan with [0, 97, 0], but then we couldn't use
// this function for prefix scans, so we lop off the null byte. If we want
// the scan to be exclusive, we scan with the next greater value, [0, 97, 1]
// (we disallow zero bytes in secondary keys).
//
// Now it gets a little tricky. We also want to be able to scan using the
// primary key, start_key. When we do this we have to encode the scan key
// a little differently We essentially have to fix the value of the
// secondary key so we can vary the start_key. That is, the match on
// start_secondary_key becomes an exact match.
//
// Consider the scan for start_secondary_key="a" and start_key=[2]. We want
// to scan with [0, 97, 0, 2]. If we want exclusive we want to scan with
// the next highest value, [0, 97, 0, 2, 0] (zero bytes are allowed in primary
// keys). So far so good. It is important to notice that we need to
// be able to distinguish between not wanting use start_key and wanting to
// use start_key=[]. In the former case we want to scan with the secondary
// key value, possibly followed by a 1 with no trailing zero byte ([0, 97]
// or [0, 97, 1]). In the latter case we want to scan by the secondary
// key value, followed by the zero byte, followed by the primary key value
// and another zero if it is exclusive ([0, 97, 0] or [0, 97, 0, 0]).
// This explains why we need the Option around start_key.
export function encodeIndexScanKey(
  secondary: string,
  primary: string | undefined,
): string {
  const k = encodeIndexKey([secondary, primary || '']);
  if (primary === undefined) {
    return k.slice(0, k.length - 1);
  }
  return k;
}

// Decodes an IndexKey encoded by encode_index_key.
export function decodeIndexKey(encodedIndexKey: string): IndexKey {
  if (encodedIndexKey[0] !== KEY_VERSION_0) {
    throw new Error('Invalid version');
  }

  const versionLen = KEY_VERSION_0.length;
  const separatorLen = KEY_SEPARATOR.length;
  const separatorOffset = encodedIndexKey.indexOf(KEY_SEPARATOR, versionLen);
  if (separatorOffset === -1) {
    throw new Error('Invalid formatting');
  }

  const secondary = encodedIndexKey.slice(versionLen, separatorOffset);
  const primary = encodedIndexKey.slice(separatorOffset + separatorLen);
  return [secondary, primary];
}

export function evaluateJSONPointer(
  value: FrozenJSONValue,
  pointer: string,
): FrozenJSONValue | undefined {
  function parseIndex(s: string): number | undefined {
    if (s.startsWith('+') || (s.startsWith('0') && s.length !== 1)) {
      return undefined;
    }
    return parseInt(s, 10);
  }

  if (pointer === '') {
    return value;
  }
  if (!pointer.startsWith('/')) {
    throw new Error(`Invalid JSON pointer: ${pointer}`);
  }

  const tokens = pointer
    .split('/')
    .slice(1)
    .map(x => x.replace(/~1/g, '/').replace(/~0/g, '~'));

  let target = value;
  for (const token of tokens) {
    let targetOpt;
    if (Array.isArray(target)) {
      const i = parseIndex(token);
      if (i === undefined) {
        return undefined;
      }
      targetOpt = target[i];
    } else if (target === null) {
      return undefined;
    } else if (typeof target === 'object') {
      target = target as FrozenJSONObject;
      targetOpt = target[token];
    }
    if (targetOpt === undefined) {
      return undefined;
    }
    target = targetOpt;
  }
  return target;
}

export const enum IndexOperation {
  Add,
  Remove,
}
