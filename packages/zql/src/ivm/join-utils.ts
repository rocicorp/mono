import {assert} from '../../../shared/src/asserts.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import {compareValues, valuesEqual} from './data.ts';
import type {Stream} from './stream.ts';

export function rowEqualsForCompoundKey(
  a: Row,
  b: Row,
  key: CompoundKey,
): boolean {
  for (let i = 0; i < key.length; i++) {
    if (compareValues(a[key[i]], b[key[i]]) !== 0) {
      return false;
    }
  }
  return true;
}

export function isJoinMatch(
  parent: Row,
  parentKey: CompoundKey,
  child: Row,
  childKey: CompoundKey,
) {
  for (let i = 0; i < parentKey.length; i++) {
    if (!valuesEqual(parent[parentKey[i]], child[childKey[i]])) {
      return false;
    }
  }
  return true;
}

/**
 * Builds a constraint object by mapping values from `sourceRow` using `sourceKey`
 * to keys specified by `targetKey`. Returns `undefined` if any source value is `null`,
 * since null foreign keys cannot match any rows.
 */
export function buildJoinConstraint(
  sourceRow: Row,
  sourceKey: CompoundKey,
  targetKey: CompoundKey,
): Record<string, Value> | undefined {
  const constraint: Record<string, Value> = {};
  for (let i = 0; i < targetKey.length; i++) {
    const value = sourceRow[sourceKey[i]];
    if (value === null) {
      return undefined;
    }
    constraint[targetKey[i]] = value;
  }
  return constraint;
}

export function canonicalKeyForTest(
  record: Record<string, Value | undefined>,
  keys: CompoundKey,
): string {
  return canonicalKey(record, keys);
}

/**
 * Canonical string key over `keys` of `record`. Tags values by type
 * so distinct types (e.g. 1 and "1") do not collide.
 */
export function canonicalKey(
  record: Record<string, Value | undefined>,
  keys: CompoundKey,
): string {
  if (keys.length === 1) {
    return canonicalValue(record[keys[0]]);
  }
  let s = '';
  for (let i = 0; i < keys.length; i++) {
    if (i > 0) s += '\x00';
    s += canonicalValue(record[keys[i]]);
  }
  return s;
}

function canonicalValue(v: Value): string {
  // Tag by type so we don't conflate e.g. `1` (number) with `"1"` (string).
  if (v === null || v === undefined) return 'n';
  const t = typeof v;
  if (t === 'string') return 's' + (v as string);
  if (t === 'number') return 'd' + (v as number);
  if (t === 'boolean') return v ? 't' : 'f';
  return 'j' + JSON.stringify(v);
}

export interface JoinStorage {
  get(key: string): unknown;
  set(key: string, value: unknown): void;
  del(key: string): void;
  scan(options?: {prefix: string}): Stream<[string, unknown]>;
}

export function makeUnpartitionedStorageKey(
  joinKey: string,
  primaryKey: string,
): string {
  return `j\x00${joinKey}\x00${primaryKey}`;
}

export function makeJoinPrefix(joinKey: string): string {
  return `j\x00${joinKey}\x00`;
}

export function makePartitionStorageKey(
  joinKey: string,
  partitionKey: string,
  primaryKey: string,
): string {
  return `j\x00${joinKey}\x00${partitionKey}\x00${primaryKey}`;
}

export function splitPartitionAndPk(
  suffix: string,
  numPartitionKeys: number,
): [partitionKey: string, pk: string] {
  let idx = 0;
  for (let i = 0; i < numPartitionKeys; i++) {
    const next = suffix.indexOf('\x00', idx);
    assert(next !== -1, 'Malformed join storage key: missing delimiter');
    if (i === numPartitionKeys - 1) {
      return [suffix.slice(0, next), suffix.slice(next + 1)];
    }
    idx = next + 1;
  }
  throw new Error('Malformed join storage key');
}

export function decodeCanonicalValue(s: string): Value {
  const tag = s[0];
  const rest = s.slice(1);
  switch (tag) {
    case 's':
      return rest;
    case 'd':
      return Number(rest);
    case 'n':
      return null;
    case 't':
      return true;
    case 'f':
      return false;
    case 'j':
      return JSON.parse(rest);
    default:
      throw new Error(`Unknown canonical tag: ${tag}`);
  }
}

export function decodePartitionConstraint(
  partitionKey: string,
  keys: CompoundKey,
): Record<string, Value | undefined> {
  const parts = keys.length === 1 ? [partitionKey] : partitionKey.split('\x00');
  const constraint: Record<string, Value | undefined> = {};
  for (let i = 0; i < keys.length; i++) {
    constraint[keys[i]] = decodeCanonicalValue(parts[i]);
  }
  return constraint;
}

export class JoinIndex {
  readonly #storage: JoinStorage;
  readonly #parentKey: CompoundKey;
  readonly #primaryKey: CompoundKey;
  readonly #parentPartitionKey?: CompoundKey | undefined;

  constructor(
    storage: JoinStorage,
    parentKey: CompoundKey,
    primaryKey: CompoundKey,
    parentPartitionKey?: CompoundKey,
  ) {
    this.#storage = storage;
    this.#parentKey = parentKey;
    this.#primaryKey = primaryKey;
    this.#parentPartitionKey = parentPartitionKey;
  }

  index(row: Row, value: number = 1): void {
    const key = this.#makeKey(row);
    if (key) {
      this.#storage.set(key, value);
    }
  }

  unindex(row: Row): void {
    const key = this.#makeKey(row);
    if (key) {
      this.#storage.del(key);
    }
  }

  get(row: Row): number | undefined {
    const key = this.#makeKey(row);
    return key ? (this.#storage.get(key) as number | undefined) : undefined;
  }

  has(row: Row): boolean {
    return this.get(row) !== undefined;
  }

  increment(row: Row): {oldCount: number; newCount: number} {
    const oldCount = this.get(row) ?? 0;
    const newCount = oldCount + 1;
    this.index(row, newCount);
    return {oldCount, newCount};
  }

  decrement(row: Row): {oldCount: number | undefined; newCount: number} {
    const current = this.get(row);
    if (current === undefined) {
      return {oldCount: undefined, newCount: 0};
    }
    const newCount = current - 1;
    if (newCount > 0) {
      this.index(row, newCount);
    } else {
      this.unindex(row);
    }
    return {oldCount: current, newCount: Math.max(0, newCount)};
  }

  getMatching(
    childRow: Row,
    childKey: CompoundKey,
  ): MatchingParentEntry[] | undefined {
    return getMatchingParentEntries(
      this.#storage,
      childRow,
      childKey,
      this.#parentPartitionKey,
    );
  }

  delEntry(
    joinKey: string,
    pk: string,
    partitionConstraint?: Record<string, Value | undefined>,
  ): void {
    const storageKey = this.#parentPartitionKey
      ? makePartitionStorageKey(
          joinKey,
          canonicalKey(partitionConstraint!, this.#parentPartitionKey),
          pk,
        )
      : makeUnpartitionedStorageKey(joinKey, pk);
    this.#storage.del(storageKey);
  }

  #makeKey(row: Row): string | undefined {
    if (this.#parentKey.some(k => row[k] === null)) {
      return undefined;
    }
    const joinKey = canonicalKey(row, this.#parentKey);
    const parentPk = canonicalKey(row, this.#primaryKey);
    return this.#parentPartitionKey
      ? makePartitionStorageKey(
          joinKey,
          canonicalKey(row, this.#parentPartitionKey),
          parentPk,
        )
      : makeUnpartitionedStorageKey(joinKey, parentPk);
  }
}

export function indexParentInStorage(
  storage: JoinStorage,
  row: Row,
  parentKey: CompoundKey,
  primaryKey: CompoundKey,
  parentPartitionKey?: CompoundKey,
  count: number = 1,
): void {
  new JoinIndex(storage, parentKey, primaryKey, parentPartitionKey).index(
    row,
    count,
  );
}

export function unindexParentInStorage(
  storage: JoinStorage,
  row: Row,
  parentKey: CompoundKey,
  primaryKey: CompoundKey,
  parentPartitionKey?: CompoundKey,
): void {
  new JoinIndex(storage, parentKey, primaryKey, parentPartitionKey).unindex(
    row,
  );
}

export type MatchingParentEntry = {
  pks: Set<string>;
  partitionConstraint?: Record<string, Value | undefined> | undefined;
};

export function getMatchingParentEntries(
  storage: JoinStorage,
  childRow: Row,
  childKey: CompoundKey,
  parentPartitionKey?: CompoundKey,
): MatchingParentEntry[] | undefined {
  if (childKey.some(k => childRow[k] === null)) {
    return undefined;
  }
  const joinKey = canonicalKey(childRow, childKey);
  const prefix = makeJoinPrefix(joinKey);

  if (!parentPartitionKey) {
    const pks = new Set<string>();
    for (const [key] of storage.scan({prefix})) {
      const pk = key.slice(prefix.length);
      pks.add(pk);
    }
    return pks.size > 0 ? [{pks}] : undefined;
  }

  const entries: MatchingParentEntry[] = [];
  let currentPartitionKey: string | undefined;
  let currentPks: Set<string> | undefined;

  for (const [key] of storage.scan({prefix})) {
    const suffix = key.slice(prefix.length);
    const [partitionKey, pk] = splitPartitionAndPk(
      suffix,
      parentPartitionKey.length,
    );
    if (partitionKey !== currentPartitionKey) {
      if (
        currentPartitionKey !== undefined &&
        currentPks &&
        currentPks.size > 0
      ) {
        entries.push({
          pks: currentPks,
          partitionConstraint: decodePartitionConstraint(
            currentPartitionKey,
            parentPartitionKey,
          ),
        });
      }
      currentPartitionKey = partitionKey;
      currentPks = new Set<string>();
    }
    currentPks?.add(pk);
  }
  if (currentPartitionKey !== undefined && currentPks && currentPks.size > 0) {
    entries.push({
      pks: currentPks,
      partitionConstraint: decodePartitionConstraint(
        currentPartitionKey,
        parentPartitionKey,
      ),
    });
  }
  return entries.length > 0 ? entries : undefined;
}
