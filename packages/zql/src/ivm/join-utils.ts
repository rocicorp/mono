import {assert} from '../../../shared/src/asserts.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import type {Change} from './change.ts';
import {compareValues, valuesEqual, type Node} from './data.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

export function generateWithOverlayNoYield(
  stream: Stream<Node>,
  overlay: Change,
  schema: SourceSchema,
): Stream<Node> {
  return generateWithOverlay(stream, overlay, schema) as Stream<Node>;
}

export function* generateWithOverlay(
  stream: Stream<Node | 'yield'>,
  overlay: Change,
  schema: SourceSchema,
): Stream<Node | 'yield'> {
  let applied = false;
  let editOldApplied = false;
  let editNewApplied = false;
  for (const node of stream) {
    if (node === 'yield') {
      yield node;
      continue;
    }
    let yieldNode = true;
    if (!applied) {
      switch (overlay[ChangeIndex.TYPE]) {
        case ChangeType.ADD: {
          if (
            schema.compareRows(overlay[ChangeIndex.NODE].row, node.row) === 0
          ) {
            applied = true;
            yieldNode = false;
          }
          break;
        }
        case ChangeType.REMOVE: {
          if (schema.compareRows(overlay[ChangeIndex.NODE].row, node.row) < 0) {
            applied = true;
            yield overlay[ChangeIndex.NODE];
          }
          break;
        }
        case ChangeType.EDIT: {
          if (
            !editOldApplied &&
            schema.compareRows(overlay[ChangeIndex.OLD_NODE].row, node.row) < 0
          ) {
            editOldApplied = true;
            if (editNewApplied) {
              applied = true;
            }
            yield overlay[ChangeIndex.OLD_NODE];
          }
          if (
            !editNewApplied &&
            schema.compareRows(overlay[ChangeIndex.NODE].row, node.row) === 0
          ) {
            editNewApplied = true;
            if (editOldApplied) {
              applied = true;
            }
            yieldNode = false;
          }
          break;
        }
        case ChangeType.CHILD: {
          if (
            schema.compareRows(overlay[ChangeIndex.NODE].row, node.row) === 0
          ) {
            applied = true;
            yield {
              row: node.row,
              relationships: {
                ...node.relationships,
                [overlay[ChangeIndex.CHILD_DATA].relationshipName]: () =>
                  generateWithOverlay(
                    node.relationships[
                      overlay[ChangeIndex.CHILD_DATA].relationshipName
                    ](),
                    overlay[ChangeIndex.CHILD_DATA].change,
                    schema.relationships[
                      overlay[ChangeIndex.CHILD_DATA].relationshipName
                    ],
                  ),
              },
            };
            yieldNode = false;
          }
          break;
        }
      }
    }
    if (yieldNode) {
      yield node;
    }
  }
  if (!applied) {
    if (overlay[ChangeIndex.TYPE] === ChangeType.REMOVE) {
      applied = true;
      yield overlay[ChangeIndex.NODE];
    } else if (overlay[ChangeIndex.TYPE] === ChangeType.EDIT) {
      assert(
        editNewApplied,
        'edit overlay: new node must be applied before old node',
      );
      editOldApplied = true;
      applied = true;
      yield overlay[ChangeIndex.OLD_NODE];
    }
  }

  assert(
    applied,
    'overlayGenerator: overlay was never applied to any fetched node',
  );
}

export function generateWithOverlayNoYieldUnordered(
  stream: Stream<Node>,
  overlay: Change,
  schema: SourceSchema,
): Stream<Node> {
  return generateWithOverlayUnordered(stream, overlay, schema) as Stream<Node>;
}

export function* generateWithOverlayUnordered(
  stream: Stream<Node | 'yield'>,
  overlay: Change,
  schema: SourceSchema,
): Stream<Node | 'yield'> {
  // Eager inject
  if (overlay[ChangeIndex.TYPE] === ChangeType.REMOVE) {
    yield overlay[ChangeIndex.NODE];
  } else if (overlay[ChangeIndex.TYPE] === ChangeType.EDIT) {
    yield overlay[ChangeIndex.OLD_NODE];
  }

  // Stream with inline suppress
  let suppressed = false;
  for (const node of stream) {
    if (node === 'yield') {
      yield node;
      continue;
    }
    if (!suppressed) {
      if (
        overlay[ChangeIndex.TYPE] === ChangeType.ADD ||
        overlay[ChangeIndex.TYPE] === ChangeType.EDIT
      ) {
        if (
          rowEqualsForCompoundKey(
            overlay[ChangeIndex.NODE].row,
            node.row,
            schema.primaryKey,
          )
        ) {
          suppressed = true;
          continue;
        }
      }
      if (overlay[ChangeIndex.TYPE] === ChangeType.CHILD) {
        if (
          rowEqualsForCompoundKey(
            overlay[ChangeIndex.NODE].row,
            node.row,
            schema.primaryKey,
          )
        ) {
          suppressed = true;
          yield {
            row: node.row,
            relationships: {
              ...node.relationships,
              [overlay[ChangeIndex.CHILD_DATA].relationshipName]: () =>
                generateWithOverlay(
                  node.relationships[
                    overlay[ChangeIndex.CHILD_DATA].relationshipName
                  ](),
                  overlay[ChangeIndex.CHILD_DATA].change,
                  schema.relationships[
                    overlay[ChangeIndex.CHILD_DATA].relationshipName
                  ],
                ),
            },
          };
          continue;
        }
      }
    }
    yield node;
  }
  assert(
    suppressed || overlay[ChangeIndex.TYPE] === ChangeType.REMOVE,
    'overlayGenerator: overlay was never applied to any fetched node',
  );
}

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

export function indexParentInStorage(
  storage: JoinStorage,
  row: Row,
  parentKey: CompoundKey,
  primaryKey: CompoundKey,
  parentPartitionKey?: CompoundKey,
): void {
  if (parentKey.some(k => row[k] === null)) {
    return;
  }
  const joinKey = canonicalKey(row, parentKey);
  const parentPk = canonicalKey(row, primaryKey);
  const storageKey = parentPartitionKey
    ? makePartitionStorageKey(
        joinKey,
        canonicalKey(row, parentPartitionKey),
        parentPk,
      )
    : makeUnpartitionedStorageKey(joinKey, parentPk);
  storage.set(storageKey, 1);
}

export function unindexParentInStorage(
  storage: JoinStorage,
  row: Row,
  parentKey: CompoundKey,
  primaryKey: CompoundKey,
  parentPartitionKey?: CompoundKey,
): void {
  if (parentKey.some(k => row[k] === null)) {
    return;
  }
  const joinKey = canonicalKey(row, parentKey);
  const parentPk = canonicalKey(row, primaryKey);
  const storageKey = parentPartitionKey
    ? makePartitionStorageKey(
        joinKey,
        canonicalKey(row, parentPartitionKey),
        parentPk,
      )
    : makeUnpartitionedStorageKey(joinKey, parentPk);
  storage.del(storageKey);
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
