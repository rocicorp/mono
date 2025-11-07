import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import type {SourceSchema} from './schema.ts';
import {take, type Stream} from './stream.ts';
import {compareValues, valuesEqual, type Node} from './data.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import {type Storage} from './operator.ts';

export type JoinChangeOverlay = {
  change: Change;
  position: Row | undefined;
};

export function* generateWithOverlay(
  stream: Stream<Node>,
  overlay: Change,
  schema: SourceSchema,
): Stream<Node> {
  let applied = false;
  let editOldApplied = false;
  let editNewApplied = false;
  for (const node of stream) {
    let yieldNode = true;
    if (!applied) {
      switch (overlay.type) {
        case 'add': {
          if (schema.compareRows(overlay.node.row, node.row) === 0) {
            applied = true;
            yieldNode = false;
          }
          break;
        }
        case 'remove': {
          if (schema.compareRows(overlay.node.row, node.row) < 0) {
            applied = true;
            yield overlay.node;
          }
          break;
        }
        case 'edit': {
          if (
            !editOldApplied &&
            schema.compareRows(overlay.oldNode.row, node.row) < 0
          ) {
            editOldApplied = true;
            if (editNewApplied) {
              applied = true;
            }
            yield overlay.oldNode;
          }
          if (
            !editNewApplied &&
            schema.compareRows(overlay.node.row, node.row) === 0
          ) {
            editNewApplied = true;
            if (editOldApplied) {
              applied = true;
            }
            yieldNode = false;
          }
          break;
        }
        case 'child': {
          if (schema.compareRows(overlay.node.row, node.row) === 0) {
            applied = true;
            yield {
              row: node.row,
              relationships: {
                ...node.relationships,
                [overlay.child.relationshipName]: () =>
                  generateWithOverlay(
                    node.relationships[overlay.child.relationshipName](),
                    overlay.child.change,
                    schema.relationships[overlay.child.relationshipName],
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
    if (overlay.type === 'remove') {
      applied = true;
      yield overlay.node;
    } else if (overlay.type === 'edit') {
      assert(editNewApplied);
      editOldApplied = true;
      applied = true;
      yield overlay.oldNode;
    }
  }

  assert(applied);
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

export class KeySet<V extends CompoundKey | undefined> {
  readonly #storage: Storage;
  readonly #name: string;
  readonly #setKey: CompoundKey;
  readonly #primaryKey: CompoundKey;
  readonly #valueKey: V;

  constructor(
    storage: Storage,
    name: string,
    setKey: CompoundKey,
    primaryKey: CompoundKey,
    valueKey: V,
  ) {
    this.#storage = storage;
    this.#name = name;
    this.#setKey = setKey;
    this.#primaryKey = primaryKey;
    this.#valueKey = valueKey;
  }

  add(row: Row): void {
    this.#storage.set(this.#makeKeySetStorageKey(row), true);
  }

  delete(row: Row): void {
    this.#storage.del(this.#makeKeySetStorageKey(row));
  }

  /**
   * Returns an iterable of all members for a set identified by the row data.
   * The `row` only needs to contain values for the `setKey`.
   */
  *getValues(row: Row): Iterable<{
    readonly [key: string]: Value;
  }> {
    if (this.#valueKey === undefined) {
      return;
    }
    const prefix = this.#makeKeySetStorageKeyPrefix(row);
    let lastValuesStringified = undefined;
    for (const [key] of this.#storage.scan({prefix})) {
      // Parse the part of the key *after* the prefix,
      // which represents the member key values.
      const valuesStringified = JSON.parse(
        '[' + key.substring(prefix.length, key.length - 1) + ']',
      )[0];
      if (valuesStringified === lastValuesStringified) {
        continue;
      }
      lastValuesStringified = valuesStringified;
      const values = JSON.parse(valuesStringified);
      yield Object.fromEntries(
        this.#valueKey.map((key, i) => [key, values[i]]),
      );
    }
  }

  /**
   * Checks if a set identified by the row data is empty.
   * The `row` only needs to contain values for the `setKey`.
   */
  isEmpty(row: Row): boolean {
    const prefix = this.#makeKeySetStorageKeyPrefix(row);
    // Get the iterator from the scan
    const iterator = this.#storage.scan({prefix})[Symbol.iterator]();
    // Check the first item. If `done` is true, the iterable is empty.
    return iterator.next().done === true;
  }

  #makeKeySetStorageKey(row: Row): string {
    const setKeyValues: Value[] = this.#setKey.map(k => row[k]);

    const primaryKeyValues: Value[] = [];
    for (const key of this.#primaryKey) {
      primaryKeyValues.push(row[key]);
    }

    if (this.#valueKey === undefined) {
      return KeySet.#makeKeySetStorageKeyForValues(this.#name, [
        setKeyValues,
        primaryKeyValues,
      ]);
    }
    const valueKeyValues: Value[] = [];
    for (const key of this.#valueKey) {
      valueKeyValues.push(row[key]);
    }
    return KeySet.#makeKeySetStorageKeyForValues(this.#name, [
      setKeyValues,
      valueKeyValues,
      primaryKeyValues,
    ]);
  }

  /**
   * Builds the storage key prefix for a specific set.
   * This is used for scanning all members of a set.
   * Format: "setName","setValue1",...,
   */
  #makeKeySetStorageKeyPrefix(row: Row): string {
    return KeySet.#makeKeySetStorageKeyForValues(this.#name, [
      this.#setKey.map(k => row[k]),
    ]);
  }

  /**
   * The core utility for serializing key parts into the storage format.
   * Example: (setName: "users", values: [123, "abc"])
   * Becomes: JSON.stringify(["users", 123, "abc"]) -> '["users",123,"abc"]'
   * Returns: '"users",123,"abc",'
   */
  static #makeKeySetStorageKeyForValues(
    setName: string,
    valueArrays: readonly Value[][],
  ): string {
    const stringified = valueArrays.map(v => JSON.stringify(v));
    const json = JSON.stringify([setName, ...stringified]);
    // Removes leading '[' and trailing ']' and appends a comma
    // to create the prefix or full key
    return json.substring(1, json.length - 1) + ',';
  }
}
