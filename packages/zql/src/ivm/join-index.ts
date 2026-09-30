import {must} from '../../../shared/src/must.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import {canonicalKey} from './join-utils.ts';

/**
 * The partitions of the indexed parents that share a join key, as fetch
 * constraints. An unpartitioned index reports a single `undefined`.
 */
export type ParentPartitions = readonly (Record<string, Value> | undefined)[];

const UNPARTITIONED: ParentPartitions = [undefined];

type Partition = {
  readonly constraint: Record<string, Value>;
  readonly pks: Set<string>;
};

/**
 * An in-memory index from a join key to the parent rows that have it. Join
 * and FlippedJoin look up a child change's join key here to skip the parent
 * fetch when no indexed parent can see the change.
 *
 * Parents are tracked by primary key so that adding or removing a row twice
 * is harmless. Lookups only report whether any parent has the key and, when
 * the index is partitioned, in which partitions.
 */
export class JoinIndex {
  readonly #parentKey: CompoundKey;
  readonly #primaryKey: CompoundKey;
  /** The join key is the primary key, so one string serves as both. */
  readonly #joinKeyIsPK: boolean;
  readonly #partitionKey: CompoundKey | undefined;
  readonly #unpartitioned = new Map<string, Set<string>>();
  readonly #partitioned = new Map<string, Map<string, Partition>>();
  #size = 0;

  constructor(
    parentKey: CompoundKey,
    primaryKey: CompoundKey,
    partitionKey?: CompoundKey | undefined,
  ) {
    this.#parentKey = parentKey;
    this.#primaryKey = primaryKey;
    this.#joinKeyIsPK =
      parentKey.length === primaryKey.length &&
      parentKey.every((k, i) => k === primaryKey[i]);
    this.#partitionKey = partitionKey;
  }

  /** The number of parent rows in the index. */
  get size(): number {
    return this.#size;
  }

  add(row: Row): void {
    const joinKey = joinKeyOf(row, this.#parentKey);
    if (joinKey === undefined) {
      return;
    }
    const pk = this.#pkOf(row, joinKey);
    const partitionKey = this.#partitionKey;
    if (partitionKey === undefined) {
      let pks = this.#unpartitioned.get(joinKey);
      if (pks === undefined) {
        pks = new Set();
        this.#unpartitioned.set(joinKey, pks);
      }
      if (!pks.has(pk)) {
        pks.add(pk);
        this.#size++;
      }
      return;
    }

    const partKey = canonicalKey(row, partitionKey);
    let partitions = this.#partitioned.get(joinKey);
    if (partitions === undefined) {
      partitions = new Map();
      this.#partitioned.set(joinKey, partitions);
    }
    let partition = partitions.get(partKey);
    if (partition === undefined) {
      const constraint: Record<string, Value> = {};
      for (const key of partitionKey) {
        constraint[key] = row[key];
      }
      partition = {constraint, pks: new Set()};
      partitions.set(partKey, partition);
    }
    if (!partition.pks.has(pk)) {
      partition.pks.add(pk);
      this.#size++;
    }
  }

  remove(row: Row): void {
    const joinKey = joinKeyOf(row, this.#parentKey);
    if (joinKey === undefined) {
      return;
    }
    const pk = this.#pkOf(row, joinKey);
    const partitionKey = this.#partitionKey;
    if (partitionKey === undefined) {
      const pks = this.#unpartitioned.get(joinKey);
      if (pks === undefined) {
        return;
      }
      if (pks.delete(pk)) {
        this.#size--;
        if (pks.size === 0) {
          this.#unpartitioned.delete(joinKey);
        }
      }
      return;
    }

    const partitions = this.#partitioned.get(joinKey);
    if (partitions === undefined) {
      return;
    }
    const partKey = canonicalKey(row, partitionKey);
    const partition = partitions.get(partKey);
    if (partition === undefined) {
      return;
    }
    if (partition.pks.delete(pk)) {
      this.#size--;
      if (partition.pks.size === 0) {
        partitions.delete(partKey);
        if (partitions.size === 0) {
          this.#partitioned.delete(joinKey);
        }
      }
    }
  }

  /**
   * The partitions of the indexed parents whose join key matches `childRow`
   * over `childKey`, or `undefined` if there are none.
   */
  lookup(childRow: Row, childKey: CompoundKey): ParentPartitions | undefined {
    const joinKey = joinKeyOf(childRow, childKey);
    if (joinKey === undefined) {
      return undefined;
    }
    if (this.#partitionKey === undefined) {
      return this.#unpartitioned.has(joinKey) ? UNPARTITIONED : undefined;
    }
    const partitions = this.#partitioned.get(joinKey);
    if (partitions === undefined) {
      return undefined;
    }
    return Array.from(partitions.values(), p => p.constraint);
  }

  /**
   * The index as the operator storage entries it replaced, keyed
   * `j\0<joinKey>\0[<partitionKey>\0]<primaryKey>`. For tests.
   */
  entriesForTest(): Record<string, 1> {
    const entries: Record<string, 1> = {};
    for (const [joinKey, pks] of this.#unpartitioned) {
      for (const pk of pks) {
        entries[`j\x00${joinKey}\x00${pk}`] = 1;
      }
    }
    for (const [joinKey, partitions] of this.#partitioned) {
      const partitionKey = must(this.#partitionKey);
      for (const {constraint, pks} of partitions.values()) {
        const partKey = canonicalKey(constraint, partitionKey);
        for (const pk of pks) {
          entries[`j\x00${joinKey}\x00${partKey}\x00${pk}`] = 1;
        }
      }
    }
    return entries;
  }

  #pkOf(row: Row, joinKey: string): string {
    return this.#joinKeyIsPK ? joinKey : canonicalKey(row, this.#primaryKey);
  }
}

/**
 * The canonical join key of `row`, or `undefined` if any part of it is null:
 * a null key cannot match anything.
 */
function joinKeyOf(row: Row, key: CompoundKey): string | undefined {
  for (const k of key) {
    if (row[k] === null) {
      return undefined;
    }
  }
  return canonicalKey(row, key);
}
