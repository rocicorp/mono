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

/**
 * The canonical primary keys of the parents under one key. A lone parent,
 * the common case, is kept as a plain string rather than a one-element Set.
 */
type PKs = string | Set<string>;

type Partition = {
  readonly constraint: Record<string, Value>;
  pks: PKs;
};

/**
 * The partitions under one join key. A lone partition, the common case, is
 * kept as is rather than in a one-entry map keyed by its canonical key.
 */
type Partitions = Partition | Map<string, Partition>;

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
  readonly #unpartitioned = new Map<string, PKs>();
  readonly #partitioned = new Map<string, Partitions>();
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
      const pks = addPK(this.#unpartitioned.get(joinKey), pk);
      if (pks !== undefined) {
        this.#unpartitioned.set(joinKey, pks);
        this.#size++;
      }
      return;
    }

    const partKey = canonicalKey(row, partitionKey);
    const partitions = this.#partitioned.get(joinKey);
    const partition =
      partitions && findPartition(partitions, partKey, partitionKey);
    if (partition) {
      const pks = addPK(partition.pks, pk);
      if (pks !== undefined) {
        partition.pks = pks;
        this.#size++;
      }
      return;
    }
    const constraint: Record<string, Value> = {};
    for (const key of partitionKey) {
      constraint[key] = row[key];
    }
    const added: Partition = {constraint, pks: pk};
    if (partitions === undefined) {
      this.#partitioned.set(joinKey, added);
    } else if (partitions instanceof Map) {
      partitions.set(partKey, added);
    } else {
      this.#partitioned.set(
        joinKey,
        new Map([
          [canonicalKey(partitions.constraint, partitionKey), partitions],
          [partKey, added],
        ]),
      );
    }
    this.#size++;
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
      const rest = removePK(pks, pk);
      if (rest === undefined) {
        return;
      }
      if (rest === null) {
        this.#unpartitioned.delete(joinKey);
      } else {
        this.#unpartitioned.set(joinKey, rest);
      }
      this.#size--;
      return;
    }

    const partitions = this.#partitioned.get(joinKey);
    if (partitions === undefined) {
      return;
    }
    const partKey = canonicalKey(row, partitionKey);
    const partition = findPartition(partitions, partKey, partitionKey);
    if (partition === undefined) {
      return;
    }
    const rest = removePK(partition.pks, pk);
    if (rest === undefined) {
      return;
    }
    if (rest !== null) {
      partition.pks = rest;
    } else if (!(partitions instanceof Map)) {
      this.#partitioned.delete(joinKey);
    } else {
      partitions.delete(partKey);
      if (partitions.size === 1) {
        const [last] = partitions.values();
        this.#partitioned.set(joinKey, last);
      }
    }
    this.#size--;
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
    return partitions instanceof Map
      ? Array.from(partitions.values(), p => p.constraint)
      : [partitions.constraint];
  }

  /**
   * The index as the operator storage entries it replaced, keyed
   * `j\0<joinKey>\0[<partitionKey>\0]<primaryKey>`. For tests.
   */
  entriesForTest(): Record<string, 1> {
    const entries: Record<string, 1> = {};
    const addEntries = (prefix: string, pks: PKs) => {
      for (const pk of typeof pks === 'string' ? [pks] : pks) {
        entries[`${prefix}${pk}`] = 1;
      }
    };
    for (const [joinKey, pks] of this.#unpartitioned) {
      addEntries(`j\x00${joinKey}\x00`, pks);
    }
    for (const [joinKey, partitions] of this.#partitioned) {
      const partitionKey = must(this.#partitionKey);
      for (const {constraint, pks} of partitions instanceof Map
        ? partitions.values()
        : [partitions]) {
        const partKey = canonicalKey(constraint, partitionKey);
        addEntries(`j\x00${joinKey}\x00${partKey}\x00`, pks);
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

function findPartition(
  partitions: Partitions,
  partKey: string,
  partitionKey: CompoundKey,
): Partition | undefined {
  if (partitions instanceof Map) {
    return partitions.get(partKey);
  }
  return canonicalKey(partitions.constraint, partitionKey) === partKey
    ? partitions
    : undefined;
}

/** Adds `pk`. Returns the new PKs, or `undefined` if `pk` was present. */
function addPK(pks: PKs | undefined, pk: string): PKs | undefined {
  if (pks === undefined) {
    return pk;
  }
  if (typeof pks === 'string') {
    return pks === pk ? undefined : new Set([pks, pk]);
  }
  return pks.has(pk) ? undefined : pks.add(pk);
}

/**
 * Removes `pk`. Returns `undefined` if `pk` was absent, `null` if it was the
 * last one, and the remaining PKs otherwise.
 */
function removePK(pks: PKs, pk: string): PKs | null | undefined {
  if (typeof pks === 'string') {
    return pks === pk ? null : undefined;
  }
  if (!pks.delete(pk)) {
    return undefined;
  }
  if (pks.size === 1) {
    const [last] = pks;
    return last;
  }
  return pks;
}
