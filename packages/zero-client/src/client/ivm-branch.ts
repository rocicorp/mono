import type {
  InternalDiff,
  InternalDiffOperation,
  NoIndexDiff,
} from '../../../replicache/src/btree/node.ts';
import type {LazyStore} from '../../../replicache/src/dag/lazy-store.ts';
import {type Read, type Store} from '../../../replicache/src/dag/store.ts';
import {readFromHash} from '../../../replicache/src/db/read.ts';
import * as FormatVersion from '../../../replicache/src/format-version-enum.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import type {ZeroReadOptions} from '../../../replicache/src/replicache-options.ts';
import {diffBinarySearch} from '../../../replicache/src/subscriptions.ts';
import type {DiffsMap} from '../../../replicache/src/sync/diff.ts';
import {diff} from '../../../replicache/src/sync/diff.ts';
import {using, withRead} from '../../../replicache/src/with-transactions.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {wrapIterable} from '../../../shared/src/iterables.ts';
import {must} from '../../../shared/src/must.ts';
import type {AggregateFunction} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {
  SchemaValue,
  ValueType,
} from '../../../zero-types/src/schema-value.ts';
import {
  isAggregateTableName,
  isTopLevelAggregateTableName,
} from '../../../zql/src/builder/builder.ts';
import {
  AGGREGATE_KEY_COLUMN,
  AGGREGATE_PAYLOAD_COLUMNS,
  AGGREGATE_VALUE_COLUMN,
} from '../../../zql/src/ivm/aggregate.ts';
import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';
import {
  ENTITIES_KEY_PREFIX,
  ROW_VERSION_COLUMN,
  sourceNameFromKey,
} from './keys.ts';

import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../../../zql/src/ivm/source.ts';
/**
 * Replicache needs to rebase mutations onto different
 * commits of it's b-tree. These mutations can have reads
 * in them and those reads must be run against the IVM sources.
 *
 * To ensure the reads get the correct state, the IVM
 * sources need to reflect the state of the commit
 * being rebased onto. `IVMSourceBranch` allows us to:
 * 1. fork the IVM sources
 * 2. patch them up to match the desired head
 * 3. run the reads against the forked sources
 *
 * (2) is expected to be a cheap operation as there should only
 * ever be a few outstanding diffs to apply given Zero is meant
 * to be run in a connected state.
 */
/**
 * Metadata for optimistically updating a synced aggregate when a child row is
 * locally mutated. Registered when a relationship-aggregate query materializes.
 */
export type OptimisticAggregate = {
  /** The synthetic aggregate table, e.g. `aggregate:<queryID>:<alias>`. */
  readonly aggTableName: string;
  /** The correlation child field(s) that key the aggregate rows. */
  readonly childField: readonly string[];
  /** The aggregate function (`count`/`sum`/`avg` are optimistically deltaed). */
  readonly fn: AggregateFunction;
  /** The aggregated field, for `sum`/`avg` (`undefined` for `count`). */
  readonly field: string | undefined;
  /**
   * The aggregate's `where`, compiled to a per-row predicate. A child row
   * contributes only when it returns true; `undefined` means no filter (every
   * child contributes). Only set when the whole `where` is per-row evaluable —
   * a correlated subquery in the `where` makes the aggregate
   * server-authoritative (it is never registered for optimism at all).
   */
  readonly predicate: ((row: Row) => boolean) | undefined;
};

export class IVMSourceBranch {
  readonly #sources: Map<string, MemorySource | undefined>;
  readonly #tables: Record<string, TableSchema>;
  // Registry for optimistic aggregate deltas: child table name -> (synthetic
  // aggregate table name -> metadata). Lets an optimistic child mutation find
  // the aggregate rows it should bump. Shared by reference across forks (it is
  // query metadata, independent of branch row state).
  readonly #aggregateOptimism: Map<string, Map<string, OptimisticAggregate>>;
  #advanceError: unknown;
  hash: Hash | undefined;

  constructor(
    tables: Record<string, TableSchema>,
    hash?: Hash,
    sources: Map<string, MemorySource | undefined> = new Map(),
    aggregateOptimism: Map<
      string,
      Map<string, OptimisticAggregate>
    > = new Map(),
  ) {
    this.#tables = tables;
    this.#sources = sources;
    this.#aggregateOptimism = aggregateOptimism;
    this.hash = hash;
  }

  getSource(name: string): MemorySource | undefined {
    this.#throwIfInvalid();

    if (this.#sources.has(name)) {
      return this.#sources.get(name);
    }

    const schema = this.#tables[name];
    let source: MemorySource | undefined;
    if (schema) {
      source = new MemorySource(name, schema.columns, schema.primaryKey);
    } else if (isTopLevelAggregateTableName(name)) {
      // Synthetic source for a synced top-level aggregate (e.g. `count()`). It
      // is not in the static schema; its shape is fixed (the singleton key
      // column + the result value), so it can be provisioned from the name
      // alone — including at reload time, before the query re-materializes. The
      // server streams the precomputed value here; the underlying rows are never
      // synced.
      source = new MemorySource(
        name,
        {
          [AGGREGATE_KEY_COLUMN]: {type: 'number'},
          [AGGREGATE_VALUE_COLUMN]: {type: 'number'},
        },
        [AGGREGATE_KEY_COLUMN],
      );
    } else {
      source = undefined;
    }
    this.#sources.set(name, source);
    return source;
  }

  /**
   * Get-or-create the synthetic source for a *relationship* aggregate
   * (`aggregate:<queryID>:<alias>`), whose shape (the correlation-key columns +
   * value) cannot be derived from the name. The query builder supplies the
   * schema when a query materializes; {@link applyDiffs} supplies an inferred
   * one when persisted rows are replayed before the query re-materializes (e.g.
   * on reload). Idempotent — the first caller wins.
   */
  getOrCreateAggregateSource(
    name: string,
    columns: Record<string, SchemaValue>,
    primaryKey: PrimaryKey,
    optimistic?: {
      readonly table: string;
      readonly childField: readonly string[];
      readonly fn: AggregateFunction;
      readonly field: string | undefined;
      readonly predicate: ((row: Row) => boolean) | undefined;
    },
  ): MemorySource {
    if (optimistic) {
      let byTable = this.#aggregateOptimism.get(optimistic.table);
      if (!byTable) {
        byTable = new Map();
        this.#aggregateOptimism.set(optimistic.table, byTable);
      }
      // Keyed by aggregate table name so re-registering the same query is a
      // no-op (idempotent).
      byTable.set(name, {
        aggTableName: name,
        childField: optimistic.childField,
        fn: optimistic.fn,
        field: optimistic.field,
        predicate: optimistic.predicate,
      });
    }
    const existing = this.#sources.get(name);
    if (existing) {
      return existing;
    }
    const source = new MemorySource(name, columns, primaryKey);
    this.#sources.set(name, source);
    return source;
  }

  /**
   * The optimistic-aggregate registrations whose child rows live in `table`.
   * An optimistic insert/delete of such a child row bumps these aggregates.
   */
  getOptimisticAggregates(table: string): Iterable<OptimisticAggregate> {
    return this.#aggregateOptimism.get(table)?.values() ?? [];
  }

  clear() {
    this.#sources.clear();
  }

  #throwIfInvalid() {
    if (this.#advanceError) {
      throw this.#advanceError;
    }
  }

  /**
   * Mutates the current branch, advancing it to the new head
   * by applying the given diffs.
   */
  advance(expectedHead: Hash | undefined, newHead: Hash, diffs: NoIndexDiff) {
    this.#throwIfInvalid();

    assert(
      this.hash === expectedHead,
      () =>
        `Expected head must match the main head. Got: ${this.hash}, expected: ${expectedHead}`,
    );

    try {
      applyDiffs(diffs, this);
      this.hash = newHead;
    } catch (e) {
      this.#advanceError = e;
      this.clear();
      throw e;
    }
  }

  /**
   * Fork the branch and patch it up to match the desired head.
   */
  async forkToHead(
    store: LazyStore,
    desiredHead: Hash,
    readOptions?: ZeroReadOptions,
  ): Promise<IVMSourceBranch> {
    this.#throwIfInvalid();

    const fork = this.fork();

    if (fork.hash === desiredHead) {
      return fork;
    }

    await patchBranch(desiredHead, store, fork, readOptions);
    fork.hash = desiredHead;
    return fork;
  }

  /**
   * Creates a new IVMSourceBranch that is a copy of the current one.
   * This is a cheap operation since the b-trees are shared until a write is performed
   * and then only the modified nodes are copied.
   *
   * IVM branches are forked when we need to rebase mutations.
   * The mutations modify the fork rather than original branch.
   */
  fork() {
    this.#throwIfInvalid();

    return new IVMSourceBranch(
      this.#tables,
      this.hash,
      new Map(
        wrapIterable(this.#sources.entries()).map(([name, source]) => [
          name,
          source?.fork(),
        ]),
      ),
      // Share the optimistic-aggregate registry by reference: it is query
      // metadata, the same for the fork as for the main branch.
      this.#aggregateOptimism,
    );
  }
}

export async function initFromStore(
  branch: IVMSourceBranch,
  hash: Hash,
  store: Store,
) {
  const diffs: InternalDiffOperation[] = [];
  await withRead(store, async dagRead => {
    const read = await readFromHash(hash, dagRead, FormatVersion.Latest);
    for await (const entry of read.map.scan(ENTITIES_KEY_PREFIX)) {
      if (!entry[0].startsWith(ENTITIES_KEY_PREFIX)) {
        break;
      }
      diffs.push({
        op: 'add',
        key: entry[0],
        newValue: entry[1],
      });
    }
  });

  branch.advance(undefined, hash, diffs);
}

async function patchBranch(
  desiredHead: Hash,
  store: LazyStore,
  fork: IVMSourceBranch,
  readOptions: ZeroReadOptions | undefined,
) {
  const diffs = await computeDiffs(
    must(fork.hash),
    desiredHead,
    store,
    readOptions,
  );
  if (!diffs) {
    return;
  }
  applyDiffs(diffs, fork);
}

async function computeDiffs(
  startHash: Hash,
  endHash: Hash,
  store: LazyStore,
  readOptions: ZeroReadOptions | undefined,
): Promise<InternalDiff | undefined> {
  const readFn = (dagRead: Read) =>
    diff(
      startHash,
      endHash,
      dagRead,
      {
        shouldComputeDiffs: () => true,
        shouldComputeDiffsForIndex(_name) {
          return false;
        },
      },
      FormatVersion.Latest,
    );

  let diffs: DiffsMap;
  if (readOptions?.openLazySourceRead) {
    diffs = await using(store.read(readOptions.openLazySourceRead), readFn);
  } else if (readOptions?.openLazyRead) {
    diffs = await readFn(readOptions.openLazyRead);
  } else {
    diffs = await withRead(store, readFn);
  }

  return diffs.get('');
}

function applyDiffs(diffs: NoIndexDiff, branch: IVMSourceBranch) {
  for (
    let i = diffBinarySearch(diffs, ENTITIES_KEY_PREFIX, diff => diff.key);
    i < diffs.length;
    i++
  ) {
    const diff = diffs[i];
    const {key} = diff;
    if (!key.startsWith(ENTITIES_KEY_PREFIX)) {
      break;
    }
    const name = sourceNameFromKey(key);
    let source = branch.getSource(name);
    if (!source && isAggregateTableName(name)) {
      // A relationship aggregate source isn't derivable from the name, so it
      // may not exist yet when persisted rows are replayed before the query
      // re-materializes (reload). Provision it from the row's own shape; the
      // builder will reuse this same source when the query materializes.
      const row = (diff.op === 'del' ? diff.oldValue : diff.newValue) as Row;
      source = branch.getOrCreateAggregateSource(
        name,
        ...inferAggregateSchema(row),
      );
    }
    source = must(source);
    switch (diff.op) {
      case 'del':
        consume(source.push(makeSourceChangeRemove(diff.oldValue as Row)));
        break;
      case 'add':
        consume(source.push(makeSourceChangeAdd(diff.newValue as Row)));
        break;
      case 'change':
        consume(
          source.push(
            makeSourceChangeEdit(diff.newValue as Row, diff.oldValue as Row),
          ),
        );
        break;
    }
  }
}

/**
 * Infers a synthetic aggregate source's schema from one of its rows: the key
 * columns are every column except the result `value` and the row-version, with
 * types read off the actual values. Used only on the reload path, where a
 * relationship aggregate's rows are replayed before its query (which would
 * otherwise supply the exact schema) materializes.
 */
function inferAggregateSchema(
  row: Row,
): [columns: Record<string, SchemaValue>, primaryKey: PrimaryKey] {
  const columns: Record<string, SchemaValue> = {};
  const keyColumns: string[] = [];
  for (const k of Object.keys(row)) {
    if (k === ROW_VERSION_COLUMN) {
      continue;
    }
    if (AGGREGATE_PAYLOAD_COLUMNS.has(k)) {
      // value (and, for avg, sum/count) — numeric payload, never part of the key.
      columns[k] = {type: 'number', optional: true};
    } else {
      columns[k] = {type: inferValueType(row[k])};
      keyColumns.push(k);
    }
  }
  keyColumns.sort();
  // The result column is always present even if absent from this particular row.
  if (!(AGGREGATE_VALUE_COLUMN in columns)) {
    columns[AGGREGATE_VALUE_COLUMN] = {type: 'number', optional: true};
  }
  return [columns, keyColumns as unknown as PrimaryKey];
}

function inferValueType(v: Value): ValueType {
  switch (typeof v) {
    case 'string':
      return 'string';
    case 'number':
      return 'number';
    case 'boolean':
      return 'boolean';
    default:
      return 'json';
  }
}
