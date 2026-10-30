import {unreachable} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONObject} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import {assignProperty} from '../../../shared/src/objects.ts';
import {aggregateValue} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import {
  type DeleteOp,
  type InsertOp,
  type UpdateOp,
  type UpsertOp,
} from '../../../zero-protocol/src/mutation.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {
  AGGREGATE_COUNT_COLUMN,
  AGGREGATE_SUM_COLUMN,
  AGGREGATE_VALUE_COLUMN,
} from '../../../zql/src/ivm/aggregate.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../../../zql/src/ivm/source.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';
import type {IVMSourceBranch, OptimisticAggregate} from './ivm-branch.ts';
import {aggregateRowKey, toPrimaryKeyString} from './keys.ts';
import type {WriteTransaction} from './replicache-types.ts';
export type {TableMutator} from '../../../zql/src/mutate/crud.ts';

function numberOr0(v: unknown): number {
  return typeof v === 'number' ? v : 0;
}

/**
 * The value a child row contributes to a sum/avg: its field, or the JSON leaf
 * at the aggregate's path (see `aggregateValue`).
 */
function contribution(agg: OptimisticAggregate, row: Row | undefined): unknown {
  return row === undefined ? undefined : aggregateValue(agg, row);
}

/** A child row's contribution as a number (null/absent ⇒ 0, SQL-style). */
function fieldNumber(agg: OptimisticAggregate, row: Row | undefined): number {
  return numberOr0(contribution(agg, row));
}

/** Whether a child row contributes a non-null number to a sum/avg. */
function contributes(agg: OptimisticAggregate, row: Row | undefined): boolean {
  return typeof contribution(agg, row) === 'number';
}

/**
 * The new aggregate row after a child change (`oldRow`→`newRow`: insert has only
 * `newRow`, delete only `oldRow`, update both), or `undefined` if unchanged.
 */
function nextAggregateRow(
  agg: OptimisticAggregate,
  curRow: Row,
  oldRowRaw: Row | undefined,
  newRowRaw: Row | undefined,
): Row | undefined {
  // A child row contributes only if it matches the aggregate's `where`
  // predicate (if any). Treating a non-matching row as absent lets all the
  // delta math below — count ±1, sum/avg ±field — naturally handle rows
  // entering or leaving the filtered set on an update (old matched / new
  // doesn't ⇒ −delta, and vice versa).
  const {predicate} = agg;
  const oldRow =
    predicate && oldRowRaw && !predicate(oldRowRaw) ? undefined : oldRowRaw;
  const newRow =
    predicate && newRowRaw && !predicate(newRowRaw) ? undefined : newRowRaw;
  const value = curRow[AGGREGATE_VALUE_COLUMN];
  switch (agg.fn) {
    case 'count': {
      const delta = (newRow ? 1 : 0) - (oldRow ? 1 : 0);
      return delta === 0
        ? undefined
        : {...curRow, [AGGREGATE_VALUE_COLUMN]: numberOr0(value) + delta};
    }
    case 'sum':
    case 'avg': {
      if (agg.field === undefined) {
        return undefined;
      }
      // The synced row carries the sum of non-null values and their count, so
      // adjust both and derive the value: SQL's sum/avg ignore nulls and are
      // null over no contributors.
      const sumDelta = fieldNumber(agg, newRow) - fieldNumber(agg, oldRow);
      const countDelta =
        (contributes(agg, newRow) ? 1 : 0) - (contributes(agg, oldRow) ? 1 : 0);
      if (sumDelta === 0 && countDelta === 0) {
        return undefined;
      }
      const sum = numberOr0(curRow[AGGREGATE_SUM_COLUMN]) + sumDelta;
      const count = numberOr0(curRow[AGGREGATE_COUNT_COLUMN]) + countDelta;
      const result = agg.fn === 'sum' ? sum : sum / count;
      return {
        ...curRow,
        [AGGREGATE_VALUE_COLUMN]: count > 0 ? result : null,
        [AGGREGATE_SUM_COLUMN]: sum,
        [AGGREGATE_COUNT_COLUMN]: count,
      };
    }
    // `min`/`max` are non-invertible; they update on the server poke.
    case 'min':
    case 'max':
      return undefined;
    default:
      unreachable(agg.fn);
  }
}

/**
 * The Replicache key of the aggregate row a child row contributes to, or
 * `undefined` when a correlation value is null: such a child has no parent, so
 * it is in no aggregate.
 */
function aggregateKeyFor(
  agg: OptimisticAggregate,
  child: Row,
): string | undefined {
  const keyRow: Record<string, Value> = {};
  for (let i = 0; i < agg.childField.length; i++) {
    const value = child[agg.childField[i]];
    if (value === null || value === undefined) {
      return undefined;
    }
    assignProperty(keyRow, agg.keyField[i], value);
  }
  return aggregateRowKey(agg.aggTableName, keyRow);
}

/**
 * Writes the aggregate row at `aggKey` as moved by one child change, if that
 * row is on the client (its parent is in some query's result). The row is
 * also pushed to the transaction's branch, so a query the mutator runs after
 * the change reads the new value.
 */
async function applyAggregateDelta(
  tx: WriteTransaction,
  ivmBranch: IVMSourceBranch,
  agg: OptimisticAggregate,
  aggKey: string,
  oldRow: Row | undefined,
  newRow: Row | undefined,
): Promise<void> {
  const cur = await tx.get(aggKey);
  if (cur === undefined) {
    return;
  }
  const updated = nextAggregateRow(agg, cur as Row, oldRow, newRow);
  if (updated !== undefined) {
    await tx.set(aggKey, updated);
    // The branch holds the row when a view of its query reads it.
    const source = ivmBranch.getSource(agg.aggTableName);
    if (source?.data.has(cur as Row)) {
      consume(source.push(makeSourceChangeEdit(updated, cur as Row)));
    }
  }
}

/**
 * Optimistically adjust any synced aggregates whose child rows live in `table`
 * when a child row is locally inserted/deleted/updated. The adjusted value is
 * written to Replicache (the aggregate's synthetic row, keyed by the child's
 * correlation field) inside the mutator's transaction — so the view updates
 * immediately, and Replicache's rebase reconciles it against the server's
 * authoritative value when it pokes back (the pending mutation's delta is
 * replayed while pending and absorbed into the base once confirmed — no
 * double-count). An aggregate row that is not on the client is left alone.
 */
async function applyOptimisticAggregateDeltas(
  tx: WriteTransaction,
  ivmBranch: IVMSourceBranch | undefined,
  table: string,
  oldRow: Row | undefined,
  newRow: Row | undefined,
): Promise<void> {
  if (!ivmBranch) {
    return;
  }
  for (const agg of ivmBranch.getOptimisticAggregates(table)) {
    const oldKey = oldRow && aggregateKeyFor(agg, oldRow);
    const newKey = newRow && aggregateKeyFor(agg, newRow);
    if (oldKey === newKey) {
      if (oldKey !== undefined) {
        await applyAggregateDelta(tx, ivmBranch, agg, oldKey, oldRow, newRow);
      }
    } else {
      // The child leaves one parent's aggregate and joins another's (either
      // side may be none: an insert, a delete, or a correlation value that
      // was or becomes null).
      if (oldKey !== undefined) {
        await applyAggregateDelta(
          tx,
          ivmBranch,
          agg,
          oldKey,
          oldRow,
          undefined,
        );
      }
      if (newKey !== undefined) {
        await applyAggregateDelta(
          tx,
          ivmBranch,
          agg,
          newKey,
          undefined,
          newRow,
        );
      }
    }
  }
}

function defaultOptionalFieldsToNull(
  schema: TableSchema,
  value: ReadonlyJSONObject,
): ReadonlyJSONObject {
  let rv = value;
  for (const name in schema.columns) {
    if (rv[name] === undefined) {
      rv = {...rv, [name]: null};
    }
  }
  return rv;
}

export async function insert(
  tx: WriteTransaction,
  arg: InsertOp,
  schema: Schema,
  ivmBranch: IVMSourceBranch | undefined,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.tableName,
    schema.tables[arg.tableName].primaryKey,
    arg.value,
  );
  if (!(await tx.has(key))) {
    const val = defaultOptionalFieldsToNull(
      schema.tables[arg.tableName],
      arg.value,
    );
    await tx.set(key, val);
    if (ivmBranch) {
      consume(
        must(ivmBranch.getSource(arg.tableName)).push(makeSourceChangeAdd(val)),
      );
    }
    await applyOptimisticAggregateDeltas(
      tx,
      ivmBranch,
      arg.tableName,
      undefined,
      val,
    );
  }
}

export async function upsert(
  tx: WriteTransaction,
  arg: InsertOp | UpsertOp,
  schema: Schema,
  ivmBranch: IVMSourceBranch | undefined,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.tableName,
    schema.tables[arg.tableName].primaryKey,
    arg.value,
  );
  if (await tx.has(key)) {
    await update(tx, {...arg, op: 'update'}, schema, ivmBranch);
  } else {
    await insert(tx, {...arg, op: 'insert'}, schema, ivmBranch);
  }
}

export async function update(
  tx: WriteTransaction,
  arg: UpdateOp,
  schema: Schema,
  ivmBranch: IVMSourceBranch | undefined,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.tableName,
    schema.tables[arg.tableName].primaryKey,
    arg.value,
  );
  const prev = await tx.get(key);
  if (prev === undefined) {
    return;
  }
  const update = arg.value;
  const next = {...(prev as ReadonlyJSONObject)};
  for (const k in update) {
    if (update[k] !== undefined) {
      next[k] = update[k];
    }
  }
  await tx.set(key, next);
  if (ivmBranch) {
    consume(
      must(ivmBranch.getSource(arg.tableName)).push(
        makeSourceChangeEdit(next, prev as Row),
      ),
    );
  }
  await applyOptimisticAggregateDeltas(
    tx,
    ivmBranch,
    arg.tableName,
    prev as Row,
    next as Row,
  );
}

async function deleteImpl(
  tx: WriteTransaction,
  arg: DeleteOp,
  schema: Schema,
  ivmBranch: IVMSourceBranch | undefined,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.tableName,
    schema.tables[arg.tableName].primaryKey,
    arg.value,
  );
  const prev = await tx.get(key);
  if (prev === undefined) {
    return;
  }
  await tx.del(key);
  if (ivmBranch) {
    consume(
      must(ivmBranch.getSource(arg.tableName)).push(
        makeSourceChangeRemove(prev as Row),
      ),
    );
  }
  await applyOptimisticAggregateDeltas(
    tx,
    ivmBranch,
    arg.tableName,
    prev as Row,
    undefined,
  );
}

export {deleteImpl as delete};
