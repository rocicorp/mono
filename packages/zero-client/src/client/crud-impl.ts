import type {ReadonlyJSONObject} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
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

/** A child row's aggregated field as a number (null/absent ⇒ 0, SQL-style). */
function fieldNumber(row: Row | undefined, field: string): number {
  return row ? numberOr0(row[field]) : 0;
}

/** Whether a child row contributes a non-null number to a sum/avg. */
function contributes(row: Row | undefined, field: string): boolean {
  return row !== undefined && typeof row[field] === 'number';
}

/**
 * The new aggregate row after a child change (`oldRow`→`newRow`: insert has only
 * `newRow`, delete only `oldRow`, update both), or `undefined` if unchanged.
 */
function nextAggregateRow(
  agg: OptimisticAggregate,
  curRow: Row,
  oldRow: Row | undefined,
  newRow: Row | undefined,
): Row | undefined {
  const value = curRow[AGGREGATE_VALUE_COLUMN];
  switch (agg.fn) {
    case 'count': {
      const delta = (newRow ? 1 : 0) - (oldRow ? 1 : 0);
      return delta === 0
        ? undefined
        : {...curRow, [AGGREGATE_VALUE_COLUMN]: numberOr0(value) + delta};
    }
    case 'sum': {
      if (agg.field === undefined) {
        return undefined;
      }
      // SQL sum ignores nulls; null/absent fields contribute 0.
      const delta =
        fieldNumber(newRow, agg.field) - fieldNumber(oldRow, agg.field);
      return delta === 0
        ? undefined
        : {...curRow, [AGGREGATE_VALUE_COLUMN]: numberOr0(value) + delta};
    }
    case 'avg': {
      if (agg.field === undefined) {
        return undefined;
      }
      // avg = sum / count(non-null). The synced row carries both components, so
      // adjust them and recompute the ratio.
      const sumDelta =
        fieldNumber(newRow, agg.field) - fieldNumber(oldRow, agg.field);
      const countDelta =
        (contributes(newRow, agg.field) ? 1 : 0) -
        (contributes(oldRow, agg.field) ? 1 : 0);
      if (sumDelta === 0 && countDelta === 0) {
        return undefined;
      }
      const sum = numberOr0(curRow[AGGREGATE_SUM_COLUMN]) + sumDelta;
      const count = numberOr0(curRow[AGGREGATE_COUNT_COLUMN]) + countDelta;
      return {
        ...curRow,
        [AGGREGATE_VALUE_COLUMN]: count > 0 ? sum / count : null,
        [AGGREGATE_SUM_COLUMN]: sum,
        [AGGREGATE_COUNT_COLUMN]: count,
      };
    }
    // `min`/`max` are non-invertible; they update on the server poke.
    default:
      return undefined;
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
 * double-count). Skipped if no aggregate row exists yet (parent not in result)
 * or if an update reparents the child (correlation field changed).
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
    const ref = newRow ?? oldRow;
    if (!ref) {
      continue;
    }
    // Reparenting (an update that moves the child to a different parent) spans
    // two aggregate rows; leave that to the server.
    if (oldRow && newRow && agg.childField.some(f => oldRow[f] !== newRow[f])) {
      continue;
    }
    const keyRow: Record<string, unknown> = {};
    for (const f of agg.childField) {
      keyRow[f] = ref[f];
    }
    const aggKey = aggregateRowKey(agg.aggTableName, keyRow as Row);
    const cur = await tx.get(aggKey);
    if (cur === undefined) {
      continue;
    }
    const updated = nextAggregateRow(agg, cur as Row, oldRow, newRow);
    if (updated !== undefined) {
      await tx.set(aggKey, updated);
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
        must(ivmBranch.getSource(arg.tableName)).push(
          makeSourceChangeAdd(arg.value),
        ),
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
