import fc from 'fast-check';
import type {
  SimColumn,
  SimColumnType,
  SimRow,
  SimTransaction,
  SimValue,
} from './sim-pg.ts';

/**
 * One upstream statement. Its table, row, and column are indexes resolved
 * against the transaction's state when it runs, so any generated statement can
 * run in any state: one that Postgres would reject is skipped.
 */
export type WorkloadOp =
  | {readonly op: 'createTable'; readonly types: readonly SimColumnType[]}
  | {
      readonly op: 'insert';
      readonly table: number;
      readonly id: number;
      readonly value: number;
    }
  | {
      readonly op: 'update';
      readonly table: number;
      readonly id: number;
      readonly value: number;
    }
  | {
      readonly op: 'keyChange';
      readonly table: number;
      readonly id: number;
      readonly to: number;
      readonly value: number;
      /**
       * Leaves the row's text columns out of the update, as Postgres leaves
       * out an unchanged TOASTed value, so the moved row keeps its old ones.
       */
      readonly toasted?: boolean | undefined;
    }
  | {readonly op: 'delete'; readonly table: number; readonly id: number}
  | {
      readonly op: 'addColumn';
      readonly table: number;
      readonly type: SimColumnType;
      readonly value: number | null;
      /** A volatile default, which needs a backfill. */
      readonly backfilling?: boolean | undefined;
    }
  | {readonly op: 'dropColumn'; readonly table: number; readonly column: number}
  | {readonly op: 'renameTable'; readonly table: number};

/** Tables stay small: DST finds bugs through many short runs. */
export const MAX_ROW_ID = 6;

const typeArb = fc.constantFrom<SimColumnType>('int8', 'text', 'bool');
const tableArb = fc.nat({max: 7});
const idArb = fc.integer({min: 1, max: MAX_ROW_ID});
const valueArb = fc.nat({max: 50});

export const workloadOpArb: fc.Arbitrary<WorkloadOp> = fc.oneof(
  {
    weight: 1,
    arbitrary: fc.record({
      op: fc.constant('createTable' as const),
      types: fc.array(typeArb, {minLength: 1, maxLength: 2}),
    }),
  },
  {
    weight: 6,
    arbitrary: fc.record({
      op: fc.constant('insert' as const),
      table: tableArb,
      id: idArb,
      value: valueArb,
    }),
  },
  {
    weight: 4,
    arbitrary: fc.record({
      op: fc.constant('update' as const),
      table: tableArb,
      id: idArb,
      value: valueArb,
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      op: fc.constant('keyChange' as const),
      table: tableArb,
      id: idArb,
      to: idArb,
      value: valueArb,
    }),
  },
  {
    weight: 2,
    arbitrary: fc.record({
      op: fc.constant('delete' as const),
      table: tableArb,
      id: idArb,
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      op: fc.constant('addColumn' as const),
      table: tableArb,
      type: typeArb,
      value: fc.option(valueArb, {nil: null}),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      op: fc.constant('dropColumn' as const),
      table: tableArb,
      column: fc.nat({max: 5}),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      op: fc.constant('renameTable' as const),
      table: tableArb,
    }),
  },
);

/** A transaction's worth of statements. */
export const workloadTxArb = fc.array(workloadOpArb, {
  minLength: 1,
  maxLength: 4,
});

/**
 * {@link workloadOpArb} with the statements that start and complicate
 * backfills: a column added with a volatile default, and a row key change that
 * leaves out unchanged TOASTed values. Only a source with a backfill manager
 * can replicate them.
 */
export const backfillWorkloadOpArb: fc.Arbitrary<WorkloadOp> = fc.oneof(
  {weight: 6, arbitrary: workloadOpArb},
  {
    weight: 2,
    arbitrary: fc.record({
      op: fc.constant('addColumn' as const),
      table: tableArb,
      type: typeArb,
      value: fc.constant(null),
      backfilling: fc.constant(true),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      op: fc.constant('keyChange' as const),
      table: tableArb,
      id: idArb,
      to: idArb,
      value: valueArb,
      toasted: fc.constant(true),
    }),
  },
);

export const backfillWorkloadTxArb = fc.array(backfillWorkloadOpArb, {
  minLength: 1,
  maxLength: 4,
});

/** Runs `op` in `tx`, returning whether it changed anything. */
export function applyWorkloadOp(tx: SimTransaction, op: WorkloadOp): boolean {
  if (op.op === 'createTable') {
    tx.createTable(op.types);
    return true;
  }
  const table = tx.tableAt(op.table);
  if (!table) {
    return false;
  }
  switch (op.op) {
    case 'insert':
      return tx.insert(table.name, rowFor(table.columns, op.id, op.value));
    case 'update':
      return tx.update(
        table.name,
        {id: op.id},
        rowFor(table.columns, op.id, op.value),
      );
    case 'keyChange':
      return (
        op.id !== op.to &&
        tx.update(
          table.name,
          {id: op.id},
          rowFor(table.columns, op.to, op.value),
          op.toasted
            ? table.columns.filter(c => c.type === 'text').map(c => c.name)
            : [],
        )
      );
    case 'delete':
      return tx.delete(table.name, {id: op.id});
    case 'addColumn':
      return (
        tx.addColumn(
          table.name,
          op.type,
          op.value === null ? null : valueFor(op.type, op.value),
          op.backfilling,
        ) !== undefined
      );
    case 'dropColumn': {
      const droppable = table.columns.filter(
        c => !table.rowKey.includes(c.name),
      );
      return (
        droppable.length > 0 &&
        tx.dropColumn(table.name, droppable[op.column % droppable.length].name)
      );
    }
    case 'renameTable':
      return tx.renameTable(table.name) !== undefined;
  }
}

/** A row whose non-key values all derive from `value`, with some nulls. */
export function rowFor(
  columns: readonly SimColumn[],
  id: number,
  value: number,
): SimRow {
  return Object.fromEntries(
    columns.map(({name, type, attnum}) => [
      name,
      name === 'id'
        ? id
        : (value + attnum) % 7 === 0
          ? null
          : valueFor(type, value + attnum),
    ]),
  );
}

export function valueFor(type: SimColumnType, value: number): SimValue {
  switch (type) {
    case 'int8':
      return value;
    case 'text':
      return `v${value}`;
    case 'bool':
      return value % 2 === 0;
  }
}
