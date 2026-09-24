import {assignProperty, mapValues} from '../../../shared/src/objects.ts';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import type {MaybePromise} from '../../../shared/src/types.ts';
import {
  CRUD_MUTATION_NAME,
  type CRUDMutationArg,
  type CRUDOp,
  type DeleteOp,
  type InsertOp,
  type UpdateOp,
  type UpsertOp,
} from '../../../zero-protocol/src/mutation.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {encodeRow} from '../../../zql/src/ivm/codec.ts';
import type {
  CRUDExecutor,
  DeleteID,
  InsertValue,
  TableMutator,
  UpdateValue,
  UpsertValue,
} from '../../../zql/src/mutate/crud.ts';
import * as crudImpl from './crud-impl.ts';
import type {IVMSourceBranch} from './ivm-branch.ts';
import type {MutatorDefs, WriteTransaction} from './replicache-types.ts';

export type DBMutator<S extends Schema> = S['enableLegacyMutators'] extends true
  ? {
      [K in keyof S['tables']]: TableMutator<S['tables'][K]>;
    }
  : {}; // {} is needed here for intersection type identity

export type BatchMutator<S extends Schema> =
  S['enableLegacyMutators'] extends true
    ? <R>(body: (m: DBMutator<S>) => MaybePromise<R>) => Promise<R>
    : undefined;

type ZeroCRUDMutate = {
  [CRUD_MUTATION_NAME]: CRUDMutate;
};

/**
 * This is the zero.mutateBatch function part representing the CRUD operations. If the
 * tables are `issue` and `label`, then this object will have `issue` and
 * `label` properties.
 *
 * @param schema - The schema defining the tables
 * @param repMutate - The replicache mutate object with the CRUD mutation
 * @param mutate - The object to use as the mutate object. Properties for each
 *                 table will be assigned to this object.
 */
export function makeCRUDMutateBatch<const S extends Schema>(
  schema: S,
  repMutate: ZeroCRUDMutate,
): BatchMutator<S> {
  if (schema.enableLegacyMutators !== true) {
    return undefined as BatchMutator<S>;
  }

  const {[CRUD_MUTATION_NAME]: zeroCRUD} = repMutate;

  const mutateBatch = async <R>(body: (m: DBMutator<S>) => R): Promise<R> => {
    const ops: CRUDOp[] = [];
    const m = mapValues(schema.tables, (_, name) =>
      makeBatchCRUDMutate(name, schema, ops),
    );
    const rv = await body(m as DBMutator<S>);
    await zeroCRUD({ops});
    return rv;
  };

  return mutateBatch as BatchMutator<S>;
}

export function addTableCRUDProperties<TSchema extends Schema>(
  schema: TSchema,
  mutate: object,
  repMutate: ZeroCRUDMutate,
): void {
  const {[CRUD_MUTATION_NAME]: zeroCRUD} = repMutate;
  for (const [name, tableSchema] of Object.entries(schema.tables)) {
    assignProperty(
      mutate as Record<string, unknown>,
      name,
      makeEntityCRUDMutate(name, tableSchema, zeroCRUD),
    );
  }
}

/**
 * Creates the `{insert, upsert, update, delete}` object for use outside a
 * batch.
 */
function makeEntityCRUDMutate<S extends TableSchema>(
  tableName: string,
  tableSchema: TableSchema,
  zeroCRUD: CRUDMutate,
): TableMutator<S> {
  return makeTableMutator(tableName, tableSchema, op => zeroCRUD({ops: [op]}));
}

/**
 * Creates the `{insert, upsert, update, delete}` object for use inside a
 * batch.
 */
export function makeBatchCRUDMutate<S extends TableSchema>(
  tableName: string,
  schema: Schema,
  ops: CRUDOp[],
): TableMutator<S> {
  return makeTableMutator(tableName, schema.tables[tableName], op => {
    ops.push(op);
    return promiseVoid;
  });
}

/**
 * Builds the CRUD op for each kind and hands it to `emit`. Values are encoded
 * through the table's column codecs first (a no-op for codec-free tables):
 * the value types accept the decoded app type (e.g. `Date`), but the op is
 * persisted, replayed into the IVM sources, and pushed to the server, all of
 * which expect the stored JSON form.
 */
function makeTableMutator<S extends TableSchema>(
  tableName: string,
  {primaryKey, columns}: TableSchema,
  emit: (op: CRUDOp) => Promise<void>,
): TableMutator<S> {
  return {
    insert: (value: InsertValue<S>) => {
      const op: InsertOp = {
        op: 'insert',
        tableName,
        primaryKey,
        value: encodeRow(value, columns),
      };
      return emit(op);
    },
    upsert: (value: UpsertValue<S>) => {
      const op: UpsertOp = {
        op: 'upsert',
        tableName,
        primaryKey,
        value: encodeRow(value, columns),
      };
      return emit(op);
    },
    update: (value: UpdateValue<S>) => {
      const op: UpdateOp = {
        op: 'update',
        tableName,
        primaryKey,
        value: encodeRow(value, columns),
      };
      return emit(op);
    },
    delete: (id: DeleteID<S>) => {
      const op: DeleteOp = {
        op: 'delete',
        tableName,
        primaryKey,
        value: encodeRow(id, columns),
      };
      return emit(op);
    },
  };
}

export type WithCRUD<MD extends MutatorDefs> = MD & {
  [CRUD_MUTATION_NAME]: CRUDMutator;
};

export type CRUDMutate = (crudArg: CRUDMutationArg) => Promise<void>;

export type CRUDMutator = (
  tx: WriteTransaction,
  crudArg: CRUDMutationArg,
) => Promise<void>;

export function makeCRUDExecutor(
  tx: WriteTransaction,
  schema: Schema,
  ivmBranch: IVMSourceBranch | undefined,
): CRUDExecutor {
  return (tableName, kind, value) => {
    const {primaryKey} = schema.tables[tableName];
    return crudImpl[kind](
      tx,
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      {op: kind, tableName, primaryKey, value} as any,
      schema,
      ivmBranch,
    );
  };
}

// Zero crud mutators cannot function at the same
// time as custom mutators as the rebase of crud mutators will not
// update the IVM branch. That's ok, we're removing crud mutators
// in favor of custom mutators.
export function makeCRUDMutator(schema: Schema): CRUDMutator {
  return async (
    tx: WriteTransaction,
    crudArg: CRUDMutationArg,
  ): Promise<void> => {
    const executor = makeCRUDExecutor(tx, schema, undefined);
    for (const op of crudArg.ops) {
      await executor(op.tableName, op.op, op.value);
    }
  };
}
