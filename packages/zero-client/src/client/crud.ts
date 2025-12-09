import type {ReadonlyJSONObject} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import type {MaybePromise} from '../../../shared/src/types.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {
  CRUD_MUTATION_NAME,
  type CRUDMutationArg,
  type CRUDOp,
  type DeleteOp,
  type InsertOp,
  type UpdateOp,
  type UpsertOp,
} from '../../../zero-protocol/src/push.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';
import type {TableMutator} from '../../../zql/src/mutate/crud.ts';
import type {
  DeleteID,
  InsertValue,
  UpdateValue,
  UpsertValue,
} from '../../../zql/src/mutate/custom.ts';
import type {IVMSourceBranch} from './ivm-branch.ts';
import {toPrimaryKeyString} from './keys.ts';
import type {MutatorDefs, WriteTransaction} from './replicache-types.ts';
export type {TableMutator} from '../../../zql/src/mutate/crud.ts';

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
 * This is the zero.mutate object part representing the CRUD operations. If the
 * tables are `issue` and `label`, then this object will have `issue` and
 * `label` properties.
 *
 * @param schema - The schema defining the tables
 * @param repMutate - The replicache mutate object with the CRUD mutation
 * @param mutate - The object to use as the mutate object. Properties for each
 *                 table will be assigned to this object.
 */
export function makeCRUDMutate<const S extends Schema>(
  schema: S,
  repMutate: ZeroCRUDMutate,
  mutate: object,
): BatchMutator<S> {
  if (schema.enableLegacyMutators !== true) {
    return undefined as BatchMutator<S>;
  }

  const {[CRUD_MUTATION_NAME]: zeroCRUD} = repMutate;

  const mutateBatch = async <R>(body: (m: DBMutator<S>) => R): Promise<R> => {
    const ops: CRUDOp[] = [];
    const m = {} as Record<string, unknown>;
    for (const name of Object.keys(schema.tables)) {
      m[name] = makeBatchCRUDMutate(name, schema, ops);
    }

    const rv = await body(m as DBMutator<S>);
    await zeroCRUD({ops});
    return rv;
  };

  for (const [name, tableSchema] of Object.entries(schema.tables)) {
    (mutate as Record<string, unknown>)[name] = makeEntityCRUDMutate(
      name,
      tableSchema.primaryKey,
      zeroCRUD,
    );
  }
  return mutateBatch as BatchMutator<S>;
}

/**
 * Creates the `{insert, upsert, update, delete}` object for use outside a
 * batch.
 */
function makeEntityCRUDMutate<S extends TableSchema>(
  tableName: string,
  primaryKey: S['primaryKey'],
  zeroCRUD: CRUDMutate,
): TableMutator<S> {
  return {
    insert: (value: InsertValue<S>) => {
      const op: InsertOp = {
        op: 'insert',
        tableName,
        primaryKey,
        value,
      };
      return zeroCRUD({ops: [op]});
    },
    upsert: (value: UpsertValue<S>) => {
      const op: UpsertOp = {
        op: 'upsert',
        tableName,
        primaryKey,
        value,
      };
      return zeroCRUD({ops: [op]});
    },
    update: (value: UpdateValue<S>) => {
      const op: UpdateOp = {
        op: 'update',
        tableName,
        primaryKey,
        value,
      };
      return zeroCRUD({ops: [op]});
    },
    delete: (id: DeleteID<S>) => {
      const op: DeleteOp = {
        op: 'delete',
        tableName,
        primaryKey,
        value: id,
      };
      return zeroCRUD({ops: [op]});
    },
  };
}

/**
 * Creates the `{inesrt, upsert, update, delete}` object for use inside a
 * batch.
 */
export function makeBatchCRUDMutate<S extends TableSchema>(
  tableName: string,
  schema: Schema,
  ops: CRUDOp[],
): TableMutator<S> {
  const {primaryKey} = schema.tables[tableName];
  return {
    insert: (value: InsertValue<S>) => {
      const op: InsertOp = {
        op: 'insert',
        tableName,
        primaryKey,
        value,
      };
      ops.push(op);
      return promiseVoid;
    },
    upsert: (value: UpsertValue<S>) => {
      const op: UpsertOp = {
        op: 'upsert',
        tableName,
        primaryKey,
        value,
      };
      ops.push(op);
      return promiseVoid;
    },
    update: (value: UpdateValue<S>) => {
      const op: UpdateOp = {
        op: 'update',
        tableName,
        primaryKey,
        value,
      };
      ops.push(op);
      return promiseVoid;
    },
    delete: (id: DeleteID<S>) => {
      const op: DeleteOp = {
        op: 'delete',
        tableName,
        primaryKey,
        value: id,
      };
      ops.push(op);
      return promiseVoid;
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

// Zero crud mutators cannot function at the same
// time as custom mutators as the rebase of crud mutators will not
// update the IVM branch. That's ok, we're removing crud mutators
// in favor of custom mutators.
export function makeCRUDMutator(schema: Schema): CRUDMutator {
  return async (
    tx: WriteTransaction,
    crudArg: CRUDMutationArg,
  ): Promise<void> => {
    for (const op of crudArg.ops) {
      switch (op.op) {
        case 'insert':
          await insertImpl(tx, op, schema, undefined);
          break;
        case 'upsert':
          await upsertImpl(tx, op, schema, undefined);
          break;
        case 'update':
          await updateImpl(tx, op, schema, undefined);
          break;
        case 'delete':
          await deleteImpl(tx, op, schema, undefined);
          break;
      }
    }
  };
}

export type CRUDExecutor = (
  tableName: string,
  kind: 'insert' | 'upsert' | 'update' | 'delete',
  value: unknown,
) => Promise<void>;

const crudImpl = {
  insert: insertImpl,
  upsert: upsertImpl,
  update: updateImpl,
  delete: deleteImpl,
};

export function makeCRUDExecutor(
  tx: WriteTransaction,
  schema: Schema,
  ivmBranch: IVMSourceBranch | undefined,
): CRUDExecutor {
  return (tableName, kind, value) => {
    const {primaryKey} = schema.tables[tableName];
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    return (crudImpl[kind] as any)(
      tx,
      {op: kind, tableName, primaryKey, value},
      schema,
      ivmBranch,
    );
  };
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

export async function insertImpl(
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
        must(ivmBranch.getSource(arg.tableName)).push({
          type: 'add',
          row: arg.value,
        }),
      );
    }
  }
}

export async function upsertImpl(
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
    await updateImpl(tx, {...arg, op: 'update'}, schema, ivmBranch);
  } else {
    await insertImpl(tx, {...arg, op: 'insert'}, schema, ivmBranch);
  }
}

export async function updateImpl(
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
      must(ivmBranch.getSource(arg.tableName)).push({
        type: 'edit',
        oldRow: prev as Row,
        row: next,
      }),
    );
  }
}

export async function deleteImpl(
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
      must(ivmBranch.getSource(arg.tableName)).push({
        type: 'remove',
        row: prev as Row,
      }),
    );
  }
}
