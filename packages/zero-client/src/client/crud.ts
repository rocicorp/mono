import {promiseVoid} from '../../../shared/src/resolved-promises.js';
import type {MaybePromise} from '../../../shared/src/types.js';
import type {Expand} from '../../../shared/src/expand.js';
import {
  CRUD_MUTATION_NAME,
  type InsertOp,
  type CRUDMutationArg,
  type CRUDOp,
  type CRUDOpKind,
  type DeleteOp,
  type UpsertOp,
  type UpdateOp,
} from '../../../zero-protocol/src/push.js';
import type {
  SchemaValueToTSType,
  TableSchema,
} from '../../../zero-schema/src/table-schema.js';
import {toPrimaryKeyString} from './keys.js';
import type {MutatorDefs, WriteTransaction} from './replicache-types.js';
import type {Schema} from '../../../zero-schema/src/mod.js';
import type {ReadonlyJSONObject} from '../mod.js';
import type {NormalizedTableSchema} from '../../../zero-schema/src/normalize-table-schema.js';
import type {NormalizedSchema} from '../../../zero-schema/src/normalized-schema.js';

export type InsertValue<S extends TableSchema> = Expand<
  PrimaryKeyFields<S> & {
    [K in keyof S['columns'] as S['columns'][K] extends {optional: true}
      ? K
      : never]?: SchemaValueToTSType<S['columns'][K]> | undefined;
  } & {
    [K in keyof S['columns'] as S['columns'][K] extends {optional: true}
      ? never
      : K]: SchemaValueToTSType<S['columns'][K]>;
  }
>;

export type UpsertValue<S extends TableSchema> = InsertValue<S>;

export type UpdateValue<S extends TableSchema> = Expand<
  PrimaryKeyFields<S> & {
    [K in keyof S['columns']]?:
      | SchemaValueToTSType<S['columns'][K]>
      | undefined;
  }
>;

export type DeleteID<S extends TableSchema> = Expand<PrimaryKeyFields<S>>;

type PrimaryKeyFields<S extends TableSchema> = {
  [K in Extract<
    S['primaryKey'][number],
    keyof S['columns']
  >]: SchemaValueToTSType<S['columns'][K]>;
};

/**
 * This is the type of the generated mutate.<name>.<verb> function.
 */
export type TableMutator<S extends TableSchema> = {
  /**
   * Writes a row if a row with the same primary key doesn't already exists.
   * Non-primary-key fields that are 'optional' can be omitted or set to
   * `undefined`. Such fields will be assigned the value `null` optimistically
   * and then the default value as defined by the server.
   */
  insert: (value: InsertValue<S>) => Promise<void>;

  /**
   * Writes a row unconditionally, overwriting any existing row with the same
   * primary key. Non-primary-key fields that are 'optional' can be omitted or
   * set to `undefined`. Such fields will be assigned the value `null`
   * optimistically and then the default value as defined by the server.
   */
  upsert: (value: UpsertValue<S>) => Promise<void>;

  /**
   * Updates a row with the same primary key. If no such row exists, this
   * function does nothing. All non-primary-key fields can be omitted or set to
   * `undefined`. Such fields will be left unchanged from previous value.
   */
  update: (value: UpdateValue<S>) => Promise<void>;

  /**
   * Deletes the row with the specified primary key. If no such row exists, this
   * function does nothing.
   */
  delete: (id: DeleteID<S>) => Promise<void>;
};

export type DBMutator<S extends Schema> = {
  [K in keyof S['tables']]: TableMutator<S['tables'][K]>;
};

export type BatchMutator<S extends Schema> = <R>(
  body: (m: DBMutator<S>) => MaybePromise<R>,
) => Promise<R>;

type ZeroCRUDMutate = {
  [CRUD_MUTATION_NAME]: CRUDMutate;
};

/**
 * This is the zero.mutate object part representing the CRUD operations. If the
 * queries are `issue` and `label`, then this object will have `issue` and
 * `label` properties.
 */
export function makeCRUDMutate<const S extends Schema>(
  schema: NormalizedSchema,
  repMutate: ZeroCRUDMutate,
): {mutate: DBMutator<S>; mutateBatch: BatchMutator<S>} {
  const {[CRUD_MUTATION_NAME]: zeroCRUD} = repMutate;
  let inBatch = false;

  const mutateBatch = async <R>(body: (m: DBMutator<S>) => R): Promise<R> => {
    if (inBatch) {
      throw new Error('Cannot call mutate inside a batch');
    }
    inBatch = true;

    try {
      const ops: CRUDOp[] = [];
      const m = {} as Record<string, unknown>;
      for (const name of Object.keys(schema.tables)) {
        m[name] = makeBatchCRUDMutate(name, schema, ops);
      }

      const rv = await body(m as DBMutator<S>);
      await zeroCRUD({ops});
      return rv;
    } finally {
      inBatch = false;
    }
  };

  const assertNotInBatch = (tableName: string, op: CRUDOpKind) => {
    if (inBatch) {
      throw new Error(`Cannot call mutate.${tableName}.${op} inside a batch`);
    }
  };

  const mutate: Record<string, TableMutator<TableSchema>> = {};
  for (const [name, tableSchema] of Object.entries(schema.tables)) {
    mutate[name] = makeEntityCRUDMutate(
      name,
      tableSchema.primaryKey,
      zeroCRUD,
      assertNotInBatch,
    );
  }
  return {
    mutate: mutate as DBMutator<S>,
    mutateBatch: mutateBatch as BatchMutator<S>,
  };
}

/**
 * Creates the `{insert, upsert, update, delete}` object for use outside a
 * batch.
 */
function makeEntityCRUDMutate<S extends NormalizedTableSchema>(
  tableName: string,
  primaryKey: S['primaryKey'],
  zeroCRUD: CRUDMutate,
  assertNotInBatch: (tableName: string, op: CRUDOpKind) => void,
): TableMutator<S> {
  return {
    insert: (value: InsertValue<S>) => {
      assertNotInBatch(tableName, 'insert');
      const op: InsertOp = {
        op: 'insert',
        tableName,
        primaryKey,
        value,
      };
      return zeroCRUD({ops: [op]});
    },
    upsert: (value: UpsertValue<S>) => {
      assertNotInBatch(tableName, 'upsert');
      const op: UpsertOp = {
        op: 'upsert',
        tableName,
        primaryKey,
        value,
      };
      return zeroCRUD({ops: [op]});
    },
    update: (value: UpdateValue<S>) => {
      assertNotInBatch(tableName, 'update');
      const op: UpdateOp = {
        op: 'update',
        tableName,
        primaryKey,
        value,
      };
      return zeroCRUD({ops: [op]});
    },
    delete: (id: DeleteID<S>) => {
      assertNotInBatch(tableName, 'delete');
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
  schema: NormalizedSchema,
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

export function makeCRUDMutator(schema: NormalizedSchema): CRUDMutator {
  return async function zeroCRUDMutator(
    tx: WriteTransaction,
    crudArg: CRUDMutationArg,
  ): Promise<void> {
    for (const op of crudArg.ops) {
      switch (op.op) {
        case 'insert':
          await insertImpl(tx, op, schema);
          break;
        case 'upsert':
          await upsertImpl(tx, op, schema);
          break;
        case 'update':
          await updateImpl(tx, op, schema);
          break;
        case 'delete':
          await deleteImpl(tx, op, schema);
          break;
      }
    }
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

async function insertImpl(
  tx: WriteTransaction,
  arg: InsertOp,
  schema: NormalizedSchema,
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
  }
}

async function upsertImpl(
  tx: WriteTransaction,
  arg: InsertOp | UpsertOp,
  schema: NormalizedSchema,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.tableName,
    schema.tables[arg.tableName].primaryKey,
    arg.value,
  );
  const val = defaultOptionalFieldsToNull(
    schema.tables[arg.tableName],
    arg.value,
  );
  await tx.set(key, val);
}

async function updateImpl(
  tx: WriteTransaction,
  arg: UpdateOp,
  schema: NormalizedSchema,
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
}

async function deleteImpl(
  tx: WriteTransaction,
  arg: DeleteOp,
  schema: NormalizedSchema,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.tableName,
    schema.tables[arg.tableName].primaryKey,
    arg.value,
  );
  await tx.del(key);
}
