import {promiseVoid} from '../../../shared/src/resolved-promises.js';
import type {MaybePromise} from '../../../shared/src/types.js';
import type {Expand} from '../../../shared/src/expand.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import {
  type PrimaryKey,
  type PrimaryKeyValueRecord,
} from '../../../zero-protocol/src/primary-key.js';
import {
  CRUD_MUTATION_NAME,
  type CreateOp,
  type CRUDMutationArg,
  type CRUDOp,
  type CRUDOpKind,
  type DeleteOp,
  type SetOp,
  type UpdateOp,
} from '../../../zero-protocol/src/push.js';
import type {NormalizedPrimaryKey} from '../../../zero-schema/src/normalize-table-schema.js';
import type {TableSchemaToRow} from '../../../zero-schema/src/table-schema.js';
import {toPrimaryKeyString} from './keys.js';
import type {NormalizedSchema} from './normalized-schema.js';
import type {MutatorDefs, WriteTransaction} from './replicache-types.js';
import type {Schema} from '../../../zero-schema/src/mod.js';

/**
 * If a field is |undefined, add the ? marker to also make the field optional.
 */
type NormalizeOptional<T> = {
  [K in keyof T as undefined extends T[K] ? K : never]?: T[K] | undefined;
} & {
  [K in keyof T as undefined extends T[K] ? never : K]: T[K];
};

export type SetValue<R extends Row, PK extends PrimaryKey> = Expand<
  AsPrimaryKeyValueRecord<Pick<R, PK[number]>> &
    NormalizeOptional<Omit<R, PK[number]>>
>;

export type CreateValue<R extends Row, PK extends PrimaryKey> = SetValue<R, PK>;

export type UpdateValue<R extends Row, PK extends PrimaryKey> = Expand<
  AsPrimaryKeyValueRecord<Pick<R, PK[number]>> &
    NormalizeOptional<Partial<Omit<R, PK[number]>>>
>;

export type DeleteID<R extends Row, PK extends PrimaryKey> = Expand<
  AsPrimaryKeyValueRecord<Pick<R, PK[number]>>
>;

type AsPrimaryKeyValueRecord<R extends Row> = R extends PrimaryKeyValueRecord
  ? R
  : never;

/**
 * This is the type of the generated mutate.<name>.<verb> function.
 */
export type TableMutator<R extends Row, PK extends PrimaryKey> = {
  create: (value: CreateValue<R, PK>) => Promise<void>;
  set: (value: SetValue<R, PK>) => Promise<void>;
  update: (value: UpdateValue<R, PK>) => Promise<void>;
  delete: (id: DeleteID<R, PK>) => Promise<void>;
};

export type DBMutator<S extends Schema> = {
  [K in keyof S['tables']]: TableMutator<
    TableSchemaToRow<S['tables'][K]>,
    S['tables'][K]['primaryKey']
  >;
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

  const mutate: Record<string, TableMutator<Row, PrimaryKey>> = {};
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
 * Creates the `{create, set, update, delete}` object for use outside a batch.
 */
function makeEntityCRUDMutate<R extends Row, PK extends NormalizedPrimaryKey>(
  tableName: string,
  primaryKey: PK,
  zeroCRUD: CRUDMutate,
  assertNotInBatch: (tableName: string, op: CRUDOpKind) => void,
): TableMutator<R, PK> {
  return {
    create: (value: CreateValue<R, PK>) => {
      assertNotInBatch(tableName, 'create');
      const op: CreateOp = {
        op: 'create',
        tableName,
        primaryKey,
        value,
      };
      return zeroCRUD({ops: [op]});
    },
    set: (value: SetValue<R, PK>) => {
      assertNotInBatch(tableName, 'set');
      const op: SetOp = {
        op: 'set',
        tableName,
        primaryKey,
        value,
      };
      return zeroCRUD({ops: [op]});
    },
    update: (value: UpdateValue<R, PK>) => {
      assertNotInBatch(tableName, 'update');
      const op: UpdateOp = {
        op: 'update',
        tableName,
        primaryKey,
        value,
      };
      return zeroCRUD({ops: [op]});
    },
    delete: (id: DeleteID<R, PK>) => {
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
 * Creates the `{create, set, update, delete}` object for use inside a batch.
 */
export function makeBatchCRUDMutate<
  R extends Row,
  PK extends NormalizedPrimaryKey,
>(
  tableName: string,
  schema: NormalizedSchema,
  ops: CRUDOp[],
): TableMutator<R, PK> {
  const {primaryKey} = schema.tables[tableName];
  return {
    create: (value: CreateValue<R, PK>) => {
      const op: CreateOp = {
        op: 'create',
        tableName,
        primaryKey,
        value,
      };
      ops.push(op);
      return promiseVoid;
    },
    set: (value: SetValue<R, PK>) => {
      const op: SetOp = {
        op: 'set',
        tableName,
        primaryKey,
        value,
      };
      ops.push(op);
      return promiseVoid;
    },
    update: (value: UpdateValue<R, PK>) => {
      const op: UpdateOp = {
        op: 'update',
        tableName,
        primaryKey,
        value,
      };
      ops.push(op);
      return promiseVoid;
    },
    delete: (id: DeleteID<R, PK>) => {
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
        case 'create':
          await createImpl(tx, op, schema);
          break;
        case 'set':
          await setImpl(tx, op, schema);
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

async function createImpl(
  tx: WriteTransaction,
  arg: CreateOp,
  schema: NormalizedSchema,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.tableName,
    schema.tables[arg.tableName].primaryKey,
    arg.value,
  );
  if (!(await tx.has(key))) {
    await tx.set(key, arg.value);
  }
}

async function setImpl(
  tx: WriteTransaction,
  arg: CreateOp | SetOp,
  schema: NormalizedSchema,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.tableName,
    schema.tables[arg.tableName].primaryKey,
    arg.value,
  );
  await tx.set(key, arg.value);
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
  const next = {...(prev as object), ...(update as object)};
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
