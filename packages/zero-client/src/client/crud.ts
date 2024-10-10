import type {ReadonlyJSONObject} from '../../../shared/src/json.js';
import {promiseVoid} from '../../../shared/src/resolved-promises.js';
import type {MaybePromise} from '../../../shared/src/types.js';
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
import type {Row} from '../../../zql/src/zql/ivm/data.js';
import type {TableSchemaBase} from '../../../zql/src/zql/ivm/schema.js';
import type {SchemaToRow} from '../../../zql/src/zql/query/query.js';
import {toPrimaryKeyString} from './keys.js';
import {
  makeIDFromPrimaryKey,
  type PrimaryKeyValueRecord,
} from './make-id-from-primary-key.js';
import type {MutatorDefs, WriteTransaction} from './replicache-types.js';
import type {Schema} from './zero.js';

export type Parse<E extends Row> = (v: ReadonlyJSONObject) => E;

export type Update<E extends Row> = Partial<E>;

// TODO: We could make the crud methods more type safe by looking at the primaryKey type.

/**
 * This is the type of the generated mutate.<name>.<verb> function.
 */
export type EntityCRUDMutate<E extends Row> = {
  create: (value: E) => Promise<void>;
  set: (value: E) => Promise<void>;
  update: (value: Update<E>) => Promise<void>;
  delete: (id: PrimaryKeyValueRecord) => Promise<void>;
};

/**
 * This is the type of the generated mutate.<name> object.
 */
export type MakeCRUDMutate<S extends Schema> = BaseCRUDMutate<S> & CRUDBatch<S>;

export type BaseCRUDMutate<S extends Schema> = {
  [K in keyof S['tables']]: EntityCRUDMutate<SchemaToRow<S['tables'][K]>>;
};

export type CRUDBatch<S extends Schema> = <R>(
  body: (m: BaseCRUDMutate<S>) => MaybePromise<R>,
) => Promise<R>;

type ZeroCRUDMutate = {
  [CRUD_MUTATION_NAME]: CRUDMutate;
};

/**
 * This is the zero.mutate object part representing the CRUD operations. If the
 * queries are `issue` and `label`, then this object will have `issue` and
 * `label` properties.
 */
export function makeCRUDMutate<S extends Schema>(
  schema: S,
  repMutate: ZeroCRUDMutate,
): MakeCRUDMutate<S> {
  const {[CRUD_MUTATION_NAME]: zeroCRUD} = repMutate;
  let inBatch = false;

  const mutate = async <R>(body: (m: BaseCRUDMutate<S>) => R): Promise<R> => {
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

      const rv = await body(m as BaseCRUDMutate<S>);
      await zeroCRUD({ops});
      return rv;
    } finally {
      inBatch = false;
    }
  };

  const assertNotInBatch = (entityType: string, op: CRUDOpKind) => {
    if (inBatch) {
      throw new Error(`Cannot call mutate.${entityType}.${op} inside a batch`);
    }
  };

  for (const [name, tableSchema] of Object.entries(schema.tables)) {
    (mutate as unknown as Record<string, EntityCRUDMutate<Row>>)[name] =
      makeEntityCRUDMutate(name, tableSchema, zeroCRUD, assertNotInBatch);
  }
  return mutate as MakeCRUDMutate<S>;
}

/**
 * Creates the `{create, set, update, delete}` object for use outside a batch.
 */
function makeEntityCRUDMutate<E extends Row>(
  entityType: string,
  tableSchema: TableSchemaBase,
  zeroCRUD: CRUDMutate,
  assertNotInBatch: (entityType: string, op: CRUDOpKind) => void,
): EntityCRUDMutate<E> {
  return {
    create: (value: E) => {
      assertNotInBatch(entityType, 'create');
      const op: CreateOp = {
        op: 'create',
        entityType,
        id: makeIDFromPrimaryKey(tableSchema.primaryKey, value),
        value,
      };
      return zeroCRUD({ops: [op]});
    },
    set: (value: E) => {
      assertNotInBatch(entityType, 'set');
      const op: SetOp = {
        op: 'set',
        entityType,
        id: makeIDFromPrimaryKey(tableSchema.primaryKey, value),
        value,
      };
      return zeroCRUD({ops: [op]});
    },
    update: (value: Update<E>) => {
      assertNotInBatch(entityType, 'update');
      const op: UpdateOp = {
        op: 'update',
        entityType,
        id: makeIDFromPrimaryKey(tableSchema.primaryKey, value),
        partialValue: value,
      };
      return zeroCRUD({ops: [op]});
    },
    delete: (id: PrimaryKeyValueRecord) => {
      assertNotInBatch(entityType, 'delete');
      const op: DeleteOp = {
        op: 'delete',
        entityType,
        id: makeIDFromPrimaryKey(tableSchema.primaryKey, id),
      };
      return zeroCRUD({ops: [op]});
    },
  };
}

/**
 * Creates the `{create, set, update, delete}` object for use inside a batch.
 */
export function makeBatchCRUDMutate<E extends Row>(
  entityType: string,
  schema: Schema,
  ops: CRUDOp[],
): EntityCRUDMutate<E> {
  return {
    create: (value: E) => {
      const op: CreateOp = {
        op: 'create',
        entityType,
        id: makeIDFromPrimaryKey(schema.tables[entityType].primaryKey, value),
        value,
      };
      ops.push(op);
      return promiseVoid;
    },
    set: (value: E) => {
      const op: SetOp = {
        op: 'set',
        entityType,
        id: makeIDFromPrimaryKey(schema.tables[entityType].primaryKey, value),
        value,
      };
      ops.push(op);
      return promiseVoid;
    },
    update: (value: Update<E>) => {
      const op: UpdateOp = {
        op: 'update',
        entityType,
        id: makeIDFromPrimaryKey(schema.tables[entityType].primaryKey, value),
        partialValue: value,
      };
      ops.push(op);
      return promiseVoid;
    },
    delete: (id: PrimaryKeyValueRecord) => {
      const op: DeleteOp = {
        op: 'delete',
        entityType,
        id: makeIDFromPrimaryKey(schema.tables[entityType].primaryKey, id),
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

export function makeCRUDMutator<S extends Schema>(schema: S): CRUDMutator {
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
  schema: Schema,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.entityType,
    schema.tables[arg.entityType].primaryKey,
    arg.id,
  );
  if (!(await tx.has(key))) {
    await tx.set(key, arg.value);
  }
}

export async function setImpl(
  tx: WriteTransaction,
  arg: CreateOp | SetOp,
  schema: Schema,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.entityType,
    schema.tables[arg.entityType].primaryKey,
    arg.id,
  );
  await tx.set(key, arg.value);
}

export async function updateImpl(
  tx: WriteTransaction,
  arg: UpdateOp,
  schema: Schema,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.entityType,
    schema.tables[arg.entityType].primaryKey,
    arg.id,
  );
  const prev = await tx.get(key);
  if (prev === undefined) {
    return;
  }
  const update = arg.partialValue;
  const next = {...(prev as object), ...(update as object)};
  await tx.set(key, next);
}

export async function deleteImpl(
  tx: WriteTransaction,
  arg: DeleteOp,
  schema: Schema,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.entityType,
    schema.tables[arg.entityType].primaryKey,
    arg.id,
  );
  await tx.del(key);
}
