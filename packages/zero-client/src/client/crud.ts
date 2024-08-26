import type {ReadonlyJSONObject} from 'shared/src/json.js';
import {promiseVoid} from 'shared/src/resolved-promises.js';
import type {MaybePromise} from 'shared/src/types.js';
import type {EntityID} from 'zero-protocol/src/entity.js';
import {
  CRUDMutationArg,
  CRUDOp,
  CRUDOpKind,
  CRUD_MUTATION_NAME,
  CreateOp,
  DeleteOp,
  SetOp,
  UpdateOp,
} from 'zero-protocol/src/push.js';
import {toEntitiesKey} from './keys.js';
import type {MutatorDefs, WriteTransaction} from './replicache-types.js';
import type {QueryDefs} from './zero.js';
import {Row} from 'zql/src/zql/ivm2/data.js';
import {SchemaToRow} from 'zql/src/zql/query2/query.js';

export type Parse<E extends Row> = (v: ReadonlyJSONObject) => E;

export type Update<E extends Row> = Partial<E>;

/**
 * This is the type of the generated mutate.<name>.<verb> function.
 */
export type EntityCRUDMutate<E extends Row> = {
  create: (value: E) => Promise<void>;
  set: (value: E) => Promise<void>;
  update: (value: Update<E>) => Promise<void>;
  delete: (id: EntityID) => Promise<void>;
};

/**
 * This is the type of the generated mutate.<name> object.
 */
export type MakeCRUDMutate<QD extends QueryDefs> = BaseCRUDMutate<QD> &
  CRUDBatch<QD>;

export type BaseCRUDMutate<QD extends QueryDefs> = {
  [K in keyof QD]: EntityCRUDMutate<SchemaToRow<QD[K]>>;
};

export type CRUDBatch<QD extends QueryDefs> = <R>(
  body: (m: BaseCRUDMutate<QD>) => MaybePromise<R>,
) => Promise<R>;

type ZeroCRUDMutate = {
  [CRUD_MUTATION_NAME]: CRUDMutate;
};

/**
 * This is the zero.mutate object part representing the CRUD operations. If the
 * queries are `issue` and `label`, then this object will have `issue` and
 * `label` properties.
 */
export function makeCRUDMutate<QD extends QueryDefs>(
  schemas: QD,
  repMutate: ZeroCRUDMutate,
): MakeCRUDMutate<QD> {
  const {[CRUD_MUTATION_NAME]: zeroCRUD} = repMutate;
  let inBatch = false;

  const mutate = async <R>(body: (m: BaseCRUDMutate<QD>) => R): Promise<R> => {
    if (inBatch) {
      throw new Error('Cannot call mutate inside a batch');
    }
    inBatch = true;

    try {
      const ops: CRUDOp[] = [];
      const m = {} as Record<string, unknown>;
      for (const name of Object.keys(schemas)) {
        m[name] = makeBatchCRUDMutate(name, ops);
      }

      const rv = await body(m as BaseCRUDMutate<QD>);
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

  for (const name of Object.keys(schemas)) {
    (mutate as unknown as Record<string, EntityCRUDMutate<Row>>)[name] =
      makeEntityCRUDMutate(name, zeroCRUD, assertNotInBatch);
  }
  return mutate as MakeCRUDMutate<QD>;
}

/**
 * Creates the `{create, set, update, delete}` object for use outside a batch.
 */
function makeEntityCRUDMutate<E extends Row>(
  entityType: string,
  zeroCRUD: CRUDMutate,
  assertNotInBatch: (entityType: string, op: CRUDOpKind) => void,
): EntityCRUDMutate<E> {
  return {
    create: (value: E) => {
      assertNotInBatch(entityType, 'create');
      const {id} = value;
      const op: CreateOp = {
        op: 'create',
        entityType,
        // TODO: Current crud mutators expect id to always exist
        id: {id: id as string},
        value,
      };
      return zeroCRUD({ops: [op]});
    },
    set: (value: E) => {
      assertNotInBatch(entityType, 'set');
      const {id} = value;
      const op: SetOp = {op: 'set', entityType, id: {id: id as string}, value};
      return zeroCRUD({ops: [op]});
    },
    update: (value: Update<E>) => {
      assertNotInBatch(entityType, 'update');
      const {id} = value;
      const op: UpdateOp = {
        op: 'update',
        entityType,
        id: {id: id as string},
        partialValue: value,
      };
      return zeroCRUD({ops: [op]});
    },
    delete: (id: EntityID) => {
      assertNotInBatch(entityType, 'delete');
      const op: DeleteOp = {op: 'delete', entityType, id};
      return zeroCRUD({ops: [op]});
    },
  };
}

/**
 * Creates the `{create, set, update, delete}` object for use inside a batch.
 */
export function makeBatchCRUDMutate<E extends Row>(
  entityType: string,
  ops: CRUDOp[],
): EntityCRUDMutate<E> {
  return {
    create: (value: E) => {
      const {id} = value;
      const op: CreateOp = {
        op: 'create',
        entityType,
        id: {id: id as string},
        value,
      };
      ops.push(op);
      return promiseVoid;
    },
    set: (value: E) => {
      const {id} = value;
      const op: SetOp = {op: 'set', entityType, id: {id: id as string}, value};
      ops.push(op);
      return promiseVoid;
    },
    update: (value: Update<E>) => {
      const {id} = value;
      const op: UpdateOp = {
        op: 'update',
        entityType,
        id: {id: id as string},
        partialValue: value,
      };
      ops.push(op);
      return promiseVoid;
    },
    delete: (id: EntityID) => {
      const op: DeleteOp = {op: 'delete', entityType, id};
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

export function makeCRUDMutator<QD extends QueryDefs>(
  _schemas: QD,
): CRUDMutator {
  return async function zeroCRUDMutator(
    tx: WriteTransaction,
    crudArg: CRUDMutationArg,
  ): Promise<void> {
    for (const op of crudArg.ops) {
      switch (op.op) {
        case 'create':
          await createImpl(tx, op);
          break;
        case 'set':
          await setImpl(tx, op);
          break;
        case 'update':
          await updateImpl(tx, op);
          break;
        case 'delete':
          await deleteImpl(tx, op);
          break;
      }
    }
  };
}

export async function createImpl(
  tx: WriteTransaction,
  arg: CreateOp,
): Promise<void> {
  const key = toEntitiesKey(arg.entityType, arg.id);
  if (!(await tx.has(key))) {
    await tx.set(key, arg.value);
  }
}

export async function setImpl(tx: WriteTransaction, arg: SetOp): Promise<void> {
  const key = toEntitiesKey(arg.entityType, arg.id);
  await tx.set(key, arg.value);
}

export async function updateImpl(
  tx: WriteTransaction,
  arg: UpdateOp,
): Promise<void> {
  const key = toEntitiesKey(arg.entityType, arg.id);
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
): Promise<void> {
  const key = toEntitiesKey(arg.entityType, arg.id);
  await tx.del(key);
}
