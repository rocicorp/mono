import type {Entity} from '@rocicorp/zql/src/entity.js';
import type {MutatorDefs, WriteTransaction} from 'reflect-shared/src/types.js';
import type {ReadonlyJSONObject} from 'shared/src/json.js';
import {promiseVoid} from 'shared/src/resolved-promises.js';
import type {EntityID} from 'zero-protocol/src/entity.js';
import type {
  CRUDMutationArg,
  CRUDOp,
  CRUDOpKind,
  CreateOp,
  DeleteOp,
  SetOp,
  UpdateOp,
} from 'zero-protocol/src/push.js';
import type {MaybePromise} from '../mod.js';
import {toEntitiesKey} from './keys.js';
import type {QueryParseDefs} from './options.js';
import type {QueryDefs} from './zero.js';

export type Parse<E extends Entity> = (v: ReadonlyJSONObject) => E;

export type Update<E extends Entity> = Entity & Partial<E>;

/**
 * This is the type of the generated mutate.<name>.<verb> function.
 */
type EntityCRUDMutate<E extends Entity> = {
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
  [K in keyof QD]: EntityCRUDMutate<QD[K]>;
};

export type CRUDBatch<QD extends QueryDefs> = <R>(
  body: (m: BaseCRUDMutate<QD>) => MaybePromise<R>,
) => Promise<R>;

type ZeroCRUDMutate = {
  ['_zero_crud']: CRUDMutate;
};

/**
 * This is the zero.mutate object part representing the CRUD operations. If the
 * queries are `issue` and `label`, then this object will have `issue` and
 * `label` properties.
 */
export function makeCRUDMutate<QD extends QueryDefs>(
  queries: QueryParseDefs<QD>,
  repMutate: ZeroCRUDMutate,
): MakeCRUDMutate<QD> {
  const {_zero_crud: zeroCRUD} = repMutate;
  let inBatch = false;

  const mutate = async <R>(body: (m: BaseCRUDMutate<QD>) => R): Promise<R> => {
    if (inBatch) {
      throw new Error('Cannot call mutate inside a batch');
    }
    inBatch = true;

    try {
      const ops: CRUDOp[] = [];
      const m = {} as Record<string, unknown>;
      for (const name of Object.keys(queries)) {
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

  for (const name of Object.keys(queries)) {
    (mutate as unknown as Record<string, EntityCRUDMutate<Entity>>)[name] =
      makeEntityCRUDMutate(name, zeroCRUD, assertNotInBatch);
  }
  return mutate as MakeCRUDMutate<QD>;
}

/**
 * Creates the `{create, set, update, delete}` object for use outside a batch.
 */
function makeEntityCRUDMutate<E extends Entity>(
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
        id: {id},
        value,
      };
      return zeroCRUD({ops: [op]});
    },
    set: (value: E) => {
      assertNotInBatch(entityType, 'set');
      const {id} = value;
      const op: SetOp = {op: 'set', entityType, id: {id}, value};
      return zeroCRUD({ops: [op]});
    },
    update: (value: Update<E>) => {
      assertNotInBatch(entityType, 'update');
      const {id} = value;
      const op: UpdateOp = {
        op: 'update',
        entityType,
        id: {id},
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
function makeBatchCRUDMutate<E extends Entity>(
  entityType: string,
  ops: CRUDOp[],
): EntityCRUDMutate<E> {
  return {
    create: (value: E) => {
      const {id} = value;
      const op: CreateOp = {
        op: 'create',
        entityType,
        id: {id},
        value,
      };
      ops.push(op);
      return promiseVoid;
    },
    set: (value: E) => {
      const {id} = value;
      const op: SetOp = {op: 'set', entityType, id: {id}, value};
      ops.push(op);
      return promiseVoid;
    },
    update: (value: Update<E>) => {
      const {id} = value;
      const op: UpdateOp = {
        op: 'update',
        entityType,
        id: {id},
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
  ['_zero_crud']: CRUDMutator;
};

export type CRUDMutate = (crudArg: CRUDMutationArg) => Promise<void>;

export type CRUDMutator = (
  tx: WriteTransaction,
  crudArg: CRUDMutationArg,
) => Promise<void>;

export function makeCRUDMutator<QD extends QueryDefs>(
  queries: QueryParseDefs<QD>,
): CRUDMutator {
  return async function zeroCRUDMutator(
    tx: WriteTransaction,
    crudArg: CRUDMutationArg,
  ): Promise<void> {
    for (const op of crudArg.ops) {
      switch (op.op) {
        case 'create':
          await createImpl(tx, op, queries[op.entityType]);
          break;
        case 'set':
          await setImpl(tx, op, queries[op.entityType]);
          break;
        case 'update':
          await updateImpl(tx, op, queries[op.entityType]);
          break;
        case 'delete':
          await deleteImpl(tx, op);
          break;
      }
    }
  };
}

export async function createImpl<E extends Entity>(
  tx: WriteTransaction,
  arg: CreateOp,
  parse: Parse<E>,
): Promise<void> {
  const value = parse(arg.value);
  const key = toEntitiesKey(arg.entityType, arg.id);
  if (!(await tx.has(key))) {
    await tx.set(key, value);
  }
}

export async function setImpl<E extends Entity>(
  tx: WriteTransaction,
  arg: SetOp,
  parse: Parse<E>,
): Promise<void> {
  const value = parse(arg.value);
  const key = toEntitiesKey(arg.entityType, arg.id);
  await tx.set(key, value);
}

export async function updateImpl<E extends Entity>(
  tx: WriteTransaction,
  arg: UpdateOp,
  parse: Parse<E>,
): Promise<void> {
  const key = toEntitiesKey(arg.entityType, arg.id);
  const prev = await tx.get(key);
  if (prev === undefined) {
    return;
  }
  const update = arg.partialValue;
  const next = {...(prev as object), ...(update as object)};
  const parsed = parse(next);
  await tx.set(key, parsed);
}

export async function deleteImpl(
  tx: WriteTransaction,
  arg: DeleteOp,
): Promise<void> {
  const key = toEntitiesKey(arg.entityType, arg.id);
  await tx.del(key);
}
