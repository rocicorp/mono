import type {Entity} from '@rocicorp/zql/src/entity.js';
import type {MutatorDefs, WriteTransaction} from 'reflect-shared/src/types.js';
import type {ReadonlyJSONObject} from 'shared/src/json.js';
import type {EntityID} from 'zero-protocol/src/entity.js';
import type {
  CRUDMutationArgs,
  CreateOp,
  DeleteOp,
  SetOp,
  UpdateOp,
} from 'zero-protocol/src/push.js';
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
export type MakeCRUDMutate<QD extends QueryDefs> = {
  [K in keyof QD]: EntityCRUDMutate<QD[K]>;
};

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
  const {_zero_crud: zeroCRUD, ...mutate} = repMutate;
  for (const name of Object.keys(queries)) {
    (mutate as Record<string, EntityCRUDMutate<Entity>>)[name] =
      makeEntityCRUDMutate(name, zeroCRUD);
  }
  return mutate as MakeCRUDMutate<QD>;
}

function makeEntityCRUDMutate<E extends Entity>(
  entityType: string,
  zeroCRUD: CRUDMutate,
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
      return zeroCRUD([{ops: [op]}]);
    },
    set: (value: E) => {
      const {id} = value;
      const op: SetOp = {op: 'set', entityType, id: {id}, value};
      return zeroCRUD([{ops: [op]}]);
    },
    update: (value: Update<E>) => {
      const {id} = value;
      const op: UpdateOp = {
        op: 'update',
        entityType,
        id: {id},
        partialValue: value,
      };
      return zeroCRUD([{ops: [op]}]);
    },
    delete: (id: EntityID) => {
      const op: DeleteOp = {op: 'delete', entityType, id};
      return zeroCRUD([{ops: [op]}]);
    },
  };
}

export type WithCRUD<MD extends MutatorDefs> = MD & {
  ['_zero_crud']: CRUDMutator;
};

export type CRUDMutate = (crudArgs: CRUDMutationArgs) => Promise<void>;

export type CRUDMutator = (
  tx: WriteTransaction,
  crudArgs: CRUDMutationArgs,
) => Promise<void>;

export function makeCRUDMutator<QD extends QueryDefs>(
  queries: QueryParseDefs<QD>,
): CRUDMutator {
  return function zeroCRUDMutator(
    tx: WriteTransaction,
    crudArgs: CRUDMutationArgs,
  ): Promise<void> {
    for (const op of crudArgs[0].ops) {
      switch (op.op) {
        case 'create':
          return createImpl(tx, op, queries[op.entityType]);
        case 'set':
          return setImpl(tx, op, queries[op.entityType]);
        case 'update':
          return updateImpl(tx, op, queries[op.entityType]);
        case 'delete':
          return deleteImpl(tx, op);
      }
    }
    return Promise.resolve();
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
