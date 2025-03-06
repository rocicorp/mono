import {must} from '../../../shared/src/must.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {ClientID} from '../types/client-state.ts';
import {deleteImpl, insertImpl, updateImpl, upsertImpl} from './crud.ts';
import type {WriteTransaction} from './replicache-types.ts';
import type {IVMSourceBranch} from './ivm-branch.ts';
import type {
  DeleteID,
  InsertValue,
  SchemaCRUD,
  SchemaQuery,
  TableCRUD,
  TransactionBase,
  UpdateValue,
  UpsertValue,
} from '../../../zql/src/mutate/custom.ts';
import {
  WriteTransactionImpl,
  zeroData,
} from '../../../replicache/src/transactions.ts';
import {newQuery} from '../../../zql/src/query/query-impl.ts';
import type {Query} from '../../../zql/src/query/query.ts';
import {ZeroContext} from './context.ts';
import type {LogContext} from '@rocicorp/logger';

/**
 * An instance of this is passed to custom mutator implementations and
 * allows reading and writing to the database and IVM at the head
 * at which the mutator is being applied.
 */
export interface Transaction<S extends Schema> extends TransactionBase<S> {
  readonly location: 'client';
  readonly reason: 'optimistic' | 'rebase';
}

/**
 * The shape which a user's custom mutator definitions must conform to.
 */
export type CustomMutatorDefs<S extends Schema> = {
  readonly [Table in keyof S['tables']]?: {
    readonly [key: string]: CustomMutatorImpl<S>;
  };
} & {
  // The user is not required to associate mutators with tables.
  // Maybe that have some other arbitrary way to namespace.
  [namespace: string]: {
    [key: string]: CustomMutatorImpl<S>;
  };
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type CustomMutatorImpl<S extends Schema, TArgs = any> = (
  tx: Transaction<S>,
  // TODO: many args. See commit: 52657c2f934b4a458d628ea77e56ce92b61eb3c6 which did have many args.
  // The issue being that it will be a protocol change to support varargs.
  args: TArgs,
) => Promise<void>;

/**
 * The shape exposed on the `Zero.mutate` instance.
 * The signature of a custom mutator takes a `transaction` as its first arg
 * but the user does not provide this arg when calling the mutator.
 *
 * This utility strips the `tx` arg from the user's custom mutator signatures.
 */
export type MakeCustomMutatorInterfaces<
  S extends Schema,
  MD extends CustomMutatorDefs<S>,
> = {
  readonly [Table in keyof MD]: {
    readonly [P in keyof MD[Table]]: MakeCustomMutatorInterface<
      S,
      MD[Table][P]
    >;
  };
};

export type MakeCustomMutatorInterface<
  S extends Schema,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  F,
> = F extends (tx: Transaction<S>, ...args: infer Args) => Promise<void>
  ? (...args: Args) => Promise<void>
  : never;

export class TransactionImpl implements Transaction<Schema> {
  constructor(
    lc: LogContext,
    repTx: WriteTransaction,
    schema: Schema,
    slowMaterializeThreshold: number,
  ) {
    const castedRepTx = repTx as WriteTransactionImpl;
    must(repTx.reason === 'initial' || repTx.reason === 'rebase');
    this.clientID = repTx.clientID;
    this.mutationID = repTx.mutationID;
    this.reason = repTx.reason === 'initial' ? 'optimistic' : 'rebase';
    // ~ Note: we will likely need to leverage proxies one day to create
    // ~ crud mutators and queries on demand for users with very large schemas.
    this.mutate = makeSchemaCRUD(
      schema,
      repTx,
      // CRUD operators should not mutate the IVM store directly
      // for `initial`. The IVM store will be updated via calls to `advance`
      // after the transaction has been committed to the Replicache b-tree.
      // Mutating the IVM store in the mutator would cause us to synchronously
      // notify listeners of IVM while we're inside of the Replicache DB transaction.
      repTx.reason === 'initial'
        ? undefined
        : (must(
            castedRepTx[zeroData],
            'zero was not set on replicache internal options!',
          ) as IVMSourceBranch),
    );
    this.query = makeSchemaQuery(
      lc,
      schema,
      must(
        castedRepTx[zeroData],
        'zero was not set on replicache internal options!',
      ) as IVMSourceBranch,
      slowMaterializeThreshold,
    );
  }

  readonly clientID: ClientID;
  readonly mutationID: number;
  readonly reason: 'optimistic' | 'rebase';
  readonly location = 'client';
  readonly mutate: SchemaCRUD<Schema>;
  readonly query: SchemaQuery<Schema>;
}

export function makeReplicacheMutator(
  lc: LogContext,
  mutator: CustomMutatorImpl<Schema>,
  schema: Schema,
  slowMaterializeThreshold: number,
) {
  return (repTx: WriteTransaction, args: ReadonlyJSONValue): Promise<void> => {
    const tx = new TransactionImpl(lc, repTx, schema, slowMaterializeThreshold);
    return mutator(tx, args);
  };
}

function makeSchemaCRUD(
  schema: Schema,
  tx: WriteTransaction,
  ivmBranch: IVMSourceBranch | undefined,
) {
  const mutate: Record<string, TableCRUD<TableSchema>> = {};
  for (const [name] of Object.entries(schema.tables)) {
    mutate[name] = makeTableCRUD(schema, name, tx, ivmBranch);
  }
  return mutate;
}

function makeSchemaQuery(
  lc: LogContext,
  schema: Schema,
  ivmBranch: IVMSourceBranch,
  slowMaterializeThreshold: number,
) {
  const rv = {} as Record<string, Query<Schema, string>>;
  const context = new ZeroContext(
    lc,
    ivmBranch,
    () => () => {},
    applyViewUpdates => applyViewUpdates(),
    slowMaterializeThreshold,
  );

  for (const name of Object.keys(schema.tables)) {
    rv[name] = newQuery(context, schema, name);
  }

  return rv as SchemaQuery<Schema>;
}

function makeTableCRUD(
  schema: Schema,
  tableName: string,
  tx: WriteTransaction,
  ivmBranch: IVMSourceBranch | undefined,
) {
  const table = must(schema.tables[tableName]);
  const {primaryKey} = table;
  return {
    insert: (value: InsertValue<TableSchema>) =>
      insertImpl(
        tx,
        {op: 'insert', tableName, primaryKey, value},
        schema,
        ivmBranch,
      ),
    upsert: (value: UpsertValue<TableSchema>) =>
      upsertImpl(
        tx,
        {op: 'upsert', tableName, primaryKey, value},
        schema,
        ivmBranch,
      ),
    update: (value: UpdateValue<TableSchema>) =>
      updateImpl(
        tx,
        {op: 'update', tableName, primaryKey, value},
        schema,
        ivmBranch,
      ),
    delete: (id: DeleteID<TableSchema>) =>
      deleteImpl(
        tx,
        {op: 'delete', tableName, primaryKey, value: id},
        schema,
        ivmBranch,
      ),
  };
}
