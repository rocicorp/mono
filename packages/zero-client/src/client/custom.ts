import type {LogContext} from '@rocicorp/logger';
import {
  WriteTransactionImpl,
  zeroData,
} from '../../../replicache/src/transactions.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {
  ClientTransaction,
  DeleteID,
  InsertValue,
  SchemaCRUD,
  SchemaQuery,
  TableCRUD,
  Transaction,
  UpdateValue,
  UpsertValue,
} from '../../../zql/src/mutate/custom.ts';
import {newQuery} from '../../../zql/src/query/query-impl.ts';
import type {Query} from '../../../zql/src/query/query.ts';
import type {ClientID} from '../types/client-state.ts';
import {ZeroContext} from './context.ts';
import {deleteImpl, insertImpl, updateImpl, upsertImpl} from './crud.ts';
import type {IVMSourceBranch} from './ivm-branch.ts';
import type {WriteTransaction} from './replicache-types.ts';
import type {MutationResult} from '../../../zero-protocol/src/push.ts';
import type {MutationTracker} from './mutation-tracker.ts';

/**
 * The shape which a user's custom mutator definitions must conform to.
 */
export type CustomMutatorDefs<S extends Schema> = {
  [namespaceOrKey: string]:
    | {
        [key: string]: CustomMutatorImpl<S>;
      }
    | CustomMutatorImpl<S>;
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
  readonly [NamespaceOrName in keyof MD]: MD[NamespaceOrName] extends (
    tx: Transaction<S>,
    ...args: infer Args
  ) => Promise<void>
    ? (...args: Args) => Promise<void>
    : {
        readonly [P in keyof MD[NamespaceOrName]]: MakeCustomMutatorInterface<
          S,
          MD[NamespaceOrName][P]
        >;
      };
};

export type MakeCustomMutatorInterface<
  S extends Schema,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  F,
> = F extends (tx: ClientTransaction<S>, ...args: infer Args) => Promise<void>
  ? (...args: Args) => Promise<void>
  : never;

export class TransactionImpl<S extends Schema> implements ClientTransaction<S> {
  constructor(
    lc: LogContext,
    repTx: WriteTransaction,
    schema: S,
    slowMaterializeThreshold: number,
  ) {
    const castedRepTx = repTx as WriteTransactionImpl;
    must(repTx.reason === 'initial' || repTx.reason === 'rebase');
    this.clientID = repTx.clientID;
    this.mutationID = repTx.mutationID;
    this.reason = repTx.reason === 'initial' ? 'optimistic' : 'rebase';
    const txData = must(
      castedRepTx[zeroData],
      'zero was not set on replicache internal options!',
    );
    this.mutate = makeSchemaCRUD(
      schema,
      repTx,
      txData.ivmSources as IVMSourceBranch,
    );
    this.query = makeSchemaQuery(
      lc,
      schema,
      txData.ivmSources as IVMSourceBranch,
      slowMaterializeThreshold,
    );
    this.token = txData.token;
  }

  readonly clientID: ClientID;
  readonly mutationID: number;
  readonly reason: 'optimistic' | 'rebase';
  readonly location = 'client';
  readonly mutate: SchemaCRUD<S>;
  readonly query: SchemaQuery<S>;
  readonly token: string | undefined;
}

type MaybeWithServerResult<T> = {
  server?: Promise<T>;
};

type PromiseWithServerResult<T, S> = Promise<T> & MaybeWithServerResult<S>;

export function makeReplicacheMutator<S extends Schema>(
  lc: LogContext,
  mutationTracker: MutationTracker,
  mutator: CustomMutatorImpl<S>,
  schema: S,
  slowMaterializeThreshold: number,
) {
  return (
    repTx: WriteTransaction,
    args: ReadonlyJSONValue,
  ): PromiseWithServerResult<void, MutationResult> => {
    const tx = new TransactionImpl(lc, repTx, schema, slowMaterializeThreshold);
    const clientPromise = mutator(tx, args);

    if (repTx.reason === 'initial') {
      const serverPromise = mutationTracker.trackMutation(repTx.mutationID);
      (clientPromise as PromiseWithServerResult<void, MutationResult>).server =
        serverPromise;
    }

    return clientPromise;
  };
}

function makeSchemaCRUD<S extends Schema>(
  schema: S,
  tx: WriteTransaction,
  ivmBranch: IVMSourceBranch,
) {
  // Only creates the CRUD mutators on demand
  // rather than creating them all up-front for each mutation.
  return new Proxy(
    {},
    {
      get(target: Record<string, TableCRUD<TableSchema>>, prop: string) {
        if (prop in target) {
          return target[prop];
        }

        target[prop] = makeTableCRUD(schema, prop, tx, ivmBranch);
        return target[prop];
      },
    },
  ) as SchemaCRUD<S>;
}

function makeSchemaQuery<S extends Schema>(
  lc: LogContext,
  schema: S,
  ivmBranch: IVMSourceBranch,
  slowMaterializeThreshold: number,
) {
  const context = new ZeroContext(
    lc,
    ivmBranch,
    () => () => {},
    () => {},
    applyViewUpdates => applyViewUpdates(),
    slowMaterializeThreshold,
  );

  return new Proxy(
    {},
    {
      get(target: Record<string, Query<S, string>>, prop: string) {
        if (prop in target) {
          return target[prop];
        }

        target[prop] = newQuery(context, schema, prop);
        return target[prop];
      },
    },
  ) as SchemaQuery<S>;
}

function makeTableCRUD(
  schema: Schema,
  tableName: string,
  tx: WriteTransaction,
  ivmBranch: IVMSourceBranch,
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
