import type {LogContext} from '@rocicorp/logger';
import type {ZeroTxData} from '../../../replicache/src/replicache-options.ts';
import type {WriteTransactionImpl} from '../../../replicache/src/transactions.ts';
import {zeroData} from '../../../replicache/src/transactions.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import {emptyFunction} from '../../../shared/src/sentinels.ts';
import type {DefaultSchema} from '../../../zero-types/src/default-types.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {makeMutateCRUDFunction} from '../../../zql/src/mutate/crud.ts';
import type {
  ClientTransaction,
  MutateCRUD,
  Transaction,
} from '../../../zql/src/mutate/custom.ts';
import {createRunnableBuilder} from '../../../zql/src/query/create-builder.ts';
import {
  type HumanReadable,
  type Query,
  type RunOptions,
} from '../../../zql/src/query/query.ts';
import type {SchemaQuery} from '../../../zql/src/query/schema-query.ts';
import type {ClientID} from '../types/client-state.ts';
import {ZeroContext} from './context.ts';
import {makeCRUDExecutor} from './crud.ts';
import type {IVMSourceBranch} from './ivm-branch.ts';
import type {WriteTransaction} from './replicache-types.ts';

/**
 * The shape which a user's custom mutator definitions must conform to.
 * Supports arbitrary depth nesting of namespaces.
 */
export type CustomMutatorDefs = {
  // oxlint-disable-next-line no-explicit-any
  [namespaceOrKey: string]: CustomMutatorImpl<any> | CustomMutatorDefs;
};

export type MutatorResultDetails =
  | {
      readonly type: 'success';
    }
  | {
      readonly type: 'error';
      readonly error:
        | {
            readonly type: 'app';
            readonly message: string;
            readonly details: ReadonlyJSONValue | undefined;
          }
        | {
            readonly type: 'zero';
            readonly message: string;
          };
    };

export type MutatorResultSuccessDetails = Extract<
  MutatorResultDetails,
  {type: 'success'}
>;
export type MutatorResultErrorDetails = Extract<
  MutatorResultDetails,
  {type: 'error'}
>;

export type MutatorResult = {
  client: Promise<MutatorResultDetails & {}>;
  server: Promise<MutatorResultDetails & {}>;
} & {};

export type CustomMutatorImpl<
  S extends Schema,
  TWrappedTransaction = unknown,
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  TArgs = any,
  Context = unknown,
> = (
  tx: Transaction<S, TWrappedTransaction>,
  // TODO: many args. See commit: 52657c2f934b4a458d628ea77e56ce92b61eb3c6 which did have many args.
  // The issue being that it will be a protocol change to support varargs.
  args: TArgs,
  ctx: Context,
) => Promise<void>;

/**
 * The shape exposed on the `Zero.mutate` instance.
 * The signature of a custom mutator takes a `transaction` as its first arg
 * but the user does not provide this arg when calling the mutator.
 *
 * This utility strips the `tx` arg from the user's custom mutator signatures.
 * Supports arbitrary depth nesting of namespaces.
 */
export type MakeCustomMutatorInterfaces<
  S extends Schema,
  MD extends CustomMutatorDefs,
  TContext,
> = {
  readonly [NamespaceOrName in keyof MD]: MD[NamespaceOrName] extends (
    tx: Transaction<S>,
    ...args: infer Args
  ) => Promise<void>
    ? (...args: Args) => MutatorResult
    : MD[NamespaceOrName] extends CustomMutatorDefs
      ? MakeCustomMutatorInterfaces<S, MD[NamespaceOrName], TContext>
      : never;
};

export type MakeCustomMutatorInterface<TSchema extends Schema, F> = F extends (
  tx: ClientTransaction<TSchema>,
  ...args: infer Args
) => Promise<void>
  ? (...args: Args) => MutatorResult
  : never;

export class TransactionImpl<TSchema extends Schema = DefaultSchema>
  implements ClientTransaction<TSchema>
{
  readonly location = 'client';
  readonly mutate: MutateCRUD<TSchema>;
  /**
   * @deprecated Use {@linkcode createBuilder} with `tx.run(zql.table.where(...))` instead.
   */
  readonly query: TSchema['enableLegacyQueries'] extends true
    ? SchemaQuery<TSchema>
    : {};
  readonly #repTx: WriteTransaction;
  readonly #zeroContext: ZeroContext;

  constructor(lc: LogContext, repTx: WriteTransaction, schema: TSchema) {
    must(repTx.reason === 'initial' || repTx.reason === 'rebase');
    const txData = getZeroTxData(repTx);
    const ivmBranch = txData.ivmSources as IVMSourceBranch;

    this.#repTx = repTx;

    const executor = makeCRUDExecutor(repTx, schema, ivmBranch);
    this.mutate = makeMutateCRUDFunction(schema, executor);

    const zeroContext = newZeroContext(
      lc,
      txData.ivmSources as IVMSourceBranch,
    );

    this.query = schema.enableLegacyQueries
      ? createRunnableBuilder(zeroContext, schema)
      : ({} as SchemaQuery<TSchema>);
    this.#zeroContext = zeroContext;
  }

  get clientID(): ClientID {
    return this.#repTx.clientID;
  }

  get mutationID(): number {
    return this.#repTx.mutationID;
  }

  get reason(): 'optimistic' | 'rebase' {
    return this.#repTx.reason === 'initial' ? 'optimistic' : 'rebase';
  }

  get token(): string | undefined {
    return (this.#repTx as WriteTransactionImpl)[zeroData]?.token;
  }

  run<TTable extends keyof TSchema['tables'] & string, TReturn>(
    query: Query<TTable, TSchema, TReturn>,
    options?: RunOptions,
  ): Promise<HumanReadable<TReturn>> {
    return this.#zeroContext.run(query, options);
  }
}

export function getZeroTxData(repTx: WriteTransaction): ZeroTxData {
  const txData = must(
    (repTx as WriteTransactionImpl)[zeroData],
    'zero was not set on replicache internal options!',
  );
  return txData as ZeroTxData;
}

export function makeReplicacheMutator<
  S extends Schema,
  TWrappedTransaction,
  Context,
>(
  lc: LogContext,
  mutator: CustomMutatorImpl<S, TWrappedTransaction>,
  schema: S,
  context: Context,
): (repTx: WriteTransaction, args: ReadonlyJSONValue) => Promise<void> {
  return async (
    repTx: WriteTransaction,
    args: ReadonlyJSONValue,
  ): Promise<void> => {
    const tx = new TransactionImpl(lc, repTx, schema);
    await mutator(tx, args, context);
  };
}

function assertValidRunOptions(options: RunOptions | undefined): void {
  // TODO(arv): We should enforce this with the type system too.
  assert(
    options?.type !== 'complete',
    'Cannot wait for complete results in custom mutations',
  );
}

function newZeroContext(lc: LogContext, ivmBranch: IVMSourceBranch) {
  return new ZeroContext(
    lc,
    ivmBranch,
    () => emptyFunction,
    () => emptyFunction,
    emptyFunction,
    emptyFunction,
    emptyFunction,
    applyViewUpdates => applyViewUpdates(),
    emptyFunction,
    assertValidRunOptions,
  );
}
