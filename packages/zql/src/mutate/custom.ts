import {assert} from '../../../shared/src/asserts.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {
  DefaultSchema,
  DefaultWrappedTransaction,
} from '../../../zero-types/src/default-types.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {ServerSchema} from '../../../zero-types/src/server-schema.ts';
import type {Format} from '../ivm/view.ts';
import type {HumanReadable, Query, RunOptions} from '../query/query.ts';
import type {ConditionalSchemaQuery} from '../query/schema-query.ts';
import type {CRUDMutateRequest, SchemaCRUD, TransactionMutate} from './crud.ts';

type ClientID = string;

/**
 * A base transaction interface that any Transaction<S, T> is assignable to.
 * Used in places where the schema type doesn't need to be preserved,
 * like the public signature of Mutator.fn.
 */
export interface AnyTransaction {
  readonly location: Location;
  readonly clientID: string;
  readonly mutationID: number;
  readonly reason: TransactionReason;
}

export type Location = 'client' | 'server';
export type TransactionReason = 'optimistic' | 'rebase' | 'authoritative';

export interface TransactionBase<S extends Schema> {
  readonly location: Location;
  readonly clientID: ClientID;
  /**
   * The ID of the mutation that is being applied.
   */
  readonly mutationID: number;

  /**
   * The reason for the transaction.
   */
  readonly reason: TransactionReason;

  readonly mutate: TransactionMutate<S>;
  /**
   * @deprecated Use {@linkcode createBuilder} with `tx.run(zql.table.where(...))` instead.
   */
  readonly query: ConditionalSchemaQuery<S>;

  run<TTable extends keyof S['tables'] & string, TReturn>(
    query: Query<TTable, S, TReturn>,
    options?: RunOptions,
  ): Promise<HumanReadable<TReturn>>;
}

export type Transaction<
  S extends Schema = DefaultSchema,
  TWrappedTransaction = DefaultWrappedTransaction,
> = ServerTransaction<S, TWrappedTransaction> | ClientTransaction<S>;

export type RetryOptions = {
  /**
   * How long to wait, once this run's transaction has rolled back, before
   * running the mutator again.
   */
  delayMs?: number | undefined;
};

export interface ServerTransaction<
  S extends Schema = DefaultSchema,
  TWrappedTransaction = DefaultWrappedTransaction,
> extends TransactionBase<S> {
  readonly location: 'server';
  readonly reason: 'authoritative';
  readonly dbTransaction: DBTransaction<TWrappedTransaction>;

  /**
   * Which run of the mutator this is within the current push: 1 for the first,
   * 2 for the first re-run after {@linkcode retry}, and so on. A push that is
   * sent again, by zero-cache or the client, starts over at 1.
   */
  readonly attempt: number;

  /**
   * Abandons this run and runs the mutator again in a fresh transaction, after
   * `delayMs` if given. Everything this run wrote is rolled back, and the
   * re-run reads a new snapshot.
   *
   * Use it for failures that may pass on their own, such as a serialization
   * failure or a rate-limited external API, and only in a mutator that is safe
   * to run again. Within one push the mutator runs at most 5 times; a retry
   * requested on the last run is recorded as the mutation's error. The wait
   * holds the push request open, so a delay that outlasts the API server's
   * request timeout gets the push sent again, starting over at attempt 1.
   */
  retry(options?: RetryOptions): never;
}

/**
 * An instance of this is passed to custom mutator implementations and
 * allows reading and writing to the database and IVM at the head at which the
 * mutator is being applied.
 */
export interface ClientTransaction<
  S extends Schema = DefaultSchema,
> extends TransactionBase<S> {
  readonly location: 'client';
  readonly reason: 'optimistic' | 'rebase';
}

export interface Row {
  [column: string]: unknown;
}

export interface DBConnection<TWrappedTransaction> {
  transaction: <T>(
    cb: (tx: DBTransaction<TWrappedTransaction>) => Promise<T>,
  ) => Promise<T>;

  /**
   * Executes a single SQL statement on the connection without opening,
   * committing, or rolling back a transaction of its own. With a pooled
   * handle this is a plain autocommit statement. With a single dedicated
   * connection, driver semantics apply: a `pg` `Client` queues it behind, and
   * inside, whatever transaction is open on that connection, while postgres.js
   * waits for the reserved connection to be released.
   *
   * Optional. When present, `ZQLDatabase.run` uses it for single-statement
   * reads instead of `transaction`, so no `BEGIN`/`COMMIT` round-trips are
   * needed and any setup done inside `transaction` does not apply to those
   * reads. Custom adapters must implement this to get that behavior; when
   * absent, `ZQLDatabase.run` falls back to `transaction`.
   */
  query?: Queryable['query'] | undefined;

  /**
   * Mirrors {@linkcode DBTransaction.runQuery} for reads issued through
   * {@linkcode query}. An adapter that customizes `DBTransaction.runQuery`
   * should implement this too so that `ZQLDatabase.run` and `tx.run` execute
   * a query the same way. Optional; when absent, the default Postgres
   * executor is used.
   */
  runQuery?: DBTransaction<TWrappedTransaction>['runQuery'] | undefined;
}

export interface DBTransaction<T> extends Queryable {
  readonly wrappedTransaction: T;
  runQuery<TReturn>(
    ast: AST,
    format: Format,
    schema: Schema,
    serverSchema: ServerSchema,
  ): Promise<HumanReadable<TReturn>>;
}

export interface Queryable {
  query: (query: string, args: unknown[]) => Promise<Iterable<Row>>;
}

/**
 * A callable mutate shape with optional table helpers used by helper factories
 * like `makeMutateCRUD`. Transactions expose `SchemaCRUD` instead of this type.
 */
export type MutateCRUD<S extends Schema, AddSchemaCRUD extends boolean> = {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  (request: CRUDMutateRequest<S, any, any, any>): Promise<void>;
} & (AddSchemaCRUD extends true ? SchemaCRUD<S> : {});

export function customMutatorKey(sep: string, parts: string[]) {
  for (const part of parts) {
    assert(
      !part.includes(sep),
      `mutator names/namespaces must not include a ${sep}`,
    );
  }
  return parts.join(sep);
}
