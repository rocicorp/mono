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
import type {SchemaQuery} from '../query/schema-query.ts';
import type {CRUDMutateRequest, SchemaCRUD} from './crud.ts';
export type {
  DeleteID,
  InsertValue,
  SchemaCRUD,
  TableCRUD,
  UpdateValue,
  UpsertValue,
} from './crud.ts';

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

  readonly mutate: MutateCRUD<S>;
  readonly query: SchemaQuery<S>;

  run<TTable extends keyof S['tables'] & string, TReturn>(
    query: Query<TTable, S, TReturn>,
    options?: RunOptions,
  ): Promise<HumanReadable<TReturn>>;
}

export type Transaction<
  S extends Schema = DefaultSchema,
  TWrappedTransaction = DefaultWrappedTransaction,
> = ServerTransaction<S, TWrappedTransaction> | ClientTransaction<S>;

export interface ServerTransaction<
  S extends Schema = DefaultSchema,
  TWrappedTransaction = DefaultWrappedTransaction,
> extends TransactionBase<S> {
  readonly location: 'server';
  readonly reason: 'authoritative';
  readonly dbTransaction: DBTransaction<TWrappedTransaction>;
}

/**
 * An instance of this is passed to custom mutator implementations and
 * allows reading and writing to the database and IVM at the head at which the
 * mutator is being applied.
 */
export interface ClientTransaction<S extends Schema = DefaultSchema>
  extends TransactionBase<S> {
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

interface Queryable {
  query: (query: string, args: unknown[]) => Promise<Iterable<Row>>;
}

/**
 * The type of `tx.mutate` which is:
 * 1. A callable function that accepts a `CRUDMutateRequest`
 * 2. When `enableLegacyMutators` is true, also an object with CRUD methods per table
 */
export type MutateCRUD<S extends Schema> = {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  (request: CRUDMutateRequest<S, any, any, any>): Promise<void>;
} & (S['enableLegacyMutators'] extends true ? SchemaCRUD<S> : {});

export function customMutatorKey(sep: string, parts: string[]) {
  for (const part of parts) {
    assert(
      !part.includes(sep),
      `mutator names/namespaces must not include a ${sep}`,
    );
  }
  return parts.join(sep);
}
