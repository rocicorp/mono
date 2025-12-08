import {assert} from '../../../shared/src/asserts.ts';
import type {Expand} from '../../../shared/src/expand.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {
  DefaultSchema,
  DefaultWrappedTransaction,
} from '../../../zero-types/src/default-types.ts';
import type {SchemaValueToTSType} from '../../../zero-types/src/schema-value.ts';
import type {Schema, TableSchema} from '../../../zero-types/src/schema.ts';
import type {ServerSchema} from '../../../zero-types/src/server-schema.ts';
import type {Format} from '../ivm/view.ts';
import type {
  HumanReadable,
  QueryBuilder,
  RunOptions,
} from '../query/query-builder.ts';
import type {SchemaQuery} from '../query/schema-query.ts';

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

  readonly mutate: SchemaCRUD<S>;
  readonly query: SchemaQuery<S>;

  run<TTable extends keyof S['tables'] & string, TReturn>(
    query: QueryBuilder<TTable, S, TReturn>,
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

export type SchemaCRUD<S extends Schema> = {
  [Table in keyof S['tables']]: TableCRUD<S['tables'][Table]>;
};

export type TableCRUD<S extends TableSchema> = {
  /**
   * Writes a row if a row with the same primary key doesn't already exists.
   * Non-primary-key fields that are 'optional' can be omitted or set to
   * `undefined`. Such fields will be assigned the value `null` optimistically
   * and then the default value as defined by the server.
   */
  insert: (value: InsertValue<S>) => Promise<void>;

  /**
   * Writes a row unconditionally, overwriting any existing row with the same
   * primary key. Non-primary-key fields that are 'optional' can be omitted or
   * set to `undefined`. Such fields will be assigned the value `null`
   * optimistically and then the default value as defined by the server.
   */
  upsert: (value: UpsertValue<S>) => Promise<void>;

  /**
   * Updates a row with the same primary key. If no such row exists, this
   * function does nothing. All non-primary-key fields can be omitted or set to
   * `undefined`. Such fields will be left unchanged from previous value.
   */
  update: (value: UpdateValue<S>) => Promise<void>;

  /**
   * Deletes the row with the specified primary key. If no such row exists, this
   * function does nothing.
   */
  delete: (id: DeleteID<S>) => Promise<void>;
};

export type DeleteID<S extends TableSchema> = Expand<PrimaryKeyFields<S>>;

type PrimaryKeyFields<S extends TableSchema> = {
  [K in Extract<
    S['primaryKey'][number],
    keyof S['columns']
  >]: SchemaValueToTSType<S['columns'][K]>;
};

export type InsertValue<S extends TableSchema> = Expand<
  PrimaryKeyFields<S> & {
    [K in keyof S['columns'] as S['columns'][K] extends {optional: true}
      ? K
      : never]?: SchemaValueToTSType<S['columns'][K]> | undefined;
  } & {
    [K in keyof S['columns'] as S['columns'][K] extends {optional: true}
      ? never
      : K]: SchemaValueToTSType<S['columns'][K]>;
  }
>;

export type UpsertValue<S extends TableSchema> = InsertValue<S>;

export type UpdateValue<S extends TableSchema> = Expand<
  PrimaryKeyFields<S> & {
    [K in keyof S['columns']]?:
      | SchemaValueToTSType<S['columns'][K]>
      | undefined;
  }
>;

export function customMutatorKey(sep: string, parts: string[]) {
  for (const part of parts) {
    assert(
      !part.includes(sep),
      `mutator names/namespaces must not include a ${sep}`,
    );
  }
  return parts.join(sep);
}

export function splitMutatorKey(key: string, sep: string | RegExp): string[] {
  return key.split(sep);
}
