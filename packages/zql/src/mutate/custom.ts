import {assert} from '../../../shared/src/asserts.ts';
import type {Expand} from '../../../shared/src/expand.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {
  SchemaValueToTSType,
  TableSchema,
} from '../../../zero-schema/src/table-schema.ts';
import type {Query} from '../query/query.ts';

type ClientID = string;

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
}

export type Transaction<S extends Schema, TWrappedTransaction = unknown> =
  | ServerTransaction<S, TWrappedTransaction>
  | ClientTransaction<S>;

export interface ServerTransaction<S extends Schema, TWrappedTransaction>
  extends TransactionBase<S> {
  readonly location: 'server';
  readonly reason: 'authoritative';
  readonly dbTransaction: DBTransaction<TWrappedTransaction>;
}

/**
 * An instance of this is passed to custom mutator implementations and
 * allows reading and writing to the database and IVM at the head
 * at which the mutator is being applied.
 */
export interface ClientTransaction<S extends Schema>
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

export type SchemaQuery<S extends Schema> = {
  readonly [K in keyof S['tables'] & string]: Query<S, K>;
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

export function customMutatorKey(namespace: string, name: string) {
  assert(!namespace.includes('|'), 'mutator namespaces must not include a |');
  assert(!name.includes('|'), 'mutator names must not include a |');
  return `${namespace}|${name}`;
}

export function splitMutatorKey(key: string) {
  return key.split('|') as [string, string];
}
