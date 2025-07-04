import {assert} from '../../../shared/src/asserts.ts';
import type {Expand} from '../../../shared/src/expand.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {
  SchemaValue,
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
   * Writes a row if a row with the same primary key doesn't already exist.
   *
   * Non-primary-key fields that are 'nullable' can be omitted or set to
   * `undefined`. Such fields will be assigned the value `null` optimistically
   * and then the default value as defined by the server.
   *
   * If there is a `default` function defined for a field, and no value is
   * provided, it will be called to generate the value for that field.
   */
  insert: (value: InsertValue<S>) => Promise<void>;

  /**
   * Writes a row unconditionally, overwriting any existing row with the same
   * primary key.
   *
   * Non-primary-key fields that are 'nullable' can be omitted or
   * set to `undefined`. Such fields will be assigned the value `null`
   * optimistically and then the default value as defined by the server.
   *
   * If there is a `default` function defined for a field, and
   * no value is provided, then it will be called to generate the value for
   * the field, depending on if the primary key already exists.
   */
  upsert: (value: UpsertValue<S>) => Promise<void>;

  /**
   * Updates a row with the same primary key. If no such row exists, this
   * function does nothing. All non-primary-key fields can be omitted or set to
   * `undefined`. Such fields will be left unchanged from previous value.
   *
   * If there is a `default` function defined for the field, and no value is
   * provided, it will be called to generate the value for that field.
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

export type DeleteID<S extends TableSchema> = Expand<{
  [K in PrimaryKeys<S>]: SchemaValueToTSType<S['columns'][K]>;
}>;

type PrimaryKeys<S extends TableSchema> = Extract<
  S['primaryKey'][number],
  keyof S['columns']
>;

type PrimaryKeyFields<S extends TableSchema> = {
  [K in PrimaryKeys<S>]: SchemaValueToTSType<S['columns'][K]>;
};

type NonPrimaryKeyFields<S extends TableSchema> = Exclude<
  keyof S['columns'],
  PrimaryKeys<S>
>;

type HasInsertDefault<T extends SchemaValue> = T extends {
  defaultConfig: {
    insert: {
      client: () => unknown;
    };
  };
}
  ? true
  : false;

type HasUpdateDefault<T extends SchemaValue> = T extends {
  defaultConfig: {
    update: {
      client: () => unknown;
    };
  };
}
  ? true
  : false;

type IsNullable<T> = T extends {nullable: true} ? true : false;

// columns that are not nullable and have no insert default
type RequiredInsertFields<S extends TableSchema> = {
  [K in NonPrimaryKeyFields<S>]: HasInsertDefault<S['columns'][K]> extends true
    ? never
    : IsNullable<S['columns'][K]> extends true
      ? never
      : K;
}[NonPrimaryKeyFields<S>];

// optional non-PK columns for insert: nullable or has insert default
type OptionalInsertFields<S extends TableSchema> = {
  [K in NonPrimaryKeyFields<S>]: HasInsertDefault<S['columns'][K]> extends true
    ? K
    : IsNullable<S['columns'][K]> extends true
      ? K
      : never;
}[NonPrimaryKeyFields<S>];

// columns that are not nullable, have no insert default, and no update default
type RequiredUpsertFields<S extends TableSchema> = {
  [K in NonPrimaryKeyFields<S>]: HasInsertDefault<S['columns'][K]> extends true
    ? never
    : HasUpdateDefault<S['columns'][K]> extends true
      ? never
      : IsNullable<S['columns'][K]> extends true
        ? never
        : K;
}[NonPrimaryKeyFields<S>];

// columns that have an update default but no insert default
type UpdateOnlyFields<S extends TableSchema> = {
  [K in NonPrimaryKeyFields<S>]: HasInsertDefault<S['columns'][K]> extends true
    ? never
    : HasUpdateDefault<S['columns'][K]> extends true
      ? K
      : never;
}[NonPrimaryKeyFields<S>];

export type InsertValue<S extends TableSchema> = Expand<
  // primary key fields (always required)
  PrimaryKeyFields<S> & {
    // required non-primary-key fields
    [K in RequiredInsertFields<S>]: SchemaValueToTSType<S['columns'][K]>;
  } & {
    // optional non-primary-key fields
    [K in OptionalInsertFields<S>]?:
      | SchemaValueToTSType<S['columns'][K]>
      | undefined;
  }
>;

export type UpsertValue<S extends TableSchema> = Expand<
  // primary key fields (always required)
  PrimaryKeyFields<S> & {
    // required non-primary-key fields
    [K in RequiredUpsertFields<S>]: SchemaValueToTSType<S['columns'][K]>;
  } & {
    // optional non-primary-key fields (nullable or has insert default)
    [K in OptionalInsertFields<S>]?:
      | SchemaValueToTSType<S['columns'][K]>
      | undefined;
  } & {
    // update-only fields (key required, value can be undefined)
    [K in UpdateOnlyFields<S>]:
      | SchemaValueToTSType<S['columns'][K]>
      | undefined;
  }
>;

export type UpdateValue<S extends TableSchema> = Expand<
  // primary key fields (always required)
  PrimaryKeyFields<S> & {
    // optional non-primary-key fields
    [K in keyof S['columns'] as K extends PrimaryKeys<S> ? never : K]?:
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
