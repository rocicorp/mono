import type {Expand} from '../../../shared/src/expand.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {
  SchemaValueToTSType,
  TableSchema,
} from '../../../zero-schema/src/table-schema.ts';
import type {Query} from '../query/query.ts';

type ClientID = string;

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

export type CustomMutatorImpl<S extends Schema> = (
  tx: Transaction<S>,
  // TODO: many args. See commit: 52657c2f934b4a458d628ea77e56ce92b61eb3c6 which did have many args.
  // The issue being that it will be a protocol change to support varargs.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  args: any,
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
> = F extends (tx: Transaction<S>, ...args: infer Args) => void
  ? (...args: Args) => void
  : never;

export type TransactionReason = 'optimistic' | 'rebase' | 'server';

/**
 * An instance of this is passed to custom mutator implementations and
 * allows reading and writing to the database and IVM at the head
 * at which the mutator is being applied.
 */
export interface Transaction<S extends Schema> {
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
  readonly [K in keyof S['tables'] & string]: Omit<
    Query<S, K>,
    'materialize' | 'preload'
  >;
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
