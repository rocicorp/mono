import type {Expand} from '../../../shared/src/expand.ts';
import {recordProxy} from '../../../shared/src/record-proxy.ts';
import type {SchemaValueToTSType} from '../../../zero-types/src/schema-value.ts';
import type {Schema, TableSchema} from '../../../zero-types/src/schema.ts';
import type {MutateCRUD} from './custom.ts';

export type SchemaCRUD<S extends Schema> = {
  [Table in keyof S['tables']]: TableCRUD<S['tables'][Table]>;
};

export type TransactionMutate<S extends Schema> = SchemaCRUD<S>;

/**
 * Options for upsert operations.
 */
export type UpsertOptions<S extends TableSchema> = {
  /**
   * Columns to use for conflict detection instead of the primary key.
   * Use this when upserting based on a unique constraint other than the primary key.
   */
  onConflict?: (keyof S['columns'] & string)[];
};

export type TableCRUD<S extends TableSchema> = {
  /**
   * Writes a row if a row with the same primary key doesn't already exist.
   * Non-primary-key fields that are 'optional' can be omitted or set to
   * `undefined`. Such fields will be assigned the value `null` optimistically
   * and then the default value as defined by the server.
   */
  insert: (value: InsertValue<S>) => Promise<void>;

  /**
   * Writes a row unconditionally, overwriting any existing row with the same
   * primary key (or custom conflict columns if specified). Non-primary-key
   * fields that are 'optional' can be omitted or set to `undefined`. Such
   * fields will be assigned the value `null` optimistically and then the
   * default value as defined by the server.
   *
   * @param value - The row data to upsert
   * @param options - Optional settings including custom conflict columns
   */
  upsert: (value: UpsertValue<S>, options?: UpsertOptions<S>) => Promise<void>;

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

export type CRUDKind = keyof TableCRUD<TableSchema>;

export const CRUD_KINDS = ['insert', 'upsert', 'update', 'delete'] as const;

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

/**
 * This is the type of the generated mutate.<name>.<verb> function.
 */
export type TableMutator<TS extends TableSchema> = {
  /**
   * Writes a row if a row with the same primary key doesn't already exist.
   * Non-primary-key fields that are 'optional' can be omitted or set to
   * `undefined`. Such fields will be assigned the value `null` optimistically
   * and then the default value as defined by the server.
   */
  insert: (value: InsertValue<TS>) => Promise<void>;

  /**
   * Writes a row unconditionally, overwriting any existing row with the same
   * primary key (or custom conflict columns if specified). Non-primary-key
   * fields that are 'optional' can be omitted or set to `undefined`. Such
   * fields will be assigned the value `null` optimistically and then the
   * default value as defined by the server.
   *
   * @param value - The row data to upsert
   * @param options - Optional settings including custom conflict columns
   */
  upsert: (value: UpsertValue<TS>, options?: UpsertOptions<TS>) => Promise<void>;

  /**
   * Updates a row with the same primary key. If no such row exists, this
   * function does nothing. All non-primary-key fields can be omitted or set to
   * `undefined`. Such fields will be left unchanged from previous value.
   */
  update: (value: UpdateValue<TS>) => Promise<void>;

  /**
   * Deletes the row with the specified primary key. If no such row exists, this
   * function does nothing.
   */
  delete: (id: DeleteID<TS>) => Promise<void>;
};

/**
 * Options passed to the executor for CRUD operations.
 */
export type CRUDExecutorOptions = {
  /** For upsert: columns to use for ON CONFLICT instead of primary key */
  onConflict?: string[];
};

/**
 * A function that executes a CRUD operation.
 * Client and server provide different implementations.
 */
export type CRUDExecutor = (
  table: string,
  kind: CRUDKind,
  args: unknown,
  options?: CRUDExecutorOptions,
) => Promise<void>;

/**
 * Creates a MutateCRUD function from a schema and executor.
 * This is the shared implementation used by both client and server.
 *
 * @param schema - The Zero schema
 * @param executor - A function that executes CRUD operations
 * @returns A MutateCRUD function that can be called with CRUDMutateRequest objects
 */
export function makeCRUDMutate<
  TSchema extends Schema,
  TAddSchemaCRUD extends boolean,
>(
  schema: TSchema,
  addSchemaCRUD: TAddSchemaCRUD,
  executor: CRUDExecutor,
): MutateCRUD<TSchema, TAddSchemaCRUD> {
  // Create a callable function that accepts CRUDMutateRequest
  const mutate = (request: AnyCRUDMutateRequest) => {
    const {table, kind, args} = request;
    return executor(table, kind, args);
  };

  // Only add table properties when enableLegacyMutators is true
  if (addSchemaCRUD) {
    // Add table names as keys so the proxy can discover them
    for (const tableName of Object.keys(schema.tables)) {
      (mutate as unknown as Record<string, undefined>)[tableName] = undefined;
    }

    // Wrap in proxy that lazily creates and caches table CRUD objects
    return recordProxy(
      mutate as unknown as Record<string, undefined>,
      (_value, tableName) => makeTableCRUD(tableName, executor),
    ) as unknown as MutateCRUD<TSchema, TAddSchemaCRUD>;
  }

  return mutate as MutateCRUD<TSchema, TAddSchemaCRUD>;
}

export function makeTransactionMutate<TSchema extends Schema>(
  schema: TSchema,
  executor: CRUDExecutor,
): TransactionMutate<TSchema> {
  const target: Record<string, undefined> = {};
  for (const tableName of Object.keys(schema.tables)) {
    target[tableName] = undefined;
  }

  return recordProxy(target, (_value, tableName) =>
    makeTableCRUD(tableName, executor),
  ) as SchemaCRUD<TSchema>;
}

/**
 * Creates a TableCRUD object that delegates to the executor.
 */
function makeTableCRUD(
  tableName: string,
  executor: CRUDExecutor,
): TableCRUD<TableSchema> {
  return {
    insert: (value: unknown) => executor(tableName, 'insert', value),
    upsert: (value: unknown, options?: UpsertOptions<TableSchema>) =>
      executor(tableName, 'upsert', value, options?.onConflict ? {onConflict: options.onConflict} : undefined),
    update: (value: unknown) => executor(tableName, 'update', value),
    delete: (value: unknown) => executor(tableName, 'delete', value),
  };
}

export type CRUDMutator<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TKind extends keyof TableMutator<TSchema['tables'][TTable]>,
  TArgs extends Parameters<TableMutator<TSchema['tables'][TTable]>[TKind]>[0],
> = {
  (args: TArgs): CRUDMutateRequest<TSchema, TTable, TKind, TArgs>;

  /**
   * Type-only phantom property to surface mutator types in a covariant position.
   */
  ['~']: Expand<CRUDMutatorTypes<TSchema, TTable, TKind, TArgs>>;
};

export type CRUDMutatorTypes<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TKind extends keyof TableMutator<TSchema['tables'][TTable]>,
  TArgs extends Parameters<TableMutator<TSchema['tables'][TTable]>[TKind]>[0],
> = 'CRUDMutator' & CRUDMutateRequest<TSchema, TTable, TKind, TArgs>;

export type CRUDMutateRequest<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'],
  TKind extends keyof TableMutator<TSchema['tables'][TTable]>,
  TArgs extends Parameters<TableMutator<TSchema['tables'][TTable]>[TKind]>[0],
> = {
  readonly schema: TSchema;
  readonly table: TTable;
  readonly kind: TKind;
  readonly args: TArgs;
};

// oxlint-disable-next-line no-explicit-any
export type AnyCRUDMutateRequest = CRUDMutateRequest<any, any, CRUDKind, any>;
