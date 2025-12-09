import type {Expand} from '../../../shared/src/expand.ts';
import {recordProxy} from '../../../shared/src/record-proxy.ts';
import type {SchemaValueToTSType} from '../../../zero-types/src/schema-value.ts';
import type {Schema, TableSchema} from '../../../zero-types/src/schema.ts';
import type {MutateCRUD} from './custom.ts';

export type SchemaCRUD<S extends Schema> = {
  [Table in keyof S['tables']]: TableCRUD<S['tables'][Table]>;
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
   * Writes a row if a row with the same primary key doesn't already exists.
   * Non-primary-key fields that are 'optional' can be omitted or set to
   * `undefined`. Such fields will be assigned the value `null` optimistically
   * and then the default value as defined by the server.
   */
  insert: (value: InsertValue<TS>) => Promise<void>;

  /**
   * Writes a row unconditionally, overwriting any existing row with the same
   * primary key. Non-primary-key fields that are 'optional' can be omitted or
   * set to `undefined`. Such fields will be assigned the value `null`
   * optimistically and then the default value as defined by the server.
   */
  upsert: (value: UpsertValue<TS>) => Promise<void>;

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
 * A function that executes a CRUD operation.
 * Client and server provide different implementations.
 */
export type CRUDExecutor = (
  table: string,
  kind: CRUDKind,
  args: unknown,
) => Promise<void>;

/**
 * Creates a MutateCRUD function from a schema and executor.
 * This is the shared implementation used by both client and server.
 *
 * @param schema - The Zero schema
 * @param executor - A function that executes CRUD operations
 * @returns A MutateCRUD function that can be called with CRUDMutateRequest objects
 */
export function makeMutateCRUDFunction<S extends Schema>(
  schema: S,
  executor: CRUDExecutor,
): MutateCRUD<S> {
  // Create a callable function that accepts CRUDMutateRequest
  const mutate = (request: AnyCRUDMutateRequest) => {
    const {table, kind, args} = request;
    return executor(table, kind, args);
  };

  // Only add table properties when enableLegacyMutators is true
  if (schema.enableLegacyMutators === true) {
    // Add table names as keys so the proxy can discover them
    for (const tableName of Object.keys(schema.tables)) {
      (mutate as unknown as Record<string, undefined>)[tableName] = undefined;
    }

    // Wrap in proxy that lazily creates and caches table CRUD objects
    return recordProxy(
      mutate as unknown as Record<string, undefined>,
      (_value, tableName) => makeTableCRUD(tableName, executor),
    ) as unknown as MutateCRUD<S>;
  }

  return mutate as MutateCRUD<S>;
}

/**
 * Creates a TableCRUD object that delegates to the executor.
 */
function makeTableCRUD(
  tableName: string,
  executor: CRUDExecutor,
): TableCRUD<TableSchema> {
  return Object.fromEntries(
    CRUD_KINDS.map(kind => [
      kind,
      (value: unknown) => executor(tableName, kind, value),
    ]),
  ) as TableCRUD<TableSchema>;
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

export type TableCRUDMutators<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
> = {
  [K in keyof TableMutator<TSchema['tables'][TTable]>]: CRUDMutator<
    TSchema,
    TTable,
    K,
    Parameters<TableMutator<TSchema['tables'][TTable]>[K]>[0]
  >;
};

/**
 * Creates a table CRUD builder that returns `CRUDMutateRequest` objects.
 * These request objects can be passed to `tx.mutate(request)`.
 */
export function makeTableCRUDRequestBuilder<
  S extends Schema,
  T extends keyof S['tables'] & string,
>(schema: S, table: T): TableCRUDMutators<S, T> {
  return Object.fromEntries(
    CRUD_KINDS.map(kind => [
      kind,
      (args: unknown) => ({schema, table, kind, args}),
    ]),
  ) as TableCRUDMutators<S, T>;
}

/**
 * Creates a schema CRUD builder where each table has methods that return
 * `CRUDMutateRequest` objects. These can be passed to `tx.mutate(request)`.
 *
 * @example
 *
 * ```ts
 * const crud = createCRUDBuilder(schema);
 *
 * // Inside a custom mutator:
 * await tx.mutate(crud.user.insert({name: 'Alice'}));
 * ```
 */
export function createCRUDBuilder<S extends Schema>(
  schema: S,
): SchemaCRUDMutators<S> {
  return recordProxy(
    schema.tables,
    (_tableSchema, tableName) =>
      makeTableCRUDRequestBuilder(
        schema,
        tableName as keyof S['tables'] & string,
      ),
    prop => {
      throw new Error(`Table ${prop} does not exist in schema`);
    },
  ) as unknown as SchemaCRUDMutators<S>;
}

export type SchemaCRUDMutators<S extends Schema> = {
  [T in keyof S['tables'] & string]: TableCRUDMutators<S, T>;
};
