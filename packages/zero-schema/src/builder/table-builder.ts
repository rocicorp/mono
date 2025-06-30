import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {
  DefaultConfig,
  SchemaValue,
  SchemaValueWithCustomType,
  TableSchema,
} from '../table-schema.ts';

/* eslint-disable @typescript-eslint/no-explicit-any */
export function table<TName extends string>(name: TName) {
  return new TableBuilder({
    name,
    columns: {},
    primaryKey: [] as any as PrimaryKey,
  });
}

export function string<T extends string = string>() {
  return new ColumnBuilder({
    type: 'string',
    nullable: false,
    customType: null as unknown as T,
  });
}

export function number<T extends number = number>() {
  return new ColumnBuilder({
    type: 'number',
    nullable: false,
    customType: null as unknown as T,
  });
}

export function boolean<T extends boolean = boolean>() {
  return new ColumnBuilder({
    type: 'boolean',
    nullable: false,
    customType: null as unknown as T,
  });
}

export function json<T extends ReadonlyJSONValue = ReadonlyJSONValue>() {
  return new ColumnBuilder({
    type: 'json',
    nullable: false,
    customType: null as unknown as T,
  });
}

export function enumeration<T extends string>() {
  return new ColumnBuilder({
    type: 'string',
    nullable: false,
    customType: null as unknown as T,
  });
}

export const column = {
  string,
  number,
  boolean,
  json,
  enumeration,
};

export class TableBuilder<TShape extends TableSchema> {
  readonly #schema: TShape;
  constructor(schema: TShape) {
    this.#schema = schema;
  }

  /**
   * Allows the table to be named differently in the database.
   *
   * @param serverName - The name of the table in the database.
   */
  from<ServerName extends string>(serverName: ServerName) {
    return new TableBuilder<TShape>({
      ...this.#schema,
      // Strip the "public." schema if specified, as tables in the upstream
      // "public" schema are created without the schema prefix on the replica.
      // See liteTableName() in zero-cache/src/types/names.ts
      serverName: serverName.startsWith('public.')
        ? serverName.substring('public.'.length)
        : serverName,
    });
  }

  /**
   * Specifies the column definitions for the table.
   *
   * @param columns - The column definitions for the table.
   */
  columns<const TColumns extends Record<string, {schema: SchemaValue}>>(
    columns: TColumns,
  ): TableBuilderWithColumns<{
    name: TShape['name'];
    columns: {[K in keyof TColumns]: TColumns[K]['schema']};
    primaryKey: TShape['primaryKey'];
  }> {
    const columnSchemas = Object.fromEntries(
      Object.entries(columns).map(([k, v]) => [k, v.schema]),
    ) as {[K in keyof TColumns]: TColumns[K]['schema']};
    return new TableBuilderWithColumns({
      ...this.#schema,
      columns: columnSchemas,
    }) as any;
  }
}

export class TableBuilderWithColumns<TShape extends TableSchema> {
  readonly #schema: TShape;

  constructor(schema: TShape) {
    this.#schema = schema;
  }

  /**
   * Specifies the primary key(s) for the table.
   *
   * This cannot include columns that have default values since these are
   * generated separately on the client and server.
   */
  primaryKey<TPKColNames extends (keyof TShape['columns'])[]>(
    ...pkColumnNames: TPKColNames
  ) {
    return new TableBuilderWithColumns({
      ...this.#schema,
      primaryKey: pkColumnNames,
    });
  }

  get schema() {
    return this.#schema;
  }

  build() {
    // We can probably get the type system to throw an error if primaryKey is not called
    // before passing the schema to createSchema
    // Till then --
    if (this.#schema.primaryKey.length === 0) {
      throw new Error(`Table "${this.#schema.name}" is missing a primary key`);
    }
    const names = new Set<string>();
    for (const [col, {serverName}] of Object.entries(this.#schema.columns)) {
      const name = serverName ?? col;
      if (names.has(name)) {
        throw new Error(
          `Table "${
            this.#schema.name
          }" has multiple columns referencing "${name}"`,
        );
      }
      names.add(name);
    }
    return this.#schema;
  }
}

class ColumnBuilder<TShape extends SchemaValue<any>> {
  protected readonly _schema: TShape;
  constructor(schema: TShape) {
    this._schema = schema;
  }

  /**
   * Allows the column to be named differently in the database.
   *
   * @param serverName - The name of the column in the database.
   */
  from<ServerName extends string>(serverName: ServerName) {
    return new ColumnBuilder<TShape & {serverName: string}>({
      ...this._schema,
      serverName,
    });
  }

  /**
   * Allows the column to be `null` or undefined on insert.
   *
   * @deprecated Use `nullable` instead.
   */
  optional(): ColumnBuilder<Omit<TShape, 'nullable'> & {nullable: true}> {
    return new ColumnBuilder({
      ...this._schema,
      nullable: true,
    });
  }

  /**
   * Allows the column to be `null` or undefined on insert.
   *
   * This affects the select model of the table - columns with `nullable` will be
   * nullable on select.
   */
  nullable(): ColumnBuilder<Omit<TShape, 'nullable'> & {nullable: true}> {
    return new ColumnBuilder({
      ...this._schema,
      nullable: true,
    });
  }

  /**
   * Provides default values for the column on insert and/or update operations.
   *
   * **The default value generated by the client is independent of the server.**
   * It can only run the same function independently on the client and server,
   * which may result in different values being generated on the server. If you want
   * the same value on the client and server (e.g. for an ID column), you should generate
   * values outside of the mutation.
   *
   * Use `server: 'db'` to indicate that the server should use a database-generated value.
   *
   * @example
   * ```ts
   * const member = table('member')
   *   .columns({
   *     id: string(),
   *     createdAt: number().default({
   *       insert: {
   *         client: () => Date.now(),
   *         server: () => Date.now(), // or 'db'
   *       }
   *     }),
   *     updatedAt: number().default({
   *       insert: {
   *         client: () => Date.now(),
   *         server: 'db',
   *       },
   *       update: {
   *         client: () => Date.now(),
   *         server: 'db',
   *       },
   *     }),
   *   })
   *   .primaryKey('id');
   * ```
   */
  default<
    TExistingType extends TShape extends SchemaValueWithCustomType<infer V>
      ? V
      : never,
    TDefaultConfig extends DefaultConfig<TExistingType>,
  >(
    config: TDefaultConfig,
  ): ColumnBuilder<
    Omit<TShape, 'defaultConfig'> & {
      defaultConfig: TDefaultConfig;
    }
  > {
    return new ColumnBuilder({
      ...this._schema,
      defaultConfig: config,
    });
  }

  get schema() {
    return this._schema;
  }
}

export type {ColumnBuilder};
