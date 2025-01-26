import type {ReadonlyJSONValue} from '../../../shared/src/json.js';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import type {SchemaValue, TableSchema} from '../table-schema.js';

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
    optional: false,
    customType: null as unknown as T,
  });
}

export function number<T extends number = number>() {
  return new ColumnBuilder({
    type: 'number',
    optional: false,
    customType: null as unknown as T,
  });
}

export function boolean<T extends boolean = boolean>() {
  return new ColumnBuilder({
    type: 'boolean',
    optional: false,
    customType: null as unknown as T,
  });
}

export function json<T extends ReadonlyJSONValue = ReadonlyJSONValue>() {
  return new ColumnBuilder({
    type: 'json',
    optional: false,
    customType: null as unknown as T,
  });
}

export function enumeration<T extends string>() {
  return new ColumnBuilder({
    type: 'string',
    optional: false,
    customType: null as unknown as T,
  });
}

export const column = {
  string,
  number,
  boolean,
  json,
};

export class TableBuilder<TShape extends TableSchema> {
  readonly #schema: TShape;
  constructor(schema: TShape) {
    this.#schema = schema;
  }

  columns<TColumns extends Record<string, ColumnBuilder<SchemaValue>>>(
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
    return this.#schema;
  }
}

export class ColumnBuilder<TShape extends SchemaValue<any>> {
  readonly #schema: TShape;
  constructor(schema: TShape) {
    this.#schema = schema;
  }

  optional(): ColumnBuilder<Omit<TShape, 'optional'> & {optional: true}> {
    return new ColumnBuilder({
      ...this.#schema,
      optional: true,
    });
  }

  get schema() {
    return this.#schema;
  }
}
