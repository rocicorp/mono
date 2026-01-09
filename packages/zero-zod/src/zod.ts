import {
  nullable,
  optional,
  any as zodAny,
  boolean as zodBoolean,
  null as zodNull,
  number as zodNumber,
  object as zodObject,
  string as zodString,
  type ZodMiniAny,
  type ZodMiniBoolean,
  type ZodMiniNull,
  type ZodMiniNullable,
  type ZodMiniNumber,
  type ZodMiniObject,
  type ZodMiniOptional,
  type ZodMiniString,
  type ZodMiniType,
} from 'zod/mini';
import {unreachable} from '../../shared/src/asserts.ts';
import type {
  SchemaValue,
  ValueType,
} from '../../zero-types/src/schema-value.ts';
import type {TableSchema} from '../../zero-types/src/schema.ts';

function baseSchema(type: ValueType) {
  switch (type) {
    case 'string':
      return zodString();
    case 'number':
      return zodNumber();
    case 'boolean':
      return zodBoolean();
    case 'null':
      return zodNull();
    case 'json':
      return zodAny();
    default:
      unreachable(type);
  }
}

export type ColumnZodType<V> = V extends SchemaValue
  ? V extends {customType: infer T}
    ? ZodMiniType<T, T>
    : V['type'] extends 'string'
      ? ZodMiniString
      : V['type'] extends 'number'
        ? ZodMiniNumber
        : V['type'] extends 'boolean'
          ? ZodMiniBoolean
          : V['type'] extends 'null'
            ? ZodMiniNull
            : V['type'] extends 'json'
              ? ZodMiniAny
              : ZodMiniAny
  : never;

export type RowZodShape<TTable extends TableSchema> = {
  readonly [K in keyof TTable['columns']]: TTable['columns'][K] extends {
    optional: true;
  }
    ? ZodMiniNullable<ColumnZodType<TTable['columns'][K]>>
    : ColumnZodType<TTable['columns'][K]>;
};

export type InsertZodShape<TTable extends TableSchema> = {
  readonly [K in keyof TTable['columns']]: K extends TTable['primaryKey'][number]
    ? ColumnZodType<TTable['columns'][K]>
    : TTable['columns'][K] extends {optional: true}
      ? ZodMiniOptional<ZodMiniNullable<ColumnZodType<TTable['columns'][K]>>>
      : ColumnZodType<TTable['columns'][K]>;
};

export type UpdateZodShape<TTable extends TableSchema> = {
  readonly [K in keyof TTable['columns']]: K extends TTable['primaryKey'][number]
    ? ColumnZodType<TTable['columns'][K]>
    : TTable['columns'][K] extends {optional: true}
      ? ZodMiniOptional<ZodMiniNullable<ColumnZodType<TTable['columns'][K]>>>
      : ZodMiniOptional<ColumnZodType<TTable['columns'][K]>>;
};

export type DeleteZodShape<TTable extends TableSchema> = {
  readonly [K in Extract<
    TTable['primaryKey'][number],
    keyof TTable['columns']
  >]: ColumnZodType<TTable['columns'][K]>;
};

function columnValueSchema<V extends SchemaValue>(column: V): ColumnZodType<V> {
  const schema = baseSchema(column.type);
  const withOptional =
    column.optional === true ? optional(nullable(schema)) : schema;
  return withOptional as ColumnZodType<V>;
}

function requireColumn(table: TableSchema, columnName: string): SchemaValue {
  const column = table.columns[columnName];
  if (!column) {
    throw new Error(
      `Unexpected error with zero-zod: column "${columnName}" is missing from table "${table.name}".`,
    );
  }
  return column;
}

export function rowSchema<TTable extends TableSchema>(
  table: TTable,
): ZodMiniObject<RowZodShape<TTable>> {
  const shape: Record<string, ReturnType<typeof columnValueSchema>> = {};

  for (const [columnName, column] of Object.entries(table.columns)) {
    shape[columnName] = columnValueSchema(column);
  }

  return zodObject(shape) as unknown as ZodMiniObject<RowZodShape<TTable>>;
}

export function insertSchema<TTable extends TableSchema>(
  table: TTable,
): ZodMiniObject<InsertZodShape<TTable>> {
  const primaryKeys = new Set(table.primaryKey);

  const shape: Record<string, ReturnType<typeof columnValueSchema>> = {};

  for (const [columnName, column] of Object.entries(table.columns)) {
    const valueSchema = columnValueSchema(column);
    const isOptional = !primaryKeys.has(columnName) && column.optional === true;
    const withOptionality = isOptional
      ? optional(nullable(valueSchema))
      : valueSchema;
    shape[columnName] = withOptionality;
  }

  return zodObject(shape) as unknown as ZodMiniObject<InsertZodShape<TTable>>;
}

export function updateSchema<TTable extends TableSchema>(
  table: TTable,
): ZodMiniObject<UpdateZodShape<TTable>> {
  const primaryKeys = new Set(table.primaryKey);
  const shape: Record<string, ReturnType<typeof columnValueSchema>> = {};

  for (const [columnName, column] of Object.entries(table.columns)) {
    const valueSchema = columnValueSchema(column);
    const withOptionality = primaryKeys.has(columnName)
      ? valueSchema
      : column.optional === true
        ? optional(nullable(valueSchema))
        : optional(valueSchema);
    shape[columnName] = withOptionality;
  }

  return zodObject(shape) as unknown as ZodMiniObject<UpdateZodShape<TTable>>;
}

export function deleteSchema<TTable extends TableSchema>(
  table: TTable,
): ZodMiniObject<DeleteZodShape<TTable>> {
  const shape: Record<string, ReturnType<typeof columnValueSchema>> = {};

  for (const columnName of table.primaryKey) {
    shape[columnName] = columnValueSchema(requireColumn(table, columnName));
  }

  return zodObject(shape) as unknown as ZodMiniObject<DeleteZodShape<TTable>>;
}
