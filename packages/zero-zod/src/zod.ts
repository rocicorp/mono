import {
  z,
  type ZodAny,
  type ZodBoolean,
  type ZodNull,
  type ZodNullable,
  type ZodNumber,
  type ZodObject,
  type ZodOptional,
  type ZodString,
  type ZodType,
} from 'zod';
import {unreachable} from '../../shared/src/asserts.ts';
import type {
  SchemaValue,
  ValueType,
} from '../../zero-types/src/schema-value.ts';
import type {TableSchema} from '../../zero-types/src/schema.ts';

function baseSchema(type: ValueType) {
  switch (type) {
    case 'string':
      return z.string();
    case 'number':
      return z.number();
    case 'boolean':
      return z.boolean();
    case 'null':
      return z.null();
    case 'json':
      return z.any();
    default:
      unreachable(type);
  }
}

export type ColumnZodType<V> = V extends SchemaValue
  ? V extends {customType: infer T}
    ? ZodType<T, T>
    : V['type'] extends 'string'
      ? ZodString
      : V['type'] extends 'number'
        ? ZodNumber
        : V['type'] extends 'boolean'
          ? ZodBoolean
          : V['type'] extends 'null'
            ? ZodNull
            : V['type'] extends 'json'
              ? ZodAny
              : ZodAny
  : never;

export type RowZodShape<TTable extends TableSchema> = {
  readonly [K in keyof TTable['columns']]: TTable['columns'][K] extends {
    optional: true;
  }
    ? ZodNullable<ColumnZodType<TTable['columns'][K]>>
    : ColumnZodType<TTable['columns'][K]>;
};

export type InsertZodShape<TTable extends TableSchema> = {
  readonly [K in keyof TTable['columns']]: K extends TTable['primaryKey'][number]
    ? ColumnZodType<TTable['columns'][K]>
    : TTable['columns'][K] extends {optional: true}
      ? ZodOptional<ZodNullable<ColumnZodType<TTable['columns'][K]>>>
      : ColumnZodType<TTable['columns'][K]>;
};

export type UpdateZodShape<TTable extends TableSchema> = {
  readonly [K in keyof TTable['columns']]: K extends TTable['primaryKey'][number]
    ? ColumnZodType<TTable['columns'][K]>
    : TTable['columns'][K] extends {optional: true}
      ? ZodOptional<ZodNullable<ColumnZodType<TTable['columns'][K]>>>
      : ZodOptional<ColumnZodType<TTable['columns'][K]>>;
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
    column.optional === true ? schema.optional().nullable() : schema;
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
): ZodObject<RowZodShape<TTable>> {
  const shape: Record<string, ReturnType<typeof columnValueSchema>> = {};

  for (const [columnName, column] of Object.entries(table.columns)) {
    shape[columnName] = columnValueSchema(column);
  }

  return z.object(shape) as unknown as ZodObject<RowZodShape<TTable>>;
}

export function insertSchema<TTable extends TableSchema>(
  table: TTable,
): ZodObject<InsertZodShape<TTable>> {
  const primaryKeys = new Set(table.primaryKey);

  const shape: Record<string, ReturnType<typeof columnValueSchema>> = {};

  for (const [columnName, column] of Object.entries(table.columns)) {
    const valueSchema = columnValueSchema(column);
    const isOptional = !primaryKeys.has(columnName) && column.optional === true;
    const withOptionality = isOptional
      ? valueSchema.optional().nullable()
      : valueSchema;
    shape[columnName] = withOptionality;
  }

  return z.object(shape) as unknown as ZodObject<InsertZodShape<TTable>>;
}

export function updateSchema<TTable extends TableSchema>(
  table: TTable,
): ZodObject<UpdateZodShape<TTable>> {
  const primaryKeys = new Set(table.primaryKey);
  const shape: Record<string, ReturnType<typeof columnValueSchema>> = {};

  for (const [columnName, column] of Object.entries(table.columns)) {
    const valueSchema = columnValueSchema(column);
    const withOptionality = primaryKeys.has(columnName)
      ? valueSchema
      : valueSchema.optional();
    shape[columnName] = withOptionality;
  }

  return z.object(shape) as unknown as ZodObject<UpdateZodShape<TTable>>;
}

export function deleteSchema<TTable extends TableSchema>(
  table: TTable,
): ZodObject<DeleteZodShape<TTable>> {
  const shape: Record<string, ReturnType<typeof columnValueSchema>> = {};

  for (const columnName of table.primaryKey) {
    shape[columnName] = columnValueSchema(requireColumn(table, columnName));
  }

  return z.object(shape) as unknown as ZodObject<DeleteZodShape<TTable>>;
}
