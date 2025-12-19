import {z, type ZodType, type ZodTypeAny} from 'zod';
import {jsonSchema as sharedJsonSchema} from '../../shared/src/json-schema.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {
  SchemaValue,
  SchemaValueToTSType,
  ValueType,
} from '../../zero-types/src/schema-value.ts';
import type {TableSchema} from '../../zero-types/src/schema.ts';
import type {
  DeleteID,
  InsertValue,
  UpdateValue,
} from '../../zql/src/mutate/crud.ts';
import type {Row} from '../../zql/src/query/query.ts';

function asType<T>(schema: ZodTypeAny): ZodType<T> {
  return schema as unknown as ZodType<T>;
}

function baseSchema(type: ValueType): ZodTypeAny {
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
      return makeJSONSchema();
  }
}

function columnValueSchema<V extends SchemaValue>(
  column: V,
): ZodType<SchemaValueToTSType<V>> {
  const schema = baseSchema(column.type);
  const withOptional = column.optional === true ? schema.nullable() : schema;
  return asType<SchemaValueToTSType<V>>(withOptional);
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
): ZodType<Row<TTable>> {
  const shape: Record<string, ZodTypeAny> = {};

  for (const [columnName, column] of Object.entries(table.columns)) {
    shape[columnName] = columnValueSchema(column);
  }

  return asType<Row<TTable>>(z.object(shape));
}

export function insertSchema<TTable extends TableSchema>(
  table: TTable,
): ZodType<InsertValue<TTable>> {
  const primaryKeys = new Set(table.primaryKey);
  const shape: Record<string, ZodTypeAny> = {};

  for (const [columnName, column] of Object.entries(table.columns)) {
    const valueSchema = columnValueSchema(column);
    const isOptional = !primaryKeys.has(columnName) && column.optional === true;
    shape[columnName] = isOptional
      ? valueSchema.optional().nullable()
      : valueSchema;
  }

  return asType<InsertValue<TTable>>(z.object(shape));
}

export function updateSchema<TTable extends TableSchema>(
  table: TTable,
): ZodType<UpdateValue<TTable>> {
  const primaryKeys = new Set(table.primaryKey);
  const shape: Record<string, ZodTypeAny> = {};

  for (const [columnName, column] of Object.entries(table.columns)) {
    const valueSchema = columnValueSchema(column);
    shape[columnName] = primaryKeys.has(columnName)
      ? valueSchema
      : valueSchema.optional().nullable();
  }

  return asType<UpdateValue<TTable>>(z.object(shape));
}

export function deleteSchema<TTable extends TableSchema>(
  table: TTable,
): ZodType<DeleteID<TTable>> {
  const shape: Record<string, ZodTypeAny> = {};

  for (const columnName of table.primaryKey) {
    shape[columnName] = columnValueSchema(requireColumn(table, columnName));
  }

  return asType<DeleteID<TTable>>(z.object(shape));
}

function makeJSONSchema(): ZodType<ReadonlyJSONValue> {
  return asType<ReadonlyJSONValue>(
    z.any().superRefine((value, ctx) => {
      try {
        sharedJsonSchema.parse(value);
      } catch (err) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message:
            err instanceof Error
              ? err.message
              : 'Invalid JSON: value is not JSON-serializable',
        });
      }
    }),
  );
}

export type {
  DeleteID,
  InsertValue,
  UpdateValue,
} from '../../zql/src/mutate/crud.ts';
export type {Row} from '../../zql/src/query/query.ts';
