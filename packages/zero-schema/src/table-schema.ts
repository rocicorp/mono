import {assert} from '../../shared/src/asserts.js';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.js';

export type ValueType = 'string' | 'number' | 'boolean' | 'null' | 'json';

/**
 * `related` calls need to know what the available relationships are.
 * The `schema` type encodes this information.
 */
export type SchemaValue = {
  type: ValueType;
  optional?: boolean | undefined;
};

export type TableSchema = {
  readonly tableName: string;
  readonly columns: Record<string, SchemaValue | ValueType>;
  readonly relationships?: {readonly [name: string]: Relationship} | undefined;
  readonly primaryKey: PrimaryKey | string;
};

export function createTableSchema<const T extends TableSchema>(schema: T) {
  return schema as T;
}

type TypeNameToTypeMap = {
  string: string;
  number: number;
  boolean: boolean;
  null: null;

  // In schema-v2, the user will be able to specify the TS type that
  // the JSON should match and `any`` will no
  // longer be used here.
  // ReadOnlyJSONValue is not used as it causes
  // infinite depth errors to pop up for users of our APIs.

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  json: any;
};

type ColumnTypeName<T extends SchemaValue | ValueType> = T extends SchemaValue
  ? T['type']
  : T;

/**
 * Given a schema value, return the TypeScript type.
 *
 * This allows us to create the correct return type for a
 * query that has a selection.
 */
export type SchemaValueToTSType<T extends SchemaValue | ValueType> =
  T extends ValueType
    ? TypeNameToTypeMap[T]
    : T extends {
        optional: true;
      }
    ? TypeNameToTypeMap[ColumnTypeName<T>] | null
    : TypeNameToTypeMap[ColumnTypeName<T>];

export type Supertype<TSchemas extends TableSchema[]> = {
  tableName: TSchemas[number]['tableName'];
  primaryKey: TSchemas[number]['primaryKey'];
  columns: {
    [K in keyof TSchemas[number]['columns']]: TSchemas[number]['columns'][K];
  };
  relationships?:
    | {
        [K in keyof TSchemas[number]['relationships']]: TSchemas[number]['relationships'][K];
      }
    | undefined;
};

/**
 * A schema might have a relationship to itself.
 * Given we cannot reference a variable in the same statement we initialize
 * the variable, we allow use of a function to get around this.
 */
type Lazy<T> = T | (() => T);

export type Relationship = FieldRelationship | JunctionRelationship;

export type AtLeastOne<T> = readonly [T, ...T[]];

export function atLeastOne<T>(arr: readonly T[]): AtLeastOne<T> {
  if (arr.length === 0) {
    throw new Error('Expected at least one element');
  }
  return arr as AtLeastOne<T>;
}

type FieldName<TSchema extends TableSchema> =
  | (keyof TSchema['columns'] & string)
  | AtLeastOne<keyof TSchema['columns'] & string>;

/**
 * A relationship between two entities where
 * that relationship is defined via fields on both entities.
 */
export type FieldRelationship<
  TSourceSchema extends TableSchema = TableSchema,
  TDestSchema extends TableSchema = TableSchema,
> = {
  sourceField: FieldName<TSourceSchema>;
  destField: FieldName<TDestSchema>;
  destSchema: Lazy<TDestSchema>;
};

/**
 * A relationship between two entities where
 * that relationship is defined via a junction table.
 */
export type JunctionRelationship<
  TSourceSchema extends TableSchema = TableSchema,
  TJunctionSchema extends TableSchema = TableSchema,
  TDestSchema extends TableSchema = TableSchema,
> = readonly [
  FieldRelationship<TSourceSchema, TJunctionSchema>,
  FieldRelationship<TJunctionSchema, TDestSchema>,
];

export function isFieldRelationship(
  relationship: Relationship,
): relationship is FieldRelationship {
  return !isJunctionRelationship(relationship);
}

export function assertFieldRelationship(
  relationship: Relationship,
): asserts relationship is FieldRelationship {
  assert(isFieldRelationship(relationship), 'Expected field relationship');
}

export function isJunctionRelationship(
  relationship: Relationship,
): relationship is JunctionRelationship {
  return Array.isArray(relationship);
}

export function assertJunctionRelationship(
  relationship: Relationship,
): asserts relationship is JunctionRelationship {
  assert(
    isJunctionRelationship(relationship),
    'Expected junction relationship',
  );
}

/**
 * Calling `related` on `Query` returns a new Query
 * since `related` moves through the relationship. This function takes
 * 1. A schema
 * 2. A relationship name
 * and returns the schema of the entity at the other end of the
 * relationship.
 */
export type PullSchemaForRelationship<
  TSchema extends TableSchema,
  TRelationship extends keyof TSchema['relationships'],
> = TSchema['relationships'][TRelationship] extends FieldRelationship<
  TableSchema,
  infer TSchema
>
  ? TSchema
  : TSchema['relationships'][TRelationship] extends JunctionRelationship<
      TableSchema,
      TableSchema,
      infer TSchema
    >
  ? TSchema
  : never;
