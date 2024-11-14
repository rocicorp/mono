import type {PrimaryKey} from '../../zero-protocol/src/primary-key.js';

export type ValueType = 'string' | 'number' | 'boolean' | 'null' | 'json';

/**
 * `related` calls need to know what the available relationships are.
 * The `schema` type encodes this information.
 */
export type SchemaValue = {
  type: ValueType;
  optional?: boolean;
};

export type SourceOrTableSchema = {
  readonly tableName: string;
  readonly primaryKey: PrimaryKey;
  readonly columns: Record<string, SchemaValue>;
};

export type TableSchema = SourceOrTableSchema & {
  readonly relationships: {readonly [name: string]: Relationship};
};

export function createTableSchema<const T extends TableSchema>(schema: T) {
  return schema as T;
}

export type TableSchemaToRow<T extends TableSchema> = {
  [K in keyof T['columns']]: SchemaValueToTSType<T['columns'][K]>;
};

/**
 * For some reason this needs to be separated out from SchemaValueToTSType in
 * order for intellisense to show `boolean`. If this gets folded into
 * SchemaValueToTSType, intellisense will show
 * SchemaValueToTSType<{type: "boolean"}> instead.
 */
type BaseType<T extends SchemaValue> = T extends {type: 'string'}
  ? string
  : T extends {type: 'number'}
  ? number
  : T extends {type: 'boolean'}
  ? boolean
  : T extends {type: 'null'}
  ? null
  : T extends {type: 'json'}
  ? // In schema-v2, the user will be able to specify the TS type that
    // the JSON should match and `any`` will no
    // longer be used here.
    // ReadOnlyJSONValue is not used as it causes
    // infinite depth errors to pop up for users of our APIs.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    any
  : never;

/**
 * Given a schema value, return the TypeScript type.
 *
 * This allows us to create the correct return type for a
 * query that has a selection.
 */
export type SchemaValueToTSType<T extends SchemaValue> = T extends {
  optional: true;
}
  ? BaseType<T> | null
  : BaseType<T>;

export type Supertype<TSchemas extends TableSchema[]> = {
  tableName: TSchemas[number]['tableName'];
  primaryKey: TSchemas[number]['primaryKey'];
  columns: {
    [K in keyof TSchemas[number]['columns']]: TSchemas[number]['columns'][K];
  };
  relationships: {
    [K in keyof TSchemas[number]['relationships']]: TSchemas[number]['relationships'][K];
  };
};

/**
 * A schema might have a relationship to itself.
 * Given we cannot reference a variable in the same statement we initialize
 * the variable, we use a function to get around this.
 */
export type Lazy<T> = () => T;

export type Relationship =
  | FieldRelationship<TableSchema, TableSchema>
  | JunctionRelationship<TableSchema, TableSchema, TableSchema>;

/**
 * A relationship between two entities where
 * that relationship is defined via fields on both entities.
 */
export type FieldRelationship<
  TSourceSchema extends TableSchema,
  TDestSchema extends TableSchema,
> = {
  source: keyof TSourceSchema['columns'];
  dest: {
    field: keyof TDestSchema['columns'];
    schema: TDestSchema | Lazy<TDestSchema>;
  };
};

/**
 * A relationship between two entities where
 * that relationship is defined via a junction table.
 */
export type JunctionRelationship<
  TSourceSchema extends TableSchema,
  TJunctionSchema extends TableSchema,
  TDestSchema extends TableSchema,
> = {
  source: keyof TSourceSchema['columns'];
  junction: {
    sourceField: keyof TJunctionSchema['columns'];
    destField: keyof TJunctionSchema['columns'];
    schema: TDestSchema | Lazy<TJunctionSchema>;
  };
  dest: {
    field: keyof TDestSchema['columns'];
    schema: TDestSchema | Lazy<TJunctionSchema>;
  };
};

export function isFieldRelationship(
  relationship: Relationship,
): relationship is FieldRelationship<TableSchema, TableSchema> {
  return (
    (
      relationship as JunctionRelationship<
        TableSchema,
        TableSchema,
        TableSchema
      >
    ).junction === undefined
  );
}

export function isJunctionRelationship(
  relationship: Relationship,
): relationship is JunctionRelationship<TableSchema, TableSchema, TableSchema> {
  return !isFieldRelationship(relationship);
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
