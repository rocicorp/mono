import type {PrimaryKeys, ValueType} from '../ivm/schema.js';

/**
 * `related` calls need to know what the available relationships are.
 * The `schema` type encodes this information.
 */
export type SchemaValue = {
  type: ValueType;
  optional?: boolean;
};

export type Schema = {
  readonly table: string;
  readonly primaryKey: PrimaryKeys;
  readonly fields: Record<string, SchemaValue>;
  readonly relationships?: {
    [key: string]:
      | FieldRelationship<Schema, Schema>
      | JunctionRelationship<Schema, Schema, Schema>;
  };
};

export function toInputArgs(schema: Schema) {
  const columns: Record<string, ValueType> = {};
  for (const [key, value] of Object.entries(schema.fields)) {
    columns[key] = value.type;
  }
  return {
    primaryKey: schema.primaryKey,
    columns,
    table: schema.table,
  };
}

/**
 * A schema might have a relationship to itself.
 * Given we cannot reference a variable in the same statement we initialize
 * the variable, we use a lazy function to get around this.
 */
export type Lazy<T> = () => T;

type Relationship =
  | FieldRelationship<Schema, Schema>
  | JunctionRelationship<Schema, Schema, Schema>;

/**
 * A relationship between two entities where
 * that relationship is defined via fields on both entities.
 */
type FieldRelationship<
  TSourceSchema extends Schema,
  TDestSchema extends Schema,
> = {
  source: keyof TSourceSchema['fields'];
  dest: {
    field: keyof TDestSchema['fields'];
    schema: TDestSchema | Lazy<TDestSchema>;
  };
};

/**
 * A relationship between two entities where
 * that relationship is defined via a junction table.
 */
type JunctionRelationship<
  TSourceSchema extends Schema,
  TJunctionSchema extends Schema,
  TDestSchema extends Schema,
> = {
  source: keyof TSourceSchema['fields'];
  junction: {
    sourceField: keyof TJunctionSchema['fields'];
    destField: keyof TJunctionSchema['fields'];
    schema: TDestSchema | Lazy<TJunctionSchema>;
  };
  dest: {
    field: keyof TDestSchema['fields'];
    schema: TDestSchema | Lazy<TJunctionSchema>;
  };
};

export function isFieldRelationship(
  relationship: Relationship,
): relationship is FieldRelationship<Schema, Schema> {
  return (
    (relationship as JunctionRelationship<Schema, Schema, Schema>).junction ===
    undefined
  );
}

export function isJunctionRelationship(
  relationship: Relationship,
): relationship is JunctionRelationship<Schema, Schema, Schema> {
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
  TSchema extends Schema,
  TRelationship extends keyof TSchema['relationships'],
> = TSchema['relationships'][TRelationship] extends FieldRelationship<
  Schema,
  infer TSchema
>
  ? TSchema
  : TSchema['relationships'][TRelationship] extends JunctionRelationship<
      Schema,
      Schema,
      infer TSchema
    >
  ? TSchema
  : never;
