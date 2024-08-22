import {ValueType} from '../ivm2/schema.js';

/**
 * `related` calls need to know what the available relationships are.
 * The `schema` type encodes this information.
 */
export type SchemaValue = {
  type: ValueType;
  optional?: boolean;
};
export type EntitySchema = {
  readonly table: string;
  primaryKey: readonly [
    keyof EntitySchema['fields'],
    ...(keyof EntitySchema['fields'])[],
  ];
  readonly fields: Record<string, SchemaValue>;
  readonly relationships?: Record<
    string,
    | FieldRelationship<EntitySchema, EntitySchema>
    | JunctionRelationship<EntitySchema, EntitySchema, EntitySchema>
  >;
};

/**
 * A schema might have a relationship to itself.
 * Given we cannot reference a variable in the same statement we initialize
 * the variable, we use a lazy function to get around this.
 */
export type Lazy<T> = () => T;

type Relationship =
  | FieldRelationship<EntitySchema, EntitySchema>
  | JunctionRelationship<EntitySchema, EntitySchema, EntitySchema>;

/**
 * A relationship between two entities where
 * that relationship is defined via fields on both entities.
 */
type FieldRelationship<
  TSourceSchema extends EntitySchema,
  TDestSchema extends EntitySchema,
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
  TSourceSchema extends EntitySchema,
  TJunctionSchema extends EntitySchema,
  TDestSchema extends EntitySchema,
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
): relationship is FieldRelationship<EntitySchema, EntitySchema> {
  return (
    (
      relationship as JunctionRelationship<
        EntitySchema,
        EntitySchema,
        EntitySchema
      >
    ).junction !== undefined
  );
}

export function isJunctionRelationship(
  relationship: Relationship,
): relationship is JunctionRelationship<
  EntitySchema,
  EntitySchema,
  EntitySchema
> {
  return !isFieldRelationship(relationship);
}

/**
 * Calling `related` on `EntityQuery` returns a new EntityQuery
 * since `related` moves through the relationship. This function takes
 * 1. A schema
 * 2. A relationship name
 * and returns the schema of the entity at the other end of the
 * relationship.
 */
export type PullSchemaForRelationship<
  TEntitySchema extends EntitySchema,
  TRelationship extends keyof TEntitySchema['relationships'],
> = TEntitySchema['relationships'][TRelationship] extends FieldRelationship<
  EntitySchema,
  infer TSchema
>
  ? TSchema
  : TEntitySchema['relationships'][TRelationship] extends JunctionRelationship<
      EntitySchema,
      EntitySchema,
      infer TSchema
    >
  ? TSchema
  : never;
