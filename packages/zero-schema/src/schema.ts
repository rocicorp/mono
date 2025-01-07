import {assert} from '../../shared/src/asserts.js';
import {
  type FieldRelationship,
  type Relationship,
  type TableSchema,
  isFieldRelationship,
} from './table-schema.js';

export type Schema = {
  readonly version: number;
  readonly tables: { readonly [table: string]: TableSchema };
};

export function createSchema<const S extends Schema>(schema: S): S {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    assert(
      tableName === table.tableName,
      `createSchema tableName mismatch, expected ${tableName} === ${table.tableName}`,
    );

    // Assert that all relationship -> destSchema entries are present in schema.tables
    const { relationships } = table;
    if (relationships) {
      for (const [
        relationshipName,
        relationship,
      ] of Object.entries(relationships)) {
        checkRelationship(
          relationship,
          tableName,
          relationshipName as keyof S['tables'][typeof tableName]['relationships'],
          schema,
        );
      }
    }
  }
  return schema as S;
}

function checkDestSchema<
  S extends Schema,
  TableName extends keyof S['tables'],
  RelationshipName extends keyof S['tables'][TableName]['relationships'],
>(
  schema: S,
  tableName: TableName,
  relationshipName: RelationshipName,
  relationship: FieldRelationship,
) {
  const destSchema =
    typeof relationship.destSchema === 'function'
      ? relationship.destSchema()
      : relationship.destSchema;

  assert(
    schema.tables[destSchema.tableName],
    `createSchema relationship missing, ${String(tableName)} relationship ${String(relationshipName)} not present in schema.tables`,
  );
}

function checkRelationship<
  const S extends Schema,
  TableName extends keyof S['tables'],
  RelationshipName extends keyof S['tables'][TableName]['relationships'],
>(
  relationship: Relationship,
  tableName: TableName,
  relationshipName: RelationshipName,
  schema: S,
) {
  if (isFieldRelationship(relationship)) {
    checkDestSchema(schema, tableName, relationshipName, relationship);
  } else {
    for (const junctionRelationship of relationship) {
      checkDestSchema(
        schema,
        tableName,
        relationshipName,
        junctionRelationship,
      );
    }
  }
}
