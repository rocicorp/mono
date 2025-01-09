import {assert} from '../../shared/src/asserts.js';
import {sortedEntries} from '../../shared/src/sorted-entries.js';
import type {Writable} from '../../shared/src/writable.js';
import type {CompoundKey} from '../../zero-protocol/src/ast.js';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.js';
import {
  isFieldRelationship,
  type FieldRelationship,
  type JunctionRelationship,
  type Relationship,
  type SchemaValue,
  type TableSchema,
  type ValueType,
} from './table-schema.js';

declare const normalized: unique symbol;

type Normalized<T> = T & {readonly [normalized]: true};

/**
 * We need a cache of the normalized table schemas to handle circular
 * dependencies.
 */
export type TableSchemaCache = Map<TableSchema, NormalizedTableSchema>;

export class NormalizedTableSchema implements TableSchema {
  declare readonly [normalized]: true;
  readonly tableName: string;
  readonly primaryKey: NormalizedPrimaryKey;
  readonly columns: Record<string, SchemaValue>;
  readonly relationships: {readonly [name: string]: NormalizedRelationship};

  constructor(
    tableSchema: TableSchema,
    tableSchemaCache: TableSchemaCache,
    assertFieldRelation: AssertFieldRelation,
  ) {
    this.tableName = tableSchema.tableName;
    const primaryKey = normalizePrimaryKey(tableSchema.primaryKey);
    this.primaryKey = primaryKey;
    this.columns = normalizeColumns(tableSchema.columns, primaryKey);
    tableSchemaCache.set(tableSchema, this);
    this.relationships = normalizeRelationships(
      this.tableName,
      tableSchema.relationships,
      tableSchemaCache,
      assertFieldRelation,
    );
  }
}

const noop = () => {};

export function normalizeTableSchema(
  tableSchema: TableSchema | NormalizedTableSchema,
): NormalizedTableSchema {
  return normalizeTableSchemaWithCache(
    tableSchema,
    tableSchema.tableName,
    new Map(),
    noop,
  );
}

export type AssertFieldRelation = (
  tableName: string,
  relationShipName: string,
  relation: NormalizedFieldRelationship,
) => void;

export function normalizeTableSchemaWithCache(
  tableSchema: TableSchema | NormalizedTableSchema,
  expectedTableName: string,
  tableSchemaCache: TableSchemaCache,
  assertFieldRelation: AssertFieldRelation,
): NormalizedTableSchema {
  if (tableSchema instanceof NormalizedTableSchema) {
    return tableSchema;
  }
  assert(
    tableSchema.tableName === expectedTableName,
    `Table name mismatch: "${tableSchema.tableName}" !== "${expectedTableName}"`,
  );

  let normalizedTableSchema = tableSchemaCache.get(tableSchema);
  if (normalizedTableSchema) {
    return normalizedTableSchema;
  }

  normalizedTableSchema = new NormalizedTableSchema(
    tableSchema,
    tableSchemaCache,
    assertFieldRelation,
  );
  return normalizedTableSchema as NormalizedTableSchema;
}

export type NormalizedPrimaryKey = Normalized<PrimaryKey>;

function assertNoDuplicates(arr: readonly string[]): void {
  assert(
    new Set(arr).size === arr.length,
    'Primary key must not contain duplicates',
  );
}

export function normalizePrimaryKey(
  primaryKey: PrimaryKey | string,
): NormalizedPrimaryKey {
  if (typeof primaryKey === 'string') {
    return [primaryKey] as const as NormalizedPrimaryKey;
  }
  assertNoDuplicates(primaryKey);
  return primaryKey as NormalizedPrimaryKey;
}

function normalizeColumns(
  columns: Record<string, SchemaValue | ValueType>,
  primaryKey: PrimaryKey,
): Record<string, SchemaValue> {
  const rv: Writable<Record<string, SchemaValue>> = {};
  for (const pk of primaryKey) {
    const schemaValue = columns[pk];
    assert(schemaValue, `Primary key column "${pk}" not found`);
    if (typeof schemaValue !== 'string') {
      const {type, optional} = schemaValue;
      assert(!optional, `Primary key column "${pk}" cannot be optional`);
      assert(
        type === 'string' || type === 'number' || type === 'boolean',
        `Primary key column "${pk}" must be a string, number, or boolean. Got ${type}`,
      );
    }
  }
  for (const [name, column] of sortedEntries(columns)) {
    rv[name] = normalizeColumn(column);
  }
  return rv;
}

function normalizeColumn(value: SchemaValue | ValueType): SchemaValue {
  if (typeof value === 'string') {
    return {type: value, optional: false};
  }
  return {
    type: value.type,
    optional: value.optional ?? false,
  };
}

type Relationships = TableSchema['relationships'];

type NormalizedRelationships = {
  readonly [name: string]: NormalizedRelationship;
};

function normalizeRelationships(
  tableName: string,
  relationships: Relationships,
  tableSchemaCache: TableSchemaCache,
  assertFieldRelation: AssertFieldRelation,
): NormalizedRelationships {
  const rv: Writable<NormalizedRelationships> = {};
  if (relationships) {
    for (const [relationshipName, relationship] of sortedEntries(
      relationships,
    )) {
      rv[relationshipName] = normalizeRelationship(
        tableName,
        relationshipName,
        relationship,
        tableSchemaCache,
        assertFieldRelation,
      );
    }
  }
  return rv;
}

export type DecycledNormalizedTableSchema = Omit<
  NormalizedTableSchema,
  'relationships'
> & {
  readonly relationships: {
    readonly [relationship: string]:
      | DecycledNormalizedFieldRelationship
      | readonly [
          DecycledNormalizedFieldRelationship,
          DecycledNormalizedFieldRelationship,
        ];
  };
};

export type DecycledNormalizedFieldRelationship = Omit<
  NormalizedFieldRelationship,
  'destSchema'
> & {readonly destSchema: string};

type NormalizedRelationship =
  | NormalizedFieldRelationship
  | NormalizedJunctionRelationship;

function normalizeRelationship(
  tableName: string,
  relationshipName: string,
  relationship: Relationship,
  tableSchemaCache: TableSchemaCache,
  assertFieldRelation: AssertFieldRelation,
): NormalizedRelationship {
  if (isFieldRelationship(relationship)) {
    return normalizeFieldRelationship(
      tableName,
      relationshipName,
      relationship,
      tableSchemaCache,
      assertFieldRelation,
    );
  }
  return normalizeJunctionRelationship(
    tableName,
    relationshipName,
    relationship,
    tableSchemaCache,
    assertFieldRelation,
  );
}

export type NormalizedFieldRelationship = {
  sourceField: CompoundKey;
  destField: CompoundKey;
  destSchema: NormalizedTableSchema;
};

function normalizeFieldRelationship(
  tableName: string,
  relationshipName: string,
  relationship: FieldRelationship,
  tableSchemaCache: TableSchemaCache,
  assertFieldRelation: AssertFieldRelation,
): NormalizedFieldRelationship {
  const sourceField = normalizeFieldName(relationship.sourceField);
  const destField = normalizeFieldName(relationship.destField);
  assert(
    sourceField.length === destField.length,
    'Source and destination fields must have the same length',
  );
  const destSchema = normalizeLazyTableSchema(
    relationship.destSchema,
    tableSchemaCache,
    assertFieldRelation,
  );
  const normalized: NormalizedFieldRelationship = {
    sourceField,
    destField,
    destSchema,
  };
  assertFieldRelation(tableName, relationshipName, normalized);
  return normalized;
}

export type NormalizedJunctionRelationship = readonly [
  NormalizedFieldRelationship,
  NormalizedFieldRelationship,
];

function normalizeJunctionRelationship(
  tableName: string,
  relationshipName: string,
  relationship: JunctionRelationship,
  tableSchemaCache: TableSchemaCache,
  assertFieldRelation: AssertFieldRelation,
): NormalizedJunctionRelationship {
  const first = normalizeFieldRelationship(
    tableName,
    relationshipName,
    relationship[0],
    tableSchemaCache,
    assertFieldRelation,
  );
  const second = normalizeFieldRelationship(
    first.destSchema.tableName,
    relationshipName,
    relationship[1],
    tableSchemaCache,
    assertFieldRelation,
  );
  return [first, second];
}

function normalizeLazyTableSchema<TS extends TableSchema>(
  tableSchema: TS | (() => TS),
  buildCache: TableSchemaCache,
  assertFieldRelation: AssertFieldRelation,
): NormalizedTableSchema {
  const tableSchemaInstance =
    typeof tableSchema === 'function' ? tableSchema() : tableSchema;
  return normalizeTableSchemaWithCache(
    tableSchemaInstance,
    tableSchemaInstance.tableName, // Don't care about name here.
    buildCache,
    assertFieldRelation,
  );
}

function normalizeFieldName(sourceField: string | CompoundKey): CompoundKey {
  if (typeof sourceField === 'string') {
    return [sourceField];
  }
  assert(sourceField.length > 0, 'Expected at least one field');
  return sourceField;
}

export function normalizeTables(
  tables: Record<string, TableSchema>,
): Record<string, NormalizedTableSchema> {
  const result: Record<string, NormalizedTableSchema> = {};
  const assertFieldRelation: AssertFieldRelation = tableName =>
    tableName in tables;
  for (const [name, table] of sortedEntries(tables)) {
    result[name] = normalizeTableSchemaWithCache(
      table,
      name,
      new Map(),
      assertFieldRelation,
    );
  }
  return result;
}
