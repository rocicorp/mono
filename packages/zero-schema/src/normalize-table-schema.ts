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

  constructor(tableSchema: TableSchema, tableSchemaCache: TableSchemaCache) {
    this.tableName = tableSchema.tableName;
    const primaryKey = normalizePrimaryKey(tableSchema.primaryKey);
    this.primaryKey = primaryKey;
    this.columns = normalizeColumns(tableSchema.columns, primaryKey);
    tableSchemaCache.set(tableSchema, this);
    this.relationships = normalizeRelationships(
      tableSchema.relationships,
      tableSchemaCache,
    );
  }
}

export function normalizeTableSchema(
  tableSchema: TableSchema | NormalizedTableSchema,
): NormalizedTableSchema {
  return normalizeTableSchemaWithCache(
    tableSchema,
    tableSchema.tableName,
    new Map(),
  );
}

export function normalizeTableSchemaWithCache(
  tableSchema: TableSchema | NormalizedTableSchema,
  expectedName: string,
  tableSchemaCache: TableSchemaCache,
): NormalizedTableSchema {
  if (tableSchema instanceof NormalizedTableSchema) {
    return tableSchema;
  }
  assert(
    tableSchema.tableName === expectedName,
    `Table name mismatch: "${tableSchema.tableName}" !== "${expectedName}"`,
  );

  let normalizedTableSchema = tableSchemaCache.get(tableSchema);
  if (normalizedTableSchema) {
    return normalizedTableSchema;
  }

  normalizedTableSchema = new NormalizedTableSchema(
    tableSchema,
    tableSchemaCache,
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
  relationships: Relationships,
  tableSchemaCache: TableSchemaCache,
): NormalizedRelationships {
  const rv: Writable<NormalizedRelationships> = {};
  if (relationships) {
    for (const [name, relationship] of sortedEntries(relationships)) {
      rv[name] = normalizeRelationship(relationship, tableSchemaCache);
    }
  }
  return rv;
}

type NormalizedRelationship =
  | NormalizedFieldRelationship
  | NormalizedJunctionRelationship;

function normalizeRelationship(
  relationship: Relationship,
  tableSchemaCache: TableSchemaCache,
): NormalizedRelationship {
  if (isFieldRelationship(relationship)) {
    return normalizeFieldRelationship(relationship, tableSchemaCache);
  }
  return normalizeJunctionRelationship(relationship, tableSchemaCache);
}

type NormalizedFieldRelationship = {
  sourceField: CompoundKey;
  destField: CompoundKey;
  destSchema: NormalizedTableSchema;
};

function normalizeFieldRelationship(
  relationship: FieldRelationship,
  tableSchemaCache: TableSchemaCache,
): NormalizedFieldRelationship {
  const sourceField = normalizeFieldName(relationship.sourceField);
  const destField = normalizeFieldName(relationship.destField);
  assert(
    sourceField.length === destField.length,
    'Source and destination fields must have the same length',
  );
  return {
    sourceField,
    destField,
    destSchema: normalizeLazyTableSchema(
      relationship.destSchema,
      tableSchemaCache,
    ),
  };
}

type NormalizedJunctionRelationship = readonly [
  NormalizedFieldRelationship,
  NormalizedFieldRelationship,
];

function normalizeJunctionRelationship(
  relationship: JunctionRelationship,
  tableSchemaCache: TableSchemaCache,
): NormalizedJunctionRelationship {
  return [
    normalizeFieldRelationship(relationship[0], tableSchemaCache),
    normalizeFieldRelationship(relationship[1], tableSchemaCache),
  ];
}

function normalizeLazyTableSchema<TS extends TableSchema>(
  tableSchema: TS | (() => TS),
  buildCache: TableSchemaCache,
): NormalizedTableSchema {
  const tableSchemaInstance =
    typeof tableSchema === 'function' ? tableSchema() : tableSchema;
  return normalizeTableSchemaWithCache(
    tableSchemaInstance,
    tableSchemaInstance.tableName, // Don't care about name here.
    buildCache,
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
  for (const [name, table] of sortedEntries(tables)) {
    result[name] = normalizeTableSchemaWithCache(table, name, new Map());
  }
  return result;
}
