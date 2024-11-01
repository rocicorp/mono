import {assert} from '../../shared/src/asserts.js';
import {sortedEntries} from '../../shared/src/sorted-entries.js';
import type {Writable} from '../../shared/src/writable.js';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.js';
import {
  type SchemaValue,
  isFieldRelationship,
  type FieldRelationship,
  type JunctionRelationship,
  type TableSchema,
} from './table-schema.js';

declare const normalized: unique symbol;

type Normalized<T> = T & {readonly [normalized]: true};

/**
 * We need a cache of the normalized table schemas to handle circular
 * dependencies.
 */
export type TableSchemaCache = Map<TableSchema, NormalizedTableSchema>;

export class NormalizedTableSchema {
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
    return normalizedTableSchema as NormalizedTableSchema;
  }

  normalizedTableSchema = new NormalizedTableSchema(
    tableSchema,
    tableSchemaCache,
  );
  return normalizedTableSchema as NormalizedTableSchema;
}

export type NormalizedPrimaryKey = Normalized<PrimaryKey>;

function isSorted(arr: readonly string[]): boolean {
  for (let i = 1; i < arr.length; i++) {
    if (arr[i - 1] >= arr[i]) {
      return false;
    }
  }
  return true;
}

function assertNoDuplicates(arr: readonly string[]): void {
  // arr is already sorted.
  for (let i = 1; i < arr.length; i++) {
    assert(arr[i - 1] !== arr[i], 'Primary key must not contain duplicates');
  }
}

export function normalizePrimaryKey(arr: PrimaryKey): NormalizedPrimaryKey {
  if (isSorted(arr)) {
    return arr as NormalizedPrimaryKey;
  }

  arr = [...arr].sort() as unknown as PrimaryKey;
  assertNoDuplicates(arr);
  return arr as NormalizedPrimaryKey;
}

function normalizeColumns(
  columns: Record<string, SchemaValue>,
  primaryKey: PrimaryKey,
): Record<string, SchemaValue> {
  const rv: Writable<Record<string, SchemaValue>> = {};
  for (const pk of primaryKey) {
    const schemaValue = columns[pk];
    assert(schemaValue, `Primary key column "${pk}" not found`);
    const {type, optional} = schemaValue;
    assert(!optional, `Primary key column "${pk}" cannot be optional`);
    assert(
      type === 'string' || type === 'number' || type === 'boolean',
      `Primary key column "${pk}" must be a string, number, or boolean. Got ${type}`,
    );
  }
  for (const [name, column] of sortedEntries(columns)) {
    rv[name] = normalizeSchemaValue(column);
  }
  return rv;
}

function normalizeSchemaValue(value: SchemaValue): SchemaValue {
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
  for (const [name, relationship] of sortedEntries(relationships)) {
    rv[name] = normalizeRelationship(relationship, tableSchemaCache);
  }
  return rv;
}

type NormalizedRelationship =
  | NormalizedFieldRelationship
  | NormalizedJunctionRelationship;

type Relationship = TableSchema['relationships'][string];

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
  source: string;
  dest: {
    field: string;
    schema: NormalizedTableSchema;
  };
};

function normalizeFieldRelationship(
  relationship: FieldRelationship<TableSchema, TableSchema>,
  tableSchemaCache: TableSchemaCache,
): NormalizedFieldRelationship {
  return {
    source: relationship.source,
    dest: {
      field: relationship.dest.field,
      schema: normalizeLazyTableSchema(
        relationship.dest.schema,
        tableSchemaCache,
      ),
    },
  };
}

type NormalizedJunctionRelationship = {
  source: string;
  junction: {
    sourceField: string;
    destField: string;
    schema: NormalizedTableSchema;
  };
  dest: {
    field: string;
    schema: NormalizedTableSchema;
  };
};

function normalizeJunctionRelationship(
  relationship: JunctionRelationship<TableSchema, TableSchema, TableSchema>,
  tableSchemaCache: TableSchemaCache,
): NormalizedJunctionRelationship {
  return {
    source: relationship.source,
    junction: {
      sourceField: relationship.junction.sourceField,
      destField: relationship.junction.destField,
      schema: normalizeLazyTableSchema(
        relationship.junction.schema,
        tableSchemaCache,
      ),
    },
    dest: {
      field: relationship.dest.field,
      schema: normalizeLazyTableSchema(
        relationship.dest.schema,
        tableSchemaCache,
      ),
    },
  };
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
