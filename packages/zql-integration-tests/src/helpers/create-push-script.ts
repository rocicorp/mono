import {Faker} from '@faker-js/faker';
import type {
  AST,
  Condition,
  CorrelatedSubquery,
  SimpleCondition,
} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {SourceChange} from '../../../zql/src/ivm/source.ts';

type Rng = () => number;

// Mutable version of Row for generation
type MutableRow = Record<string, JSONValue>;

/**
 * Information about tables involved in the query
 */
type TableInfo = {
  name: string;
  schema: TableSchema;
  // Correlations where this table is the parent
  childRelations: Array<{
    childTable: string;
    parentField: readonly string[];
    childField: readonly string[];
  }>;
  // Correlations where this table is the child
  parentRelations: Array<{
    parentTable: string;
    parentField: readonly string[];
    childField: readonly string[];
  }>;
};

/**
 * Context for generating changes
 */
type ChangeContext = {
  rng: Rng;
  faker: Faker;
  schema: Schema;
  query: AST;
  tables: Map<string, TableInfo>;
  // Track generated rows by table for editing/removing
  generatedRows: Map<string, MutableRow[]>;
  // Global counter for generating unique primary keys
  pkCounter: number;
};

/**
 * Extract all tables involved in the query (root + related)
 */
function extractTables(query: AST, schema: Schema): Map<string, TableInfo> {
  const tables = new Map<string, TableInfo>();

  function processAST(
    ast: AST,
    parentTable?: string,
    correlation?: {
      parentField: readonly string[];
      childField: readonly string[];
    },
  ) {
    const tableName = ast.table;
    const tableSchema = schema.tables[tableName];

    if (!tableSchema) {
      throw new Error(`Table ${tableName} not found in schema`);
    }

    if (!tables.has(tableName)) {
      tables.set(tableName, {
        name: tableName,
        schema: tableSchema,
        childRelations: [],
        parentRelations: [],
      });
    }

    const tableInfo = tables.get(tableName)!;

    // Track parent-child relationship
    if (parentTable && correlation) {
      const parentInfo = tables.get(parentTable)!;
      parentInfo.childRelations.push({
        childTable: tableName,
        parentField: correlation.parentField,
        childField: correlation.childField,
      });
      tableInfo.parentRelations.push({
        parentTable,
        parentField: correlation.parentField,
        childField: correlation.childField,
      });
    }

    // Process related subqueries
    if (ast.related) {
      for (const rel of ast.related) {
        processAST(rel.subquery, tableName, rel.correlation);
      }
    }

    // Process correlated subqueries in WHERE conditions
    if (ast.where) {
      extractCorrelatedSubqueries(ast.where).forEach(correlated => {
        processAST(correlated.subquery, tableName, correlated.correlation);
      });
    }
  }

  processAST(query);
  return tables;
}

/**
 * Extract correlated subqueries from conditions
 */
function extractCorrelatedSubqueries(
  condition: Condition,
): CorrelatedSubquery[] {
  const result: CorrelatedSubquery[] = [];

  if (condition.type === 'correlatedSubquery') {
    result.push(condition.related);
  } else if (condition.type === 'and' || condition.type === 'or') {
    for (const cond of condition.conditions) {
      result.push(...extractCorrelatedSubqueries(cond));
    }
  }

  return result;
}

/**
 * Extract all simple conditions from a condition tree
 */
function extractSimpleConditions(condition: Condition): SimpleCondition[] {
  const result: SimpleCondition[] = [];

  if (condition.type === 'simple') {
    result.push(condition);
  } else if (condition.type === 'and' || condition.type === 'or') {
    for (const cond of condition.conditions) {
      result.push(...extractSimpleConditions(cond));
    }
  }

  return result;
}

/**
 * Generate a random row for a table with unique primary key
 */
function generateRow(
  ctx: ChangeContext,
  tableName: string,
  overrides?: Partial<MutableRow>,
): MutableRow {
  const tableInfo = ctx.tables.get(tableName)!;
  const row: MutableRow = {};

  for (const [columnName, columnSchema] of Object.entries(
    tableInfo.schema.columns,
  )) {
    const isPrimaryKey = tableInfo.schema.primaryKey.includes(columnName);

    // NEVER use overrides for primary key columns - always use counter for uniqueness
    if (isPrimaryKey) {
      row[columnName] = generateColumnValue(ctx, columnSchema, true);
    } else if (overrides && columnName in overrides) {
      // Use override for non-PK columns (like foreign keys, filter fields, etc.)
      row[columnName] = overrides[columnName]!;
    } else {
      row[columnName] = generateColumnValue(ctx, columnSchema, false);
    }
  }
  return row;
}

/**
 * Generate a value for a column
 */
function generateColumnValue(
  ctx: ChangeContext,
  columnSchema: {type: string; optional?: boolean | undefined},
  isPrimaryKey: boolean,
): JSONValue {
  const {rng, faker} = ctx;

  // Handle optional fields (but not for primary keys)
  if (!isPrimaryKey && columnSchema.optional && rng() < 0.2) {
    return null;
  }

  // Generate based on type
  switch (columnSchema.type) {
    case 'number':
      if (isPrimaryKey) {
        // Use global counter for unique PKs
        return ctx.pkCounter++;
      }
      return Math.floor(rng() * 1000);
    case 'string':
      if (isPrimaryKey) {
        // Use global counter for unique PKs
        return `pk_${ctx.pkCounter++}`;
      }
      const ret = faker.string.alphanumeric(10);
      return ret;
    case 'boolean':
      return rng() < 0.5;
    case 'json':
      return {};
    default:
      return faker.string.alphanumeric(10);
  }
}

/**
 * Add a row change and track it
 */
function addRow(
  ctx: ChangeContext,
  tableName: string,
  row: MutableRow,
  changes: [string, SourceChange][],
): void {
  changes.push([tableName, {type: 'add', row: row as Row}]);
  ctx.generatedRows.get(tableName)!.push(row);
}

/**
 * Edit a row change and track it
 */
function editRow(
  ctx: ChangeContext,
  tableName: string,
  oldRow: MutableRow,
  newRow: MutableRow,
  changes: [string, SourceChange][],
): void {
  changes.push([
    tableName,
    {type: 'edit', row: newRow as Row, oldRow: oldRow as Row},
  ]);

  const rows = ctx.generatedRows.get(tableName)!;
  const index = rows.indexOf(oldRow);
  if (index !== -1) {
    rows[index] = newRow;
  }
}

/**
 * Remove a row change and track it
 */
function removeRow(
  ctx: ChangeContext,
  tableName: string,
  row: MutableRow,
  changes: [string, SourceChange][],
): void {
  changes.push([tableName, {type: 'remove', row: row as Row}]);

  const rows = ctx.generatedRows.get(tableName)!;
  const index = rows.indexOf(row);
  if (index !== -1) {
    rows.splice(index, 1);
  }
}

/**
 * Generate initial baseline data for all tables
 */
function generateInitialData(
  ctx: ChangeContext,
  changes: [string, SourceChange][],
): void {
  // Generate a few rows for each table to establish baseline
  for (const tableName of ctx.tables.keys()) {
    const numRows = Math.floor(ctx.rng() * 5) + 2; // 2-6 rows
    for (let i = 0; i < numRows; i++) {
      const row = generateRow(ctx, tableName);
      addRow(ctx, tableName, row, changes);
    }
  }
}

/**
 * Generate changes that exercise limit behavior for a specific AST node
 */
function generateLimitChanges(
  ctx: ChangeContext,
  ast: AST,
  changes: [string, SourceChange][],
): void {
  if (ast.limit) {
    const tableName = ast.table;
    const limit = ast.limit;

    // Add rows to approach limit
    for (let i = 0; i < limit - 1; i++) {
      const row = generateRow(ctx, tableName);
      addRow(ctx, tableName, row, changes);
    }

    // Add row to hit limit exactly
    addRow(ctx, tableName, generateRow(ctx, tableName), changes);

    // Add rows to exceed limit
    for (let i = 0; i < 3; i++) {
      addRow(ctx, tableName, generateRow(ctx, tableName), changes);
    }

    // Remove some rows to go under limit
    const rows = ctx.generatedRows.get(tableName)!;
    if (rows.length > 0) {
      const rowToRemove = rows[Math.floor(ctx.rng() * rows.length)];
      removeRow(ctx, tableName, rowToRemove, changes);
    }
  }

  // Recurse into related subqueries
  if (ast.related) {
    for (const rel of ast.related) {
      generateLimitChanges(ctx, rel.subquery, changes);
    }
  }
}

/**
 * Generate changes that exercise filter conditions
 */
function generateFilterChanges(
  ctx: ChangeContext,
  ast: AST,
  changes: [string, SourceChange][],
): void {
  if (ast.where) {
    const tableName = ast.table;
    const conditions = extractSimpleConditions(ast.where);

    for (const condition of conditions) {
      if (condition.left.type === 'column') {
        const columnName = condition.left.name;
        const rightValue =
          condition.right.type === 'literal' ? condition.right.value : null;

        if (rightValue !== null && !Array.isArray(rightValue)) {
          // Generate row that matches the condition
          const matchingRow = generateRow(ctx, tableName, {
            [columnName]: rightValue as JSONValue,
          });
          addRow(ctx, tableName, matchingRow, changes);

          // Generate row that doesn't match
          const nonMatchingValue =
            typeof rightValue === 'number' ? rightValue + 1 : 'different';
          const nonMatchingRow = generateRow(ctx, tableName, {
            [columnName]: nonMatchingValue as JSONValue,
          });
          addRow(ctx, tableName, nonMatchingRow, changes);

          // Change matching to non-matching
          const isPrimaryKey = ctx.tables
            .get(tableName)!
            .schema.primaryKey.includes(columnName);
          const rowsToChange = ctx.generatedRows.get(tableName)!;
          if (rowsToChange.length > 0) {
            const oldRow = rowsToChange[0];
            if (isPrimaryKey) {
              // PK change is semantically remove + add
              removeRow(ctx, tableName, oldRow, changes);
              const newRow = generateRow(ctx, tableName, {
                [columnName]: nonMatchingValue as JSONValue,
              });
              addRow(ctx, tableName, newRow, changes);
            } else {
              // Non-PK change is an edit
              const newRow = {...oldRow, [columnName]: nonMatchingValue};
              editRow(ctx, tableName, oldRow, newRow, changes);
            }
          }
        }
      }
    }
  }

  // Recurse into related subqueries
  if (ast.related) {
    for (const rel of ast.related) {
      generateFilterChanges(ctx, rel.subquery, changes);
    }
  }

  // Recurse into correlated subqueries in WHERE
  if (ast.where) {
    for (const correlated of extractCorrelatedSubqueries(ast.where)) {
      generateFilterChanges(ctx, correlated.subquery, changes);
    }
  }
}

/**
 * Generate changes that exercise relationships
 */
function generateRelationshipChanges(
  ctx: ChangeContext,
  changes: [string, SourceChange][],
): void {
  for (const tableInfo of ctx.tables.values()) {
    // Generate parent without children
    if (tableInfo.childRelations.length > 0) {
      const parentRow = generateRow(ctx, tableInfo.name);
      addRow(ctx, tableInfo.name, parentRow, changes);
    }

    // Generate parent with children
    for (const relation of tableInfo.childRelations) {
      const parentRow = generateRow(ctx, tableInfo.name);
      addRow(ctx, tableInfo.name, parentRow, changes);

      // Generate matching children
      const parentFieldValues: Partial<MutableRow> = {};
      for (let i = 0; i < relation.parentField.length; i++) {
        parentFieldValues[relation.childField[i]] =
          parentRow[relation.parentField[i]];
      }

      const childRow = generateRow(ctx, relation.childTable, parentFieldValues);
      addRow(ctx, relation.childTable, childRow, changes);
    }

    // Generate orphan children (children without matching parent)
    if (tableInfo.parentRelations.length > 0) {
      const orphanRow = generateRow(ctx, tableInfo.name);
      addRow(ctx, tableInfo.name, orphanRow, changes);
    }
  }
}

/**
 * Generate changes that exercise start bounds
 */
function generateStartChanges(
  ctx: ChangeContext,
  ast: AST,
  changes: [string, SourceChange][],
): void {
  if (ast.start) {
    const tableName = ast.table;
    const {row: boundRow} = ast.start;

    // Generate row that exactly matches the bound
    // Need to convert Row to MutableRow
    const boundRowMutable: Partial<MutableRow> = {};
    for (const [key, value] of Object.entries(boundRow)) {
      if (value !== undefined) {
        boundRowMutable[key] = value as JSONValue;
      }
    }
    const matchingRow = generateRow(ctx, tableName, boundRowMutable);
    addRow(ctx, tableName, matchingRow, changes);

    // Generate rows before the bound (if using ordering)
    if (ast.orderBy) {
      for (const [columnName] of ast.orderBy) {
        if (columnName in boundRow) {
          const boundValue = boundRow[columnName];
          if (typeof boundValue === 'number') {
            // Add rows before the bound
            const beforeRow = generateRow(ctx, tableName, {
              [columnName]: boundValue - 10,
            });
            addRow(ctx, tableName, beforeRow, changes);
          }
        }
      }
    }

    // Generate rows after the bound
    if (ast.orderBy) {
      for (const [columnName] of ast.orderBy) {
        if (columnName in boundRow) {
          const boundValue = boundRow[columnName];
          if (typeof boundValue === 'number') {
            // Add rows after the bound
            const afterRow = generateRow(ctx, tableName, {
              [columnName]: boundValue + 10,
            });
            addRow(ctx, tableName, afterRow, changes);
          }
        }
      }
    }

    // If exclusive, the matching row should not be included in results
    // If inclusive, it should be included
    // This tests the exclusive flag behavior
  }

  // Recurse into related subqueries
  if (ast.related) {
    for (const rel of ast.related) {
      generateStartChanges(ctx, rel.subquery, changes);
    }
  }

  // Recurse into correlated subqueries in WHERE
  if (ast.where) {
    for (const correlated of extractCorrelatedSubqueries(ast.where)) {
      generateStartChanges(ctx, correlated.subquery, changes);
    }
  }
}

/**
 * Generate changes that exercise orderBy
 */
function generateOrderByChanges(
  ctx: ChangeContext,
  ast: AST,
  changes: [string, SourceChange][],
): void {
  if (ast.orderBy) {
    const tableName = ast.table;
    const tableInfo = ctx.tables.get(tableName)!;

    for (const [columnName] of ast.orderBy) {
      const columnSchema = tableInfo.schema.columns[columnName];

      // Generate rows with different ordering values based on type
      const numValues = Math.floor(ctx.rng() * 5) + 5; // 5-9 values
      const values: JSONValue[] = [];

      switch (columnSchema.type) {
        case 'number': {
          // Generate a mix of values, including duplicates
          const baseValues = new Set<number>();
          while (baseValues.size < Math.max(3, Math.floor(numValues / 2))) {
            baseValues.add(Math.floor(ctx.rng() * 1000));
          }
          values.push(...baseValues);
          // Add some duplicates
          values.push(...Array.from(baseValues).slice(0, 2));
          break;
        }
        case 'string': {
          // Generate strings that will sort differently
          const stringValues = new Set<string>();
          while (stringValues.size < Math.max(3, Math.floor(numValues / 2))) {
            stringValues.add(ctx.faker.string.alphanumeric(10));
          }
          values.push(...stringValues);
          // Add some duplicates
          values.push(...Array.from(stringValues).slice(0, 2));
          break;
        }
        case 'boolean':
          // Only two values possible
          values.push(true, false, true, false);
          break;
        default:
          // For other types, generate random strings
          for (let i = 0; i < numValues; i++) {
            values.push(ctx.faker.string.alphanumeric(10));
          }
      }

      for (const value of values) {
        const row = generateRow(ctx, tableName, {[columnName]: value});
        addRow(ctx, tableName, row, changes);
      }
    }
  }

  // Recurse into related subqueries
  if (ast.related) {
    for (const rel of ast.related) {
      generateOrderByChanges(ctx, rel.subquery, changes);
    }
  }

  // Recurse into correlated subqueries in WHERE
  if (ast.where) {
    for (const correlated of extractCorrelatedSubqueries(ast.where)) {
      generateOrderByChanges(ctx, correlated.subquery, changes);
    }
  }
}

/**
 * Generate random changes for robustness testing
 */
function generateRandomChanges(
  ctx: ChangeContext,
  changes: [string, SourceChange][],
): void {
  const numChanges = Math.floor(ctx.rng() * 100) + 5; // 5-104 random changes

  for (let i = 0; i < numChanges; i++) {
    const tableNames = Array.from(ctx.tables.keys());
    const tableName = tableNames[Math.floor(ctx.rng() * tableNames.length)];
    const rows = ctx.generatedRows.get(tableName)!;

    const changeType = ctx.rng();
    if (changeType < 0.5 || rows.length === 0) {
      // Add
      const row = generateRow(ctx, tableName);
      addRow(ctx, tableName, row, changes);
    } else if (changeType < 0.75 && rows.length > 0) {
      // Edit - change a random column
      const oldRow = rows[Math.floor(ctx.rng() * rows.length)];
      const columns = Object.keys(oldRow);
      const columnToChange = columns[Math.floor(ctx.rng() * columns.length)];
      const isPrimaryKey = ctx.tables
        .get(tableName)!
        .schema.primaryKey.includes(columnToChange);

      if (isPrimaryKey) {
        // PK change is semantically remove + add
        removeRow(ctx, tableName, oldRow, changes);
        const newRow = generateRow(ctx, tableName);
        addRow(ctx, tableName, newRow, changes);
      } else {
        // Non-PK change is an edit
        const newRow = {...oldRow};
        newRow[columnToChange] = generateColumnValue(
          ctx,
          ctx.tables.get(tableName)!.schema.columns[columnToChange],
          false,
        );
        editRow(ctx, tableName, oldRow, newRow, changes);
      }
    } else if (rows.length > 0) {
      // Remove
      const row = rows[Math.floor(ctx.rng() * rows.length)];
      removeRow(ctx, tableName, row, changes);
    }
  }
}

/**
 *
 * @param rng seeded pseudo-random number generator for reproducibility
 * @param faker faker-js instance to create fake data if needed
 * @param schema the schema of the database we are pushing changes into
 * @param query the query that will be running for each change / query being fuzzed
 * @returns A list of changes to apply to the source database, in the form of [tableName, change].
 * Each change will be applied in order, with the query being run after each change
 */
export function createPushScript(
  rng: () => number,
  faker: Faker,
  schema: Schema,
  query: AST,
): [table: string, SourceChange][] {
  const changes: [string, SourceChange][] = [];

  // Extract all tables involved in the query
  const tables = extractTables(query, schema);

  const ctx: ChangeContext = {
    rng,
    faker,
    schema,
    query,
    tables,
    generatedRows: new Map(),
    pkCounter: 1, // Start from 1 for readable PKs
  };

  // Initialize generatedRows map for all tables
  for (const tableName of tables.keys()) {
    ctx.generatedRows.set(tableName, []);
  }

  // Generate changes based on query features
  generateInitialData(ctx, changes);
  generateLimitChanges(ctx, ctx.query, changes);
  generateFilterChanges(ctx, ctx.query, changes);
  generateRelationshipChanges(ctx, changes);
  generateStartChanges(ctx, ctx.query, changes);
  generateOrderByChanges(ctx, ctx.query, changes);
  generateRandomChanges(ctx, changes);

  return changes;
}
