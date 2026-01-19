# Phase 1: PostgreSQL Introspection

## Goal

Build the introspection layer that connects to PostgreSQL and extracts schema metadata into a structured intermediate representation (IR).

## Prerequisites

- Package scaffolding (package.json, tsconfig.json)
- postgres.js dependency for database connection

## Files to Create

```
packages/sql-to-zqlschema/
├── src/
│   ├── types.ts              # Shared type definitions (IR types)
│   ├── introspector.ts       # Main introspection logic
│   └── queries.ts            # SQL query definitions
├── test/
│   └── introspector.pg.test.ts
├── package.json
└── tsconfig.json
```

## Implementation Plan

### Step 1: Package Scaffolding

Create `package.json`:

```json
{
  "name": "@rocicorp/sql-to-zqlschema",
  "version": "0.0.1",
  "type": "module",
  "main": "./src/index.ts",
  "scripts": {
    "test": "vitest",
    "build": "tsc",
    "check-types": "tsc --noEmit",
    "lint": "oxlint",
    "format": "prettier --write ."
  },
  "dependencies": {
    "postgres": "^3.4.5"
  },
  "devDependencies": {
    "vitest": "^3.0.0",
    "typescript": "~5.8.2"
  }
}
```

Create `tsconfig.json` extending the monorepo base config.

### Step 2: Define IR Types (`src/types.ts`)

```typescript
/**
 * PostgreSQL type class from pg_type.typtype
 */
export type PgTypeClass = 'b' | 'c' | 'd' | 'e' | 'p' | 'r' | 'm';
// b=base, c=composite, d=domain, e=enum, p=pseudo, r=range, m=multirange

/**
 * Introspected column metadata
 */
export interface IntrospectedColumn {
  name: string;
  position: number;
  dataType: string; // SQL standard type (e.g., 'character varying')
  udtName: string; // PostgreSQL type name (e.g., 'varchar', 'int4')
  isNullable: boolean;
  characterMaxLength: number | null;
  numericPrecision: number | null;
  numericScale: number | null;
  defaultValue: string | null;
  pgTypeClass: PgTypeClass;
  isArray: boolean;
  arrayElementTypeClass: PgTypeClass | null;
}

/**
 * Introspected table metadata
 */
export interface IntrospectedTable {
  schema: string;
  name: string;
  columns: IntrospectedColumn[];
  primaryKey: string[]; // Column names in order
}

/**
 * Introspected enum type
 */
export interface IntrospectedEnum {
  schema: string;
  name: string;
  values: string[]; // Enum labels in sort order
}

/**
 * Introspected foreign key constraint
 */
export interface IntrospectedForeignKey {
  constraintName: string;
  sourceSchema: string;
  sourceTable: string;
  sourceColumns: string[]; // In constraint order
  targetSchema: string;
  targetTable: string;
  targetColumns: string[]; // In constraint order
  onDelete: 'NO ACTION' | 'RESTRICT' | 'CASCADE' | 'SET NULL' | 'SET DEFAULT';
  onUpdate: 'NO ACTION' | 'RESTRICT' | 'CASCADE' | 'SET NULL' | 'SET DEFAULT';
}

/**
 * Introspected unique constraint (for relationship inference)
 */
export interface IntrospectedUniqueConstraint {
  constraintName: string;
  schema: string;
  tableName: string;
  columns: string[];
  isPrimaryKey: boolean;
}

/**
 * Complete introspected schema
 */
export interface IntrospectedSchema {
  schemaName: string;
  tables: IntrospectedTable[];
  enums: IntrospectedEnum[];
  foreignKeys: IntrospectedForeignKey[];
  uniqueConstraints: IntrospectedUniqueConstraint[];
}

/**
 * Introspection options
 */
export interface IntrospectOptions {
  /** PostgreSQL connection string */
  connectionString?: string;
  /** Existing postgres.js client */
  client?: postgres.Sql;
  /** Schema to introspect (default: 'public') */
  schema?: string;
  /** Tables to include (default: all) */
  includeTables?: string[];
  /** Tables to exclude (default: none) */
  excludeTables?: string[];
}
```

### Step 3: SQL Queries (`src/queries.ts`)

```typescript
/**
 * Query to get all tables and columns in a schema
 */
export const COLUMNS_QUERY = `
SELECT
  c.table_schema,
  c.table_name,
  c.column_name,
  c.ordinal_position,
  c.data_type,
  c.udt_name,
  c.is_nullable,
  c.character_maximum_length,
  c.numeric_precision,
  c.numeric_scale,
  c.column_default,
  pt.typtype AS pg_type_class,
  pt.typelem != 0 AS is_array,
  elem_pt.typtype AS elem_type_class
FROM information_schema.columns c
JOIN pg_catalog.pg_type pt ON pt.typname = c.udt_name
JOIN pg_catalog.pg_namespace pn ON pn.oid = pt.typnamespace
LEFT JOIN pg_catalog.pg_type elem_pt ON elem_pt.oid = pt.typelem
WHERE c.table_schema = $1
  AND c.table_name NOT LIKE '\\_zero%' ESCAPE '\\'
  AND pn.nspname = c.udt_schema
ORDER BY c.table_name, c.ordinal_position
`;

/**
 * Query to get primary keys
 */
export const PRIMARY_KEYS_QUERY = `
SELECT
  tc.table_schema,
  tc.table_name,
  kcu.column_name,
  kcu.ordinal_position
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
  AND tc.table_name = kcu.table_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = $1
ORDER BY tc.table_name, kcu.ordinal_position
`;

/**
 * Query to get foreign keys
 */
export const FOREIGN_KEYS_QUERY = `
SELECT
  tc.constraint_name,
  tc.table_schema AS source_schema,
  tc.table_name AS source_table,
  kcu.column_name AS source_column,
  kcu.ordinal_position,
  ccu.table_schema AS target_schema,
  ccu.table_name AS target_table,
  ccu.column_name AS target_column,
  rc.delete_rule AS on_delete,
  rc.update_rule AS on_update
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
JOIN information_schema.referential_constraints rc
  ON tc.constraint_name = rc.constraint_name
  AND tc.table_schema = rc.constraint_schema
JOIN information_schema.constraint_column_usage ccu
  ON rc.unique_constraint_name = ccu.constraint_name
  AND rc.unique_constraint_schema = ccu.constraint_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = $1
ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
`;

/**
 * Query to get enum types and their values
 */
export const ENUMS_QUERY = `
SELECT
  n.nspname AS schema_name,
  t.typname AS enum_name,
  array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
FROM pg_catalog.pg_type t
JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid
JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
WHERE n.nspname = $1
GROUP BY n.nspname, t.typname
ORDER BY t.typname
`;

/**
 * Query to get unique constraints (for relationship inference)
 */
export const UNIQUE_CONSTRAINTS_QUERY = `
SELECT
  tc.constraint_name,
  tc.table_schema,
  tc.table_name,
  tc.constraint_type = 'PRIMARY KEY' AS is_primary_key,
  array_agg(kcu.column_name ORDER BY kcu.ordinal_position) AS columns
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
  AND tc.table_schema = $1
GROUP BY tc.constraint_name, tc.table_schema, tc.table_name, tc.constraint_type
ORDER BY tc.table_name, tc.constraint_name
`;
```

### Step 4: Introspector Implementation (`src/introspector.ts`)

```typescript
import postgres from 'postgres';
import type {
  IntrospectedSchema,
  IntrospectedTable,
  IntrospectedColumn,
  IntrospectedEnum,
  IntrospectedForeignKey,
  IntrospectedUniqueConstraint,
  IntrospectOptions,
  PgTypeClass,
} from './types.ts';
import {
  COLUMNS_QUERY,
  PRIMARY_KEYS_QUERY,
  FOREIGN_KEYS_QUERY,
  ENUMS_QUERY,
  UNIQUE_CONSTRAINTS_QUERY,
} from './queries.ts';

export async function introspect(
  options: IntrospectOptions,
): Promise<IntrospectedSchema> {
  const {
    connectionString,
    client,
    schema = 'public',
    includeTables,
    excludeTables = [],
  } = options;

  // Create or use provided client
  const sql = client ?? postgres(connectionString!);
  const shouldClose = !client;

  try {
    // Execute all queries in parallel
    const [
      columnsResult,
      primaryKeysResult,
      foreignKeysResult,
      enumsResult,
      uniqueResult,
    ] = await Promise.all([
      sql.unsafe(COLUMNS_QUERY, [schema]),
      sql.unsafe(PRIMARY_KEYS_QUERY, [schema]),
      sql.unsafe(FOREIGN_KEYS_QUERY, [schema]),
      sql.unsafe(ENUMS_QUERY, [schema]),
      sql.unsafe(UNIQUE_CONSTRAINTS_QUERY, [schema]),
    ]);

    // Process results into IR
    const tables = processColumns(
      columnsResult,
      primaryKeysResult,
      includeTables,
      excludeTables,
    );
    const enums = processEnums(enumsResult);
    const foreignKeys = processForeignKeys(
      foreignKeysResult,
      includeTables,
      excludeTables,
    );
    const uniqueConstraints = processUniqueConstraints(
      uniqueResult,
      includeTables,
      excludeTables,
    );

    return {
      schemaName: schema,
      tables,
      enums,
      foreignKeys,
      uniqueConstraints,
    };
  } finally {
    if (shouldClose) {
      await sql.end();
    }
  }
}

function processColumns(
  columnsResult: postgres.RowList<postgres.Row[]>,
  primaryKeysResult: postgres.RowList<postgres.Row[]>,
  includeTables: string[] | undefined,
  excludeTables: string[],
): IntrospectedTable[] {
  // Build primary key lookup: tableName -> columns[]
  const primaryKeyMap = new Map<string, string[]>();
  for (const row of primaryKeysResult) {
    const key = `${row.table_schema}.${row.table_name}`;
    if (!primaryKeyMap.has(key)) {
      primaryKeyMap.set(key, []);
    }
    primaryKeyMap.get(key)!.push(row.column_name);
  }

  // Group columns by table
  const tableMap = new Map<string, IntrospectedTable>();

  for (const row of columnsResult) {
    const tableName = row.table_name as string;

    // Apply include/exclude filters
    if (includeTables && !includeTables.includes(tableName)) continue;
    if (excludeTables.includes(tableName)) continue;

    const key = `${row.table_schema}.${tableName}`;

    if (!tableMap.has(key)) {
      tableMap.set(key, {
        schema: row.table_schema,
        name: tableName,
        columns: [],
        primaryKey: primaryKeyMap.get(key) ?? [],
      });
    }

    const column: IntrospectedColumn = {
      name: row.column_name,
      position: row.ordinal_position,
      dataType: row.data_type,
      udtName: row.udt_name,
      isNullable: row.is_nullable === 'YES',
      characterMaxLength: row.character_maximum_length,
      numericPrecision: row.numeric_precision,
      numericScale: row.numeric_scale,
      defaultValue: row.column_default,
      pgTypeClass: row.pg_type_class as PgTypeClass,
      isArray: row.is_array,
      arrayElementTypeClass: row.elem_type_class as PgTypeClass | null,
    };

    tableMap.get(key)!.columns.push(column);
  }

  return Array.from(tableMap.values());
}

function processEnums(
  enumsResult: postgres.RowList<postgres.Row[]>,
): IntrospectedEnum[] {
  return enumsResult.map(row => ({
    schema: row.schema_name,
    name: row.enum_name,
    values: row.enum_values,
  }));
}

function processForeignKeys(
  foreignKeysResult: postgres.RowList<postgres.Row[]>,
  includeTables: string[] | undefined,
  excludeTables: string[],
): IntrospectedForeignKey[] {
  // Group by constraint name
  const fkMap = new Map<string, IntrospectedForeignKey>();

  for (const row of foreignKeysResult) {
    const sourceTable = row.source_table as string;
    const targetTable = row.target_table as string;

    // Apply filters
    if (includeTables && !includeTables.includes(sourceTable)) continue;
    if (excludeTables.includes(sourceTable)) continue;
    if (includeTables && !includeTables.includes(targetTable)) continue;
    if (excludeTables.includes(targetTable)) continue;

    const key = `${row.source_schema}.${row.constraint_name}`;

    if (!fkMap.has(key)) {
      fkMap.set(key, {
        constraintName: row.constraint_name,
        sourceSchema: row.source_schema,
        sourceTable,
        sourceColumns: [],
        targetSchema: row.target_schema,
        targetTable,
        targetColumns: [],
        onDelete: row.on_delete,
        onUpdate: row.on_update,
      });
    }

    const fk = fkMap.get(key)!;
    fk.sourceColumns.push(row.source_column);
    fk.targetColumns.push(row.target_column);
  }

  return Array.from(fkMap.values());
}

function processUniqueConstraints(
  uniqueResult: postgres.RowList<postgres.Row[]>,
  includeTables: string[] | undefined,
  excludeTables: string[],
): IntrospectedUniqueConstraint[] {
  return uniqueResult
    .filter(row => {
      const tableName = row.table_name as string;
      if (includeTables && !includeTables.includes(tableName)) return false;
      if (excludeTables.includes(tableName)) return false;
      return true;
    })
    .map(row => ({
      constraintName: row.constraint_name,
      schema: row.table_schema,
      tableName: row.table_name,
      columns: row.columns,
      isPrimaryKey: row.is_primary_key,
    }));
}
```

### Step 5: Tests (`test/introspector.pg.test.ts`)

```typescript
import {describe, test, expect, beforeAll, afterAll} from 'vitest';
import postgres from 'postgres';
import {introspect} from '../src/introspector.ts';

describe('introspector', () => {
  let sql: postgres.Sql;
  const TEST_SCHEMA = 'sql_to_zql_test';

  beforeAll(async () => {
    sql = postgres(
      process.env.DATABASE_URL ?? 'postgresql://localhost:5432/postgres',
    );

    // Create test schema and tables
    await sql.unsafe(`
      DROP SCHEMA IF EXISTS ${TEST_SCHEMA} CASCADE;
      CREATE SCHEMA ${TEST_SCHEMA};

      -- Enum type
      CREATE TYPE ${TEST_SCHEMA}.user_role AS ENUM ('admin', 'user', 'guest');

      -- Users table
      CREATE TABLE ${TEST_SCHEMA}.users (
        id VARCHAR PRIMARY KEY,
        name VARCHAR(100),
        email TEXT NOT NULL,
        role ${TEST_SCHEMA}.user_role NOT NULL DEFAULT 'user',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        metadata JSONB
      );

      -- Projects table
      CREATE TABLE ${TEST_SCHEMA}.projects (
        id VARCHAR PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        owner_id VARCHAR NOT NULL REFERENCES ${TEST_SCHEMA}.users(id),
        is_active BOOLEAN DEFAULT true
      );

      -- Tasks table with composite FK
      CREATE TABLE ${TEST_SCHEMA}.tasks (
        id VARCHAR PRIMARY KEY,
        project_id VARCHAR NOT NULL,
        title VARCHAR(500) NOT NULL,
        assignee_id VARCHAR REFERENCES ${TEST_SCHEMA}.users(id) ON DELETE SET NULL,
        tags TEXT[],
        FOREIGN KEY (project_id) REFERENCES ${TEST_SCHEMA}.projects(id) ON DELETE CASCADE
      );

      -- Junction table with composite PK
      CREATE TABLE ${TEST_SCHEMA}.project_members (
        project_id VARCHAR NOT NULL REFERENCES ${TEST_SCHEMA}.projects(id),
        user_id VARCHAR NOT NULL REFERENCES ${TEST_SCHEMA}.users(id),
        role VARCHAR NOT NULL DEFAULT 'member',
        PRIMARY KEY (project_id, user_id)
      );
    `);
  });

  afterAll(async () => {
    await sql.unsafe(`DROP SCHEMA IF EXISTS ${TEST_SCHEMA} CASCADE`);
    await sql.end();
  });

  test('introspects tables and columns', async () => {
    const result = await introspect({client: sql, schema: TEST_SCHEMA});

    expect(result.tables).toHaveLength(4);

    const usersTable = result.tables.find(t => t.name === 'users');
    expect(usersTable).toBeDefined();
    expect(usersTable!.columns).toHaveLength(6);
    expect(usersTable!.primaryKey).toEqual(['id']);

    const idCol = usersTable!.columns.find(c => c.name === 'id');
    expect(idCol).toMatchObject({
      dataType: 'character varying',
      isNullable: false,
    });

    const nameCol = usersTable!.columns.find(c => c.name === 'name');
    expect(nameCol).toMatchObject({
      dataType: 'character varying',
      isNullable: true,
      characterMaxLength: 100,
    });
  });

  test('introspects composite primary keys', async () => {
    const result = await introspect({client: sql, schema: TEST_SCHEMA});

    const membersTable = result.tables.find(t => t.name === 'project_members');
    expect(membersTable!.primaryKey).toEqual(['project_id', 'user_id']);
  });

  test('introspects enum types', async () => {
    const result = await introspect({client: sql, schema: TEST_SCHEMA});

    expect(result.enums).toHaveLength(1);
    expect(result.enums[0]).toMatchObject({
      name: 'user_role',
      values: ['admin', 'user', 'guest'],
    });

    // Check column references enum
    const usersTable = result.tables.find(t => t.name === 'users');
    const roleCol = usersTable!.columns.find(c => c.name === 'role');
    expect(roleCol!.pgTypeClass).toBe('e');
    expect(roleCol!.udtName).toBe('user_role');
  });

  test('introspects foreign keys', async () => {
    const result = await introspect({client: sql, schema: TEST_SCHEMA});

    expect(result.foreignKeys.length).toBeGreaterThanOrEqual(4);

    // Project -> User FK
    const projectOwnerFK = result.foreignKeys.find(
      fk =>
        fk.sourceTable === 'projects' && fk.sourceColumns.includes('owner_id'),
    );
    expect(projectOwnerFK).toMatchObject({
      sourceTable: 'projects',
      sourceColumns: ['owner_id'],
      targetTable: 'users',
      targetColumns: ['id'],
    });

    // Task -> Project FK with CASCADE
    const taskProjectFK = result.foreignKeys.find(
      fk =>
        fk.sourceTable === 'tasks' && fk.sourceColumns.includes('project_id'),
    );
    expect(taskProjectFK).toMatchObject({
      onDelete: 'CASCADE',
    });
  });

  test('introspects array columns', async () => {
    const result = await introspect({client: sql, schema: TEST_SCHEMA});

    const tasksTable = result.tables.find(t => t.name === 'tasks');
    const tagsCol = tasksTable!.columns.find(c => c.name === 'tags');

    expect(tagsCol!.isArray).toBe(true);
    expect(tagsCol!.udtName).toBe('_text'); // PostgreSQL array naming
  });

  test('introspects JSON columns', async () => {
    const result = await introspect({client: sql, schema: TEST_SCHEMA});

    const usersTable = result.tables.find(t => t.name === 'users');
    const metadataCol = usersTable!.columns.find(c => c.name === 'metadata');

    expect(metadataCol!.dataType).toBe('jsonb');
    expect(metadataCol!.pgTypeClass).toBe('b');
  });

  test('filters tables with includeTables', async () => {
    const result = await introspect({
      client: sql,
      schema: TEST_SCHEMA,
      includeTables: ['users', 'projects'],
    });

    expect(result.tables).toHaveLength(2);
    expect(result.tables.map(t => t.name).sort()).toEqual([
      'projects',
      'users',
    ]);
  });

  test('filters tables with excludeTables', async () => {
    const result = await introspect({
      client: sql,
      schema: TEST_SCHEMA,
      excludeTables: ['project_members'],
    });

    expect(result.tables).toHaveLength(3);
    expect(
      result.tables.find(t => t.name === 'project_members'),
    ).toBeUndefined();
  });

  test('introspects unique constraints', async () => {
    const result = await introspect({client: sql, schema: TEST_SCHEMA});

    // Should include primary keys as unique constraints
    const usersPK = result.uniqueConstraints.find(
      uc => uc.tableName === 'users' && uc.isPrimaryKey,
    );
    expect(usersPK).toBeDefined();
    expect(usersPK!.columns).toEqual(['id']);

    // Composite PK
    const membersPK = result.uniqueConstraints.find(
      uc => uc.tableName === 'project_members' && uc.isPrimaryKey,
    );
    expect(membersPK).toBeDefined();
    expect(membersPK!.columns).toEqual(['project_id', 'user_id']);
  });
});
```

## Acceptance Criteria

1. **Tables**: Correctly extracts all tables in the specified schema
2. **Columns**: Extracts name, type, nullability, max length, default, position
3. **Primary Keys**: Handles single and composite primary keys
4. **Foreign Keys**: Extracts source/target tables and columns, ON DELETE/UPDATE actions
5. **Enums**: Discovers custom enum types and their values
6. **Arrays**: Detects array columns and element types
7. **Filtering**: includeTables and excludeTables work correctly
8. **Connection**: Works with both connection string and existing client
9. **Tests**: All PostgreSQL integration tests pass

## Dependencies

- `postgres` - PostgreSQL client
- `vitest` - Testing framework

## Notes

- Tests require a running PostgreSQL instance (use `npm run db-up` in zbugs or configure DATABASE_URL)
- The `.pg.test.ts` suffix enables the PostgreSQL vitest config
- Excludes `_zero*` tables by default (Zero internal tables)
