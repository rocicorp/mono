# sql-to-zqlschema Package Design

## Overview

A package that introspects a PostgreSQL database and generates Zero schema TypeScript code. This removes the burden of manually maintaining Zero schemas that mirror existing database schemas.

## Goals

1. Accept a PostgreSQL connection string and schema name
2. Introspect the database to extract table structure, columns, types, constraints, and foreign keys
3. Emit valid Zero schema TypeScript code using the builder API

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           sql-to-zqlschema                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐     ┌─────────────────┐     ┌──────────────────────────┐   │
│  │  PostgreSQL │────▶│  Introspector   │────▶│  IR (Intermediate Rep)   │   │
│  │  Connection │     │                 │     │                          │   │
│  └─────────────┘     └─────────────────┘     └──────────────────────────┘   │
│                                                        │                     │
│                                                        ▼                     │
│                      ┌─────────────────┐     ┌──────────────────────────┐   │
│                      │  TypeScript     │◀────│  Code Generator          │   │
│                      │  Schema File    │     │                          │   │
│                      └─────────────────┘     └──────────────────────────┘   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Introspector (`src/introspector.ts`)

Queries PostgreSQL system catalogs to extract schema metadata.

#### Tables and Columns Query

```sql
SELECT
  c.table_schema,
  c.table_name,
  c.column_name,
  c.ordinal_position,
  c.data_type,
  c.udt_name,
  c.is_nullable,
  c.character_maximum_length,
  c.column_default,
  pt.typtype as pg_type_class,
  elem_pt.typtype as elem_type_class
FROM information_schema.columns c
JOIN pg_catalog.pg_type pt ON pt.typname = c.udt_name
LEFT JOIN pg_catalog.pg_type elem_pt ON elem_pt.oid = pt.typelem
WHERE c.table_schema = $1
  AND c.table_name NOT LIKE '_zero%'
ORDER BY c.table_name, c.ordinal_position;
```

#### Primary Keys Query

```sql
SELECT
  tc.table_schema,
  tc.table_name,
  kcu.column_name,
  kcu.ordinal_position
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = $1
ORDER BY tc.table_name, kcu.ordinal_position;
```

#### Foreign Keys Query

```sql
SELECT
  tc.table_schema,
  tc.table_name,
  kcu.column_name,
  ccu.table_schema AS foreign_table_schema,
  ccu.table_name AS foreign_table_name,
  ccu.column_name AS foreign_column_name,
  tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = $1
ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position;
```

#### Enum Types Query

```sql
SELECT
  t.typname as enum_name,
  n.nspname as schema_name,
  array_agg(e.enumlabel ORDER BY e.enumsortorder) as values
FROM pg_type t
JOIN pg_enum e ON t.oid = e.enumtypid
JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE n.nspname = $1
GROUP BY t.typname, n.nspname;
```

#### Output: Intermediate Representation (IR)

```typescript
interface IntrospectedSchema {
  tables: IntrospectedTable[];
  enums: IntrospectedEnum[];
  foreignKeys: IntrospectedForeignKey[];
}

interface IntrospectedTable {
  schema: string;
  name: string;
  columns: IntrospectedColumn[];
  primaryKey: string[];
}

interface IntrospectedColumn {
  name: string;
  dataType: string;       // e.g., 'varchar', 'integer', 'timestamp'
  udtName: string;        // e.g., 'varchar', 'int4', 'user_role'
  isNullable: boolean;
  maxLength: number | null;
  defaultValue: string | null;
  pgTypeClass: 'b' | 'e' | 'c' | 'd' | 'p' | 'r' | 'm';  // base, enum, composite, etc.
  isArray: boolean;
  arrayElementTypeClass: string | null;
}

interface IntrospectedEnum {
  schema: string;
  name: string;
  values: string[];
}

interface IntrospectedForeignKey {
  constraintName: string;
  sourceTable: string;
  sourceColumns: string[];
  targetTable: string;
  targetColumns: string[];
}
```

### 2. Type Mapper (`src/type-mapper.ts`)

Maps PostgreSQL types to Zero column types. Based on existing mapping in `zero-cache/src/types/pg-data-type.ts`.

```typescript
type ZeroColumnType = 'string' | 'number' | 'boolean' | 'json';

const pgToZeroTypeMap: Record<string, ZeroColumnType> = {
  // Numeric
  'smallint': 'number',
  'integer': 'number',
  'int': 'number',
  'int2': 'number',
  'int4': 'number',
  'int8': 'number',
  'bigint': 'number',
  'serial': 'number',
  'bigserial': 'number',
  'decimal': 'number',
  'numeric': 'number',
  'real': 'number',
  'double precision': 'number',
  'float': 'number',
  'float4': 'number',
  'float8': 'number',

  // Date/Time (stored as numbers in Zero)
  'date': 'number',
  'time': 'number',
  'timestamp': 'number',
  'timestamptz': 'number',
  'timestamp with time zone': 'number',
  'timestamp without time zone': 'number',

  // String
  'varchar': 'string',
  'character varying': 'string',
  'char': 'string',
  'character': 'string',
  'bpchar': 'string',
  'text': 'string',
  'uuid': 'string',

  // Boolean
  'bool': 'boolean',
  'boolean': 'boolean',

  // JSON
  'json': 'json',
  'jsonb': 'json',
};

function mapColumnType(column: IntrospectedColumn): ZeroColumnType {
  // Arrays become JSON
  if (column.isArray) {
    return 'json';
  }

  // Enums become strings
  if (column.pgTypeClass === 'e') {
    return 'string';
  }

  // Look up type
  const normalized = column.dataType.toLowerCase();
  return pgToZeroTypeMap[normalized] ?? 'string';
}
```

### 3. Relationship Inferrer (`src/relationship-inferrer.ts`)

Converts foreign keys to Zero relationships. The key challenge is determining cardinality (`one` vs `many`).

**Heuristics for cardinality:**

1. **Foreign key on single column → `one`** (many-to-one from source perspective)
2. **Foreign key includes primary key columns → potentially `one`** (one-to-one)
3. **Junction table pattern** (table with only FKs as PK) → generate `many` through junction

```typescript
interface ZeroRelationship {
  sourceName: string;       // Variable name in relationships definition
  relationshipName: string; // Property name in relationships object
  cardinality: 'one' | 'many';
  sourceTable: string;
  sourceFields: string[];
  destTable: string;
  destFields: string[];

  // For junction tables (many-to-many)
  junction?: {
    table: string;
    sourceField: string[];
    destField: string[];
    finalDestTable: string;
    finalDestField: string[];
  };
}

function inferRelationships(
  tables: IntrospectedTable[],
  foreignKeys: IntrospectedForeignKey[]
): ZeroRelationship[] {
  const relationships: ZeroRelationship[] = [];

  for (const fk of foreignKeys) {
    // Infer relationship name from target table (singular)
    const relationshipName = inferRelationshipName(fk);

    relationships.push({
      sourceName: `${fk.sourceTable}Relationships`,
      relationshipName,
      cardinality: 'one', // FK holder is the "many" side, so relation is "one"
      sourceTable: fk.sourceTable,
      sourceFields: fk.sourceColumns,
      destTable: fk.targetTable,
      destFields: fk.targetColumns,
    });
  }

  // Detect junction tables and generate many-to-many relationships
  const junctionTables = detectJunctionTables(tables, foreignKeys);
  // ... handle junction tables

  return relationships;
}
```

### 4. Code Generator (`src/code-generator.ts`)

Generates TypeScript code using the Zero schema builder API.

```typescript
interface GeneratorOptions {
  includeRelationships: boolean;
  enumStyle: 'enumeration' | 'string-literal';
  schemaName: string;        // For .from() mapping if different from 'public'
  exportName: string;        // Name for exported schema constant
  includeTypes: boolean;     // Generate TypeScript type exports
}

function generateSchema(
  ir: IntrospectedSchema,
  options: GeneratorOptions
): string {
  const imports = generateImports(ir, options);
  const enumTypes = generateEnumTypes(ir.enums, options);
  const tables = generateTables(ir.tables, ir.enums, options);
  const relationships = generateRelationships(ir, options);
  const schema = generateSchemaExport(ir.tables, relationships, options);

  return [imports, enumTypes, tables, relationships, schema].join('\n\n');
}
```

#### Example Output

Given a PostgreSQL schema like zbugs, the generator would produce:

```typescript
import {
  boolean,
  createBuilder,
  createSchema,
  enumeration,
  number,
  relationships,
  string,
  table,
} from '@rocicorp/zero';

// Enum types (generated as TypeScript types for use with enumeration<T>())
type Role = 'user' | 'admin' | 'crew';
type Visibility = 'internal' | 'public';

// Table definitions
const user = table('user')
  .columns({
    id: string(),
    login: string(),
    name: string().optional(),
    avatar: string().optional(),
    role: enumeration<Role>(),
  })
  .primaryKey('id');

const project = table('project')
  .columns({
    id: string(),
    name: string(),
    lowerCaseName: string(),
    issueCountEstimate: number().optional(),
    supportsSearch: boolean(),
    markURL: string().optional(),
    logoURL: string().optional(),
  })
  .primaryKey('id');

const issue = table('issue')
  .columns({
    id: string(),
    shortID: number().optional(),
    title: string(),
    open: boolean(),
    modified: number().optional(),
    created: number().optional(),
    projectID: string(),
    creatorID: string(),
    assigneeID: string().optional(),
    description: string().optional(),
    visibility: enumeration<Visibility>(),
  })
  .primaryKey('id');

// ... more tables

// Relationships (inferred from foreign keys)
const issueRelationships = relationships(issue, ({one}) => ({
  project: one({
    sourceField: ['projectID'],
    destField: ['id'],
    destSchema: project,
  }),
  creator: one({
    sourceField: ['creatorID'],
    destField: ['id'],
    destSchema: user,
  }),
  assignee: one({
    sourceField: ['assigneeID'],
    destField: ['id'],
    destSchema: user,
  }),
}));

// ... more relationships

export const schema = createSchema({
  tables: [user, project, issue, comment, label, issueLabel, viewState, emoji, userPref, issueNotifications],
  relationships: [issueRelationships, commentRelationships, /* ... */],
});

export const builder = createBuilder(schema);
```

### 5. CLI (`src/cli.ts`)

Command-line interface for the tool.

```bash
npx sql-to-zqlschema \
  --connection "postgresql://user:pass@localhost:5432/mydb" \
  --schema public \
  --output ./src/schema.ts \
  --relationships \
  --enum-style enumeration
```

#### CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--connection`, `-c` | PostgreSQL connection string | Required |
| `--schema`, `-s` | Database schema to introspect | `public` |
| `--output`, `-o` | Output file path | `stdout` |
| `--relationships`, `-r` | Include relationship definitions | `true` |
| `--enum-style` | How to handle enums: `enumeration` or `string` | `enumeration` |
| `--export-name` | Name for exported schema constant | `schema` |
| `--include-types` | Export TypeScript types for tables | `false` |
| `--tables` | Comma-separated list of tables to include | All tables |
| `--exclude` | Comma-separated list of tables to exclude | None |
| `--dry-run` | Print output without writing file | `false` |

### 6. Programmatic API (`src/index.ts`)

```typescript
import {introspect, generate, type GeneratorOptions} from 'sql-to-zqlschema';
import postgres from 'postgres';

// Using connection string
const schema = await introspect({
  connectionString: 'postgresql://user:pass@localhost:5432/mydb',
  schema: 'public',
});

// Or using existing postgres.js client
const sql = postgres('postgresql://...');
const schema = await introspect({
  client: sql,
  schema: 'public',
});

// Generate code
const code = generate(schema, {
  includeRelationships: true,
  enumStyle: 'enumeration',
  exportName: 'schema',
});

console.log(code);
```

## File Structure

```
packages/sql-to-zqlschema/
├── src/
│   ├── index.ts              # Public API exports
│   ├── cli.ts                # CLI entry point
│   ├── introspector.ts       # PostgreSQL introspection
│   ├── type-mapper.ts        # PG → Zero type mapping
│   ├── relationship-inferrer.ts # FK → relationships
│   ├── code-generator.ts     # TypeScript code generation
│   └── types.ts              # Shared type definitions
├── test/
│   ├── introspector.test.ts
│   ├── type-mapper.test.ts
│   ├── relationship-inferrer.test.ts
│   ├── code-generator.test.ts
│   └── integration.pg.test.ts
├── package.json
├── tsconfig.json
└── README.md
```

## Dependencies

```json
{
  "dependencies": {
    "postgres": "^3.4.0",
    "commander": "^12.0.0"
  },
  "devDependencies": {
    "typescript": "^5.0.0",
    "vitest": "^3.0.0"
  },
  "peerDependencies": {
    "@rocicorp/zero": "^0.x"
  }
}
```

## Edge Cases and Limitations

### Handled Edge Cases

1. **Composite primary keys** - Fully supported via `.primaryKey('col1', 'col2')`
2. **Composite foreign keys** - Supported with multi-column sourceField/destField arrays
3. **Self-referential foreign keys** - Handled normally (e.g., `parent_id` referencing same table)
4. **Nullable columns** - Mapped to `.optional()`
5. **Array columns** - Mapped to `json()` type
6. **Custom enum types** - Detected via `pg_type.typtype = 'e'` and generated as TypeScript types
7. **Schema-qualified names** - Tables can use `.from('schema.table')` when schema !== 'public'

### Known Limitations

1. **Relationship naming** - Inferred from target table name; may need manual adjustment for multiple FKs to same table
2. **Many-to-many relationships** - Requires heuristics to detect junction tables
3. **Inverse relationships** - Only forward relationships (FK holder → target) are auto-generated; inverse relationships require manual addition
4. **Views** - Not supported; only tables are introspected
5. **Computed/generated columns** - Excluded from schema (consistent with Zero behavior)
6. **Domain types** - Mapped based on base type
7. **Check constraints** - Not represented in Zero schema
8. **Default values** - Not represented in Zero schema (DB-side only)

### Unsupported PostgreSQL Types

The following types will emit a warning and default to `string()`:
- `bytea` (binary data)
- `interval`
- `point`, `line`, `polygon`, `path`, `box`, `circle` (geometric)
- `inet`, `cidr`, `macaddr` (network)
- `tsvector`, `tsquery` (full-text search)
- `xml`
- Range types (other than arrays)

## Configuration File Support

Optional `sql-to-zqlschema.config.ts` for project-level defaults:

```typescript
import type {Config} from 'sql-to-zqlschema';

export default {
  connection: process.env.DATABASE_URL,
  schema: 'public',
  output: './src/schema.ts',
  relationships: true,
  enumStyle: 'enumeration',

  // Custom relationship naming
  relationshipNames: {
    'issue.creatorID': 'creator',
    'issue.assigneeID': 'assignee',
  },

  // Custom type overrides
  typeOverrides: {
    'user.role': 'enumeration<Role>',
  },

  // Tables to exclude
  exclude: ['_migrations', '_zero*'],
} satisfies Config;
```

## Future Enhancements

1. **Watch mode** - Re-generate on database schema changes
2. **Diff mode** - Show changes between current schema and generated
3. **Migration integration** - Hook into Drizzle/Prisma migrations
4. **Schema validation** - Validate existing Zero schema against database
5. **Incremental generation** - Update specific tables without full regeneration
6. **Custom templates** - Allow customization of generated code format
