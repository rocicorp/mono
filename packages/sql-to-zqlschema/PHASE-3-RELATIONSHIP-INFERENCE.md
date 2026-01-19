# Phase 3: Relationship Inference

## Goal

Build the relationship inference engine that converts foreign key constraints into Zero relationship definitions. This is the most nuanced phase as it requires heuristics to determine relationship naming and cardinality.

## Prerequisites

- Phase 1 complete (introspection types with foreign keys)
- Phase 2 complete (mapped tables available)

## Files to Create/Modify

```
packages/sql-to-zqlschema/
├── src/
│   ├── relationship-inferrer.ts  # Relationship inference logic
│   └── types.ts                  # Add relationship types
├── test/
│   └── relationship-inferrer.test.ts
```

## Implementation Plan

### Step 1: Extend Types (`src/types.ts`)

```typescript
/**
 * A single relationship definition for Zero schema
 */
export interface InferredRelationship {
  /** Source table name */
  sourceTable: string;
  /** Relationship property name (e.g., 'creator', 'project') */
  name: string;
  /** Cardinality: 'one' means FK holder -> target, 'many' means inverse */
  cardinality: 'one' | 'many';
  /** Source columns in the relationship */
  sourceFields: string[];
  /** Destination table name */
  destTable: string;
  /** Destination columns */
  destFields: string[];
  /** For junction table relationships (many-to-many) */
  through?: {
    junctionTable: string;
    junctionSourceFields: string[];
    junctionDestFields: string[];
  };
}

/**
 * Grouped relationships by source table for code generation
 */
export interface TableRelationships {
  sourceTable: string;
  relationships: InferredRelationship[];
}

/**
 * Relationship inference options
 */
export interface RelationshipInferrerOptions {
  /** Custom relationship names: 'table.column' -> 'relationshipName' */
  relationshipNames?: Record<string, string>;
  /** Generate inverse (many) relationships (default: false) */
  generateInverse?: boolean;
  /** Detect and generate many-to-many through junction tables (default: true) */
  detectManyToMany?: boolean;
}
```

### Step 2: Relationship Inferrer (`src/relationship-inferrer.ts`)

```typescript
import type {
  IntrospectedSchema,
  IntrospectedForeignKey,
  IntrospectedTable,
  IntrospectedUniqueConstraint,
  InferredRelationship,
  TableRelationships,
  RelationshipInferrerOptions,
} from './types.ts';

/**
 * Convert a table or column name to a relationship name
 * e.g., 'users' -> 'user', 'creatorID' -> 'creator'
 */
function toRelationshipName(name: string): string {
  // Remove common suffixes
  let result = name.replace(/ID$/i, '').replace(/_id$/i, '').replace(/Id$/, '');

  // Simple singularization for common patterns
  if (result.endsWith('ies')) {
    result = result.slice(0, -3) + 'y';
  } else if (
    result.endsWith('ses') ||
    result.endsWith('xes') ||
    result.endsWith('zes')
  ) {
    result = result.slice(0, -2);
  } else if (result.endsWith('s') && !result.endsWith('ss')) {
    result = result.slice(0, -1);
  }

  // Convert to camelCase if needed
  result = result.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());

  // Lowercase first letter
  return result.charAt(0).toLowerCase() + result.slice(1);
}

/**
 * Generate a unique relationship name for a foreign key
 */
function generateRelationshipName(
  fk: IntrospectedForeignKey,
  existingNames: Set<string>,
  customNames: Record<string, string>,
): string {
  // Check for custom override
  const overrideKey = `${fk.sourceTable}.${fk.sourceColumns[0]}`;
  if (customNames[overrideKey]) {
    return customNames[overrideKey];
  }

  // For single-column FKs, derive from column name
  if (fk.sourceColumns.length === 1) {
    const baseName = toRelationshipName(fk.sourceColumns[0]);

    // If name conflicts, append target table
    if (existingNames.has(baseName)) {
      return `${baseName}${capitalize(fk.targetTable)}`;
    }
    return baseName;
  }

  // For multi-column FKs, use target table name
  const baseName = toRelationshipName(fk.targetTable);

  if (existingNames.has(baseName)) {
    // Append a suffix based on source columns
    const suffix = fk.sourceColumns
      .map(c => capitalize(toRelationshipName(c)))
      .join('');
    return `${baseName}By${suffix}`;
  }

  return baseName;
}

function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Detect if a table is a junction table (for many-to-many relationships)
 *
 * Heuristics:
 * 1. Table has exactly 2 foreign keys
 * 2. Primary key consists of the FK columns
 * 3. Table has few or no additional columns (besides FKs and audit columns)
 */
function isJunctionTable(
  table: IntrospectedTable,
  tableFKs: IntrospectedForeignKey[],
): boolean {
  if (tableFKs.length !== 2) {
    return false;
  }

  // Get all FK columns
  const fkColumns = new Set(tableFKs.flatMap(fk => fk.sourceColumns));

  // Check if PK is composed of FK columns
  const pkColumns = new Set(table.primaryKey);
  if (pkColumns.size !== fkColumns.size) {
    return false;
  }

  for (const col of pkColumns) {
    if (!fkColumns.has(col)) {
      return false;
    }
  }

  // Optional: check that non-PK columns are minimal
  // Allow audit columns like created_at, updated_at, or a single 'role' column
  const nonPKColumns = table.columns.filter(c => !pkColumns.has(c.name));
  const allowedExtraColumns = [
    'created_at',
    'updated_at',
    'created',
    'modified',
    'role',
    'type',
  ];

  const unexpectedColumns = nonPKColumns.filter(
    c => !allowedExtraColumns.includes(c.name.toLowerCase()),
  );

  return unexpectedColumns.length <= 1;
}

/**
 * Infer relationships from foreign keys
 */
export function inferRelationships(
  schema: IntrospectedSchema,
  options: RelationshipInferrerOptions = {},
): TableRelationships[] {
  const {
    relationshipNames = {},
    generateInverse = false,
    detectManyToMany = true,
  } = options;

  // Build lookup maps
  const tableMap = new Map(schema.tables.map(t => [t.name, t]));
  const tableFKsMap = new Map<string, IntrospectedForeignKey[]>();

  for (const fk of schema.foreignKeys) {
    if (!tableFKsMap.has(fk.sourceTable)) {
      tableFKsMap.set(fk.sourceTable, []);
    }
    tableFKsMap.get(fk.sourceTable)!.push(fk);
  }

  // Detect junction tables
  const junctionTables = new Set<string>();
  if (detectManyToMany) {
    for (const table of schema.tables) {
      const fks = tableFKsMap.get(table.name) ?? [];
      if (isJunctionTable(table, fks)) {
        junctionTables.add(table.name);
      }
    }
  }

  // Group relationships by source table
  const relationshipsByTable = new Map<string, InferredRelationship[]>();

  // Process each foreign key
  for (const fk of schema.foreignKeys) {
    // Skip FKs from junction tables (handled separately for many-to-many)
    if (junctionTables.has(fk.sourceTable)) {
      continue;
    }

    const existingNames = new Set(
      (relationshipsByTable.get(fk.sourceTable) ?? []).map(r => r.name),
    );

    const name = generateRelationshipName(fk, existingNames, relationshipNames);

    const relationship: InferredRelationship = {
      sourceTable: fk.sourceTable,
      name,
      cardinality: 'one',
      sourceFields: fk.sourceColumns,
      destTable: fk.targetTable,
      destFields: fk.targetColumns,
    };

    if (!relationshipsByTable.has(fk.sourceTable)) {
      relationshipsByTable.set(fk.sourceTable, []);
    }
    relationshipsByTable.get(fk.sourceTable)!.push(relationship);
  }

  // Generate many-to-many relationships through junction tables
  if (detectManyToMany) {
    for (const junctionTableName of junctionTables) {
      const junctionFKs = tableFKsMap.get(junctionTableName) ?? [];
      if (junctionFKs.length !== 2) continue;

      const [fk1, fk2] = junctionFKs;

      // Create relationship from table1 -> table2 through junction
      addManyToManyRelationship(
        relationshipsByTable,
        fk1.targetTable,
        fk2.targetTable,
        junctionTableName,
        fk1,
        fk2,
        relationshipNames,
      );

      // Create inverse relationship from table2 -> table1 through junction
      addManyToManyRelationship(
        relationshipsByTable,
        fk2.targetTable,
        fk1.targetTable,
        junctionTableName,
        fk2,
        fk1,
        relationshipNames,
      );
    }
  }

  // Generate inverse relationships if requested
  if (generateInverse) {
    for (const fk of schema.foreignKeys) {
      if (junctionTables.has(fk.sourceTable)) continue;

      const existingNames = new Set(
        (relationshipsByTable.get(fk.targetTable) ?? []).map(r => r.name),
      );

      // Name inverse relationship as plural of source table
      let inverseName = fk.sourceTable;
      if (!inverseName.endsWith('s')) {
        inverseName += 's';
      }

      // Handle conflicts
      if (existingNames.has(inverseName)) {
        inverseName = `${inverseName}By${capitalize(fk.sourceColumns[0])}`;
      }

      const inverseRelationship: InferredRelationship = {
        sourceTable: fk.targetTable,
        name: inverseName,
        cardinality: 'many',
        sourceFields: fk.targetColumns,
        destTable: fk.sourceTable,
        destFields: fk.sourceColumns,
      };

      if (!relationshipsByTable.has(fk.targetTable)) {
        relationshipsByTable.set(fk.targetTable, []);
      }
      relationshipsByTable.get(fk.targetTable)!.push(inverseRelationship);
    }
  }

  // Convert to array format
  return Array.from(relationshipsByTable.entries())
    .map(([sourceTable, relationships]) => ({
      sourceTable,
      relationships,
    }))
    .sort((a, b) => a.sourceTable.localeCompare(b.sourceTable));
}

function addManyToManyRelationship(
  relationshipsByTable: Map<string, InferredRelationship[]>,
  sourceTable: string,
  destTable: string,
  junctionTable: string,
  sourceFk: IntrospectedForeignKey,
  destFk: IntrospectedForeignKey,
  customNames: Record<string, string>,
): void {
  const existingNames = new Set(
    (relationshipsByTable.get(sourceTable) ?? []).map(r => r.name),
  );

  // Check for custom name
  const overrideKey = `${sourceTable}.${junctionTable}`;
  let name = customNames[overrideKey];

  if (!name) {
    // Use plural of destination table
    name = destTable;
    if (!name.endsWith('s')) {
      name += 's';
    }
  }

  // Handle conflicts
  if (existingNames.has(name)) {
    name = `${name}Via${capitalize(junctionTable)}`;
  }

  const relationship: InferredRelationship = {
    sourceTable,
    name,
    cardinality: 'many',
    sourceFields: sourceFk.targetColumns,
    destTable: destTable,
    destFields: destFk.targetColumns,
    through: {
      junctionTable,
      junctionSourceFields: sourceFk.sourceColumns,
      junctionDestFields: destFk.sourceColumns,
    },
  };

  if (!relationshipsByTable.has(sourceTable)) {
    relationshipsByTable.set(sourceTable, []);
  }
  relationshipsByTable.get(sourceTable)!.push(relationship);
}

/**
 * Check if a foreign key represents a one-to-one relationship
 * (i.e., the FK columns are also a unique constraint)
 */
export function isOneToOneRelationship(
  fk: IntrospectedForeignKey,
  uniqueConstraints: IntrospectedUniqueConstraint[],
): boolean {
  return uniqueConstraints.some(uc => {
    if (uc.tableName !== fk.sourceTable) return false;
    if (uc.columns.length !== fk.sourceColumns.length) return false;
    return fk.sourceColumns.every(col => uc.columns.includes(col));
  });
}
```

### Step 3: Tests (`test/relationship-inferrer.test.ts`)

```typescript
import {describe, test, expect} from 'vitest';
import {
  inferRelationships,
  isOneToOneRelationship,
} from '../src/relationship-inferrer.ts';
import type {IntrospectedSchema, IntrospectedForeignKey} from '../src/types.ts';

const makeSchema = (
  tables: Array<{name: string; columns: string[]; primaryKey: string[]}>,
  foreignKeys: Array<{
    sourceTable: string;
    sourceColumns: string[];
    targetTable: string;
    targetColumns: string[];
  }>,
): IntrospectedSchema => ({
  schemaName: 'public',
  tables: tables.map(t => ({
    schema: 'public',
    name: t.name,
    columns: t.columns.map((c, i) => ({
      name: c,
      position: i + 1,
      dataType: 'text',
      udtName: 'text',
      isNullable: !t.primaryKey.includes(c),
      characterMaxLength: null,
      numericPrecision: null,
      numericScale: null,
      defaultValue: null,
      pgTypeClass: 'b' as const,
      isArray: false,
      arrayElementTypeClass: null,
    })),
    primaryKey: t.primaryKey,
  })),
  enums: [],
  foreignKeys: foreignKeys.map(fk => ({
    constraintName: `${fk.sourceTable}_${fk.sourceColumns.join('_')}_fkey`,
    sourceSchema: 'public',
    sourceTable: fk.sourceTable,
    sourceColumns: fk.sourceColumns,
    targetSchema: 'public',
    targetTable: fk.targetTable,
    targetColumns: fk.targetColumns,
    onDelete: 'NO ACTION' as const,
    onUpdate: 'NO ACTION' as const,
  })),
  uniqueConstraints: tables.map(t => ({
    constraintName: `${t.name}_pkey`,
    schema: 'public',
    tableName: t.name,
    columns: t.primaryKey,
    isPrimaryKey: true,
  })),
});

describe('inferRelationships', () => {
  describe('basic foreign keys', () => {
    test('infers one relationship from FK', () => {
      const schema = makeSchema(
        [
          {name: 'users', columns: ['id', 'name'], primaryKey: ['id']},
          {
            name: 'posts',
            columns: ['id', 'title', 'authorID'],
            primaryKey: ['id'],
          },
        ],
        [
          {
            sourceTable: 'posts',
            sourceColumns: ['authorID'],
            targetTable: 'users',
            targetColumns: ['id'],
          },
        ],
      );

      const result = inferRelationships(schema);

      expect(result).toHaveLength(1);
      expect(result[0].sourceTable).toBe('posts');
      expect(result[0].relationships).toHaveLength(1);
      expect(result[0].relationships[0]).toMatchObject({
        name: 'author',
        cardinality: 'one',
        sourceFields: ['authorID'],
        destTable: 'users',
        destFields: ['id'],
      });
    });

    test('handles multiple FKs to same table', () => {
      const schema = makeSchema(
        [
          {name: 'users', columns: ['id', 'name'], primaryKey: ['id']},
          {
            name: 'posts',
            columns: ['id', 'creatorID', 'editorID'],
            primaryKey: ['id'],
          },
        ],
        [
          {
            sourceTable: 'posts',
            sourceColumns: ['creatorID'],
            targetTable: 'users',
            targetColumns: ['id'],
          },
          {
            sourceTable: 'posts',
            sourceColumns: ['editorID'],
            targetTable: 'users',
            targetColumns: ['id'],
          },
        ],
      );

      const result = inferRelationships(schema);

      expect(result).toHaveLength(1);
      const postRels = result[0].relationships;
      expect(postRels).toHaveLength(2);

      const names = postRels.map(r => r.name);
      expect(names).toContain('creator');
      expect(names).toContain('editor');
    });

    test('infers relationship name from target table for composite FK', () => {
      const schema = makeSchema(
        [
          {
            name: 'projects',
            columns: ['id', 'orgID'],
            primaryKey: ['id', 'orgID'],
          },
          {
            name: 'tasks',
            columns: ['id', 'projectID', 'projectOrgID'],
            primaryKey: ['id'],
          },
        ],
        [
          {
            sourceTable: 'tasks',
            sourceColumns: ['projectID', 'projectOrgID'],
            targetTable: 'projects',
            targetColumns: ['id', 'orgID'],
          },
        ],
      );

      const result = inferRelationships(schema);

      expect(result[0].relationships[0]).toMatchObject({
        name: 'project',
        sourceFields: ['projectID', 'projectOrgID'],
        destFields: ['id', 'orgID'],
      });
    });
  });

  describe('junction tables (many-to-many)', () => {
    test('detects junction table and creates many-to-many relationships', () => {
      const schema = makeSchema(
        [
          {name: 'users', columns: ['id', 'name'], primaryKey: ['id']},
          {name: 'roles', columns: ['id', 'name'], primaryKey: ['id']},
          {
            name: 'userRoles',
            columns: ['userID', 'roleID'],
            primaryKey: ['userID', 'roleID'],
          },
        ],
        [
          {
            sourceTable: 'userRoles',
            sourceColumns: ['userID'],
            targetTable: 'users',
            targetColumns: ['id'],
          },
          {
            sourceTable: 'userRoles',
            sourceColumns: ['roleID'],
            targetTable: 'roles',
            targetColumns: ['id'],
          },
        ],
      );

      const result = inferRelationships(schema);

      // Should have relationships for users and roles (not userRoles)
      const userRels = result.find(r => r.sourceTable === 'users');
      const roleRels = result.find(r => r.sourceTable === 'roles');

      expect(userRels).toBeDefined();
      expect(userRels!.relationships).toHaveLength(1);
      expect(userRels!.relationships[0]).toMatchObject({
        name: 'roles',
        cardinality: 'many',
        destTable: 'roles',
        through: {
          junctionTable: 'userRoles',
        },
      });

      expect(roleRels).toBeDefined();
      expect(roleRels!.relationships).toHaveLength(1);
      expect(roleRels!.relationships[0]).toMatchObject({
        name: 'users',
        cardinality: 'many',
        destTable: 'users',
      });
    });

    test('skips junction table detection when disabled', () => {
      const schema = makeSchema(
        [
          {name: 'users', columns: ['id'], primaryKey: ['id']},
          {name: 'roles', columns: ['id'], primaryKey: ['id']},
          {
            name: 'userRoles',
            columns: ['userID', 'roleID'],
            primaryKey: ['userID', 'roleID'],
          },
        ],
        [
          {
            sourceTable: 'userRoles',
            sourceColumns: ['userID'],
            targetTable: 'users',
            targetColumns: ['id'],
          },
          {
            sourceTable: 'userRoles',
            sourceColumns: ['roleID'],
            targetTable: 'roles',
            targetColumns: ['id'],
          },
        ],
      );

      const result = inferRelationships(schema, {detectManyToMany: false});

      // Should have direct FK relationships from userRoles
      const junctionRels = result.find(r => r.sourceTable === 'userRoles');
      expect(junctionRels).toBeDefined();
      expect(junctionRels!.relationships).toHaveLength(2);
    });
  });

  describe('inverse relationships', () => {
    test('generates inverse many relationships when enabled', () => {
      const schema = makeSchema(
        [
          {name: 'users', columns: ['id'], primaryKey: ['id']},
          {name: 'posts', columns: ['id', 'authorID'], primaryKey: ['id']},
        ],
        [
          {
            sourceTable: 'posts',
            sourceColumns: ['authorID'],
            targetTable: 'users',
            targetColumns: ['id'],
          },
        ],
      );

      const result = inferRelationships(schema, {generateInverse: true});

      const userRels = result.find(r => r.sourceTable === 'users');
      expect(userRels).toBeDefined();
      expect(userRels!.relationships[0]).toMatchObject({
        name: 'posts',
        cardinality: 'many',
        destTable: 'posts',
      });
    });
  });

  describe('custom relationship names', () => {
    test('uses custom names when provided', () => {
      const schema = makeSchema(
        [
          {name: 'users', columns: ['id'], primaryKey: ['id']},
          {name: 'posts', columns: ['id', 'ownerID'], primaryKey: ['id']},
        ],
        [
          {
            sourceTable: 'posts',
            sourceColumns: ['ownerID'],
            targetTable: 'users',
            targetColumns: ['id'],
          },
        ],
      );

      const result = inferRelationships(schema, {
        relationshipNames: {'posts.ownerID': 'author'},
      });

      expect(result[0].relationships[0].name).toBe('author');
    });
  });
});

describe('isOneToOneRelationship', () => {
  test('returns true when FK columns have unique constraint', () => {
    const fk: IntrospectedForeignKey = {
      constraintName: 'profile_userID_fkey',
      sourceSchema: 'public',
      sourceTable: 'profiles',
      sourceColumns: ['userID'],
      targetSchema: 'public',
      targetTable: 'users',
      targetColumns: ['id'],
      onDelete: 'NO ACTION',
      onUpdate: 'NO ACTION',
    };

    const uniqueConstraints = [
      {
        constraintName: 'profiles_userID_key',
        schema: 'public',
        tableName: 'profiles',
        columns: ['userID'],
        isPrimaryKey: false,
      },
    ];

    expect(isOneToOneRelationship(fk, uniqueConstraints)).toBe(true);
  });

  test('returns false when FK columns are not unique', () => {
    const fk: IntrospectedForeignKey = {
      constraintName: 'posts_authorID_fkey',
      sourceSchema: 'public',
      sourceTable: 'posts',
      sourceColumns: ['authorID'],
      targetSchema: 'public',
      targetTable: 'users',
      targetColumns: ['id'],
      onDelete: 'NO ACTION',
      onUpdate: 'NO ACTION',
    };

    const uniqueConstraints = [
      {
        constraintName: 'posts_pkey',
        schema: 'public',
        tableName: 'posts',
        columns: ['id'],
        isPrimaryKey: true,
      },
    ];

    expect(isOneToOneRelationship(fk, uniqueConstraints)).toBe(false);
  });
});
```

## Acceptance Criteria

1. **Basic FK → one**: Single-column FK generates `one` relationship
2. **Composite FK**: Multi-column FK generates single relationship with multiple fields
3. **Multiple FKs to same table**: Generates unique relationship names
4. **Relationship naming**: Derives sensible names from column names (strips ID suffix)
5. **Junction tables**: Detects and generates many-to-many relationships
6. **Custom names**: Respects custom relationship name overrides
7. **Inverse relationships**: Optionally generates `many` inverse relationships
8. **One-to-one detection**: Helper identifies one-to-one via unique constraints
9. **Tests**: All unit tests pass

## Edge Cases Handled

1. **Self-referential FK**: e.g., `parent_id` referencing same table
2. **Multiple FKs between same tables**: Different relationship names
3. **Junction tables with extra columns**: Still detected if PK matches FKs
4. **Name conflicts**: Appends target table or source column to disambiguate

## Limitations

1. **Relationship names are heuristic**: May not always match desired naming
2. **Junction detection is heuristic**: May miss complex junction tables
3. **Inverse relationships optional**: Not generated by default (often manually defined)
4. **No polymorphic relationships**: Cannot infer from single FK column pointing to multiple tables

## Dependencies

- Phase 1 types (IntrospectedForeignKey, etc.)
- vitest for testing

## Notes

- This phase has no database dependencies - all tests are unit tests
- Relationship names follow common conventions (singular for `one`, plural for `many`)
- Custom names override allows users to fix incorrect inferences
