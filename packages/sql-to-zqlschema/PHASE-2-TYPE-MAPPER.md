# Phase 2: Type Mapper

## Goal

Build the type mapper that converts PostgreSQL column types to Zero schema types. This component takes the introspected column metadata and determines the appropriate Zero type (`string`, `number`, `boolean`, `json`).

## Prerequisites

- Phase 1 complete (introspection types available)

## Files to Create/Modify

```
packages/sql-to-zqlschema/
├── src/
│   ├── type-mapper.ts        # Type mapping logic
│   └── types.ts              # Add ZeroColumnType
├── test/
│   └── type-mapper.test.ts   # Unit tests (no DB needed)
```

## Implementation Plan

### Step 1: Extend Types (`src/types.ts`)

Add Zero-specific types:

```typescript
/**
 * Zero column types
 */
export type ZeroColumnType = 'string' | 'number' | 'boolean' | 'json';

/**
 * Mapped column info for code generation
 */
export interface MappedColumn {
  name: string;
  zeroType: ZeroColumnType;
  isOptional: boolean;
  isEnum: boolean;
  enumName: string | null; // If isEnum, the enum type name
  enumValues: string[] | null; // If isEnum, the possible values
  pgType: string; // Original PG type for comments/debugging
  warnings: string[]; // Any mapping warnings
}

/**
 * Mapped table for code generation
 */
export interface MappedTable {
  schema: string;
  name: string;
  columns: MappedColumn[];
  primaryKey: string[];
}

/**
 * Type mapping options
 */
export interface TypeMapperOptions {
  /** Treat nullable columns as optional (default: true) */
  nullableAsOptional?: boolean;
  /** Treat columns with defaults as optional (default: true) */
  defaultAsOptional?: boolean;
  /** Custom type overrides: 'table.column' -> ZeroColumnType */
  typeOverrides?: Record<string, ZeroColumnType>;
  /** Warn about unsupported types (default: true) */
  warnUnsupported?: boolean;
}
```

### Step 2: Type Mapper Implementation (`src/type-mapper.ts`)

```typescript
import type {
  IntrospectedColumn,
  IntrospectedTable,
  IntrospectedEnum,
  IntrospectedSchema,
  ZeroColumnType,
  MappedColumn,
  MappedTable,
  TypeMapperOptions,
} from './types.ts';

/**
 * PostgreSQL numeric types -> Zero 'number'
 */
const PG_NUMERIC_TYPES = new Set([
  'smallint',
  'integer',
  'int',
  'int2',
  'int4',
  'int8',
  'bigint',
  'smallserial',
  'serial',
  'serial2',
  'serial4',
  'serial8',
  'bigserial',
  'decimal',
  'numeric',
  'real',
  'double precision',
  'float',
  'float4',
  'float8',
]);

/**
 * PostgreSQL date/time types -> Zero 'number' (stored as timestamps)
 */
const PG_DATETIME_TYPES = new Set([
  'date',
  'time',
  'time with time zone',
  'time without time zone',
  'timestamp',
  'timestamptz',
  'timestamp with time zone',
  'timestamp without time zone',
]);

/**
 * PostgreSQL string types -> Zero 'string'
 */
const PG_STRING_TYPES = new Set([
  'bpchar',
  'character',
  'character varying',
  'char',
  'varchar',
  'text',
  'uuid',
  'name', // PostgreSQL internal identifier type
]);

/**
 * PostgreSQL boolean types -> Zero 'boolean'
 */
const PG_BOOLEAN_TYPES = new Set(['bool', 'boolean']);

/**
 * PostgreSQL JSON types -> Zero 'json'
 */
const PG_JSON_TYPES = new Set(['json', 'jsonb']);

/**
 * Unsupported types that will trigger warnings
 */
const UNSUPPORTED_TYPES = new Set([
  'bytea',
  'interval',
  'point',
  'line',
  'lseg',
  'box',
  'path',
  'polygon',
  'circle',
  'inet',
  'cidr',
  'macaddr',
  'macaddr8',
  'tsvector',
  'tsquery',
  'xml',
  'money',
  'bit',
  'bit varying',
  'varbit',
]);

/**
 * Normalize PostgreSQL type name for lookup
 * Strips length/precision args and lowercases
 */
function normalizeTypeName(pgType: string): string {
  // Remove array brackets if present
  let normalized = pgType.replace(/\[\]$/, '');

  // Remove arguments like (255) or (10,2)
  const parenIndex = normalized.indexOf('(');
  if (parenIndex !== -1) {
    normalized = normalized.substring(0, parenIndex);
  }

  return normalized.toLowerCase().trim();
}

/**
 * Map a single PostgreSQL type to Zero type
 */
export function mapPgTypeToZero(
  dataType: string,
  udtName: string,
  pgTypeClass: string,
  isArray: boolean,
): {zeroType: ZeroColumnType; isEnum: boolean; warning: string | null} {
  // Arrays always become JSON
  if (isArray) {
    return {zeroType: 'json', isEnum: false, warning: null};
  }

  // Enum types become string
  if (pgTypeClass === 'e') {
    return {zeroType: 'string', isEnum: true, warning: null};
  }

  const normalizedDataType = normalizeTypeName(dataType);
  const normalizedUdtName = normalizeTypeName(udtName);

  // Check each type category
  if (
    PG_NUMERIC_TYPES.has(normalizedDataType) ||
    PG_NUMERIC_TYPES.has(normalizedUdtName)
  ) {
    return {zeroType: 'number', isEnum: false, warning: null};
  }

  if (
    PG_DATETIME_TYPES.has(normalizedDataType) ||
    PG_DATETIME_TYPES.has(normalizedUdtName)
  ) {
    return {zeroType: 'number', isEnum: false, warning: null};
  }

  if (
    PG_STRING_TYPES.has(normalizedDataType) ||
    PG_STRING_TYPES.has(normalizedUdtName)
  ) {
    return {zeroType: 'string', isEnum: false, warning: null};
  }

  if (
    PG_BOOLEAN_TYPES.has(normalizedDataType) ||
    PG_BOOLEAN_TYPES.has(normalizedUdtName)
  ) {
    return {zeroType: 'boolean', isEnum: false, warning: null};
  }

  if (
    PG_JSON_TYPES.has(normalizedDataType) ||
    PG_JSON_TYPES.has(normalizedUdtName)
  ) {
    return {zeroType: 'json', isEnum: false, warning: null};
  }

  // Check for unsupported types
  if (
    UNSUPPORTED_TYPES.has(normalizedDataType) ||
    UNSUPPORTED_TYPES.has(normalizedUdtName)
  ) {
    return {
      zeroType: 'string',
      isEnum: false,
      warning: `Unsupported type '${dataType}' mapped to string`,
    };
  }

  // Domain types - try to map based on the udtName which may hint at base type
  if (pgTypeClass === 'd') {
    return {
      zeroType: 'string',
      isEnum: false,
      warning: `Domain type '${udtName}' mapped to string`,
    };
  }

  // Unknown type - default to string with warning
  return {
    zeroType: 'string',
    isEnum: false,
    warning: `Unknown type '${dataType}' (udt: ${udtName}) mapped to string`,
  };
}

/**
 * Determine if a column should be optional in Zero schema
 */
export function isColumnOptional(
  column: IntrospectedColumn,
  options: TypeMapperOptions,
): boolean {
  const {nullableAsOptional = true, defaultAsOptional = true} = options;

  // Nullable columns are optional
  if (nullableAsOptional && column.isNullable) {
    return true;
  }

  // Columns with defaults are optional (Zero doesn't need to provide a value)
  if (defaultAsOptional && column.defaultValue !== null) {
    return true;
  }

  return false;
}

/**
 * Map a single column
 */
export function mapColumn(
  column: IntrospectedColumn,
  tableName: string,
  enums: IntrospectedEnum[],
  options: TypeMapperOptions,
): MappedColumn {
  const {typeOverrides = {}, warnUnsupported = true} = options;

  // Check for type override
  const overrideKey = `${tableName}.${column.name}`;
  const warnings: string[] = [];

  let zeroType: ZeroColumnType;
  let isEnum = false;
  let enumName: string | null = null;
  let enumValues: string[] | null = null;

  if (typeOverrides[overrideKey]) {
    zeroType = typeOverrides[overrideKey];
  } else {
    const mapping = mapPgTypeToZero(
      column.dataType,
      column.udtName,
      column.pgTypeClass,
      column.isArray,
    );

    zeroType = mapping.zeroType;
    isEnum = mapping.isEnum;

    if (mapping.warning && warnUnsupported) {
      warnings.push(mapping.warning);
    }

    // If it's an enum, find the enum definition
    if (isEnum) {
      const enumDef = enums.find(e => e.name === column.udtName);
      if (enumDef) {
        enumName = enumDef.name;
        enumValues = enumDef.values;
      }
    }
  }

  return {
    name: column.name,
    zeroType,
    isOptional: isColumnOptional(column, options),
    isEnum,
    enumName,
    enumValues,
    pgType: column.dataType,
    warnings,
  };
}

/**
 * Map an entire table
 */
export function mapTable(
  table: IntrospectedTable,
  enums: IntrospectedEnum[],
  options: TypeMapperOptions,
): MappedTable {
  return {
    schema: table.schema,
    name: table.name,
    columns: table.columns.map(col =>
      mapColumn(col, table.name, enums, options),
    ),
    primaryKey: table.primaryKey,
  };
}

/**
 * Map an entire schema
 */
export function mapSchema(
  schema: IntrospectedSchema,
  options: TypeMapperOptions = {},
): {
  tables: MappedTable[];
  enums: IntrospectedEnum[];
  warnings: Array<{table: string; column: string; message: string}>;
} {
  const tables: MappedTable[] = [];
  const warnings: Array<{table: string; column: string; message: string}> = [];

  for (const table of schema.tables) {
    const mappedTable = mapTable(table, schema.enums, options);
    tables.push(mappedTable);

    // Collect warnings
    for (const col of mappedTable.columns) {
      for (const warning of col.warnings) {
        warnings.push({
          table: table.name,
          column: col.name,
          message: warning,
        });
      }
    }
  }

  return {
    tables,
    enums: schema.enums,
    warnings,
  };
}
```

### Step 3: Unit Tests (`test/type-mapper.test.ts`)

```typescript
import {describe, test, expect} from 'vitest';
import {
  mapPgTypeToZero,
  mapColumn,
  mapTable,
  mapSchema,
  isColumnOptional,
} from '../src/type-mapper.ts';
import type {
  IntrospectedColumn,
  IntrospectedTable,
  IntrospectedSchema,
} from '../src/types.ts';

describe('mapPgTypeToZero', () => {
  describe('numeric types', () => {
    test.each([
      ['integer', 'int4'],
      ['bigint', 'int8'],
      ['smallint', 'int2'],
      ['serial', 'serial4'],
      ['bigserial', 'serial8'],
      ['numeric', 'numeric'],
      ['decimal', 'numeric'],
      ['real', 'float4'],
      ['double precision', 'float8'],
    ])('%s -> number', (dataType, udtName) => {
      const result = mapPgTypeToZero(dataType, udtName, 'b', false);
      expect(result.zeroType).toBe('number');
      expect(result.isEnum).toBe(false);
      expect(result.warning).toBeNull();
    });
  });

  describe('date/time types', () => {
    test.each([
      ['timestamp with time zone', 'timestamptz'],
      ['timestamp without time zone', 'timestamp'],
      ['date', 'date'],
      ['time', 'time'],
    ])('%s -> number', (dataType, udtName) => {
      const result = mapPgTypeToZero(dataType, udtName, 'b', false);
      expect(result.zeroType).toBe('number');
    });
  });

  describe('string types', () => {
    test.each([
      ['character varying', 'varchar'],
      ['character', 'bpchar'],
      ['text', 'text'],
      ['uuid', 'uuid'],
    ])('%s -> string', (dataType, udtName) => {
      const result = mapPgTypeToZero(dataType, udtName, 'b', false);
      expect(result.zeroType).toBe('string');
    });

    test('varchar with length', () => {
      const result = mapPgTypeToZero(
        'character varying(255)',
        'varchar',
        'b',
        false,
      );
      expect(result.zeroType).toBe('string');
    });
  });

  describe('boolean types', () => {
    test('boolean -> boolean', () => {
      const result = mapPgTypeToZero('boolean', 'bool', 'b', false);
      expect(result.zeroType).toBe('boolean');
    });
  });

  describe('json types', () => {
    test.each([
      ['json', 'json'],
      ['jsonb', 'jsonb'],
    ])('%s -> json', (dataType, udtName) => {
      const result = mapPgTypeToZero(dataType, udtName, 'b', false);
      expect(result.zeroType).toBe('json');
    });
  });

  describe('array types', () => {
    test('text[] -> json', () => {
      const result = mapPgTypeToZero('ARRAY', '_text', 'b', true);
      expect(result.zeroType).toBe('json');
    });

    test('integer[] -> json', () => {
      const result = mapPgTypeToZero('ARRAY', '_int4', 'b', true);
      expect(result.zeroType).toBe('json');
    });
  });

  describe('enum types', () => {
    test('enum -> string with isEnum flag', () => {
      const result = mapPgTypeToZero('USER-DEFINED', 'user_role', 'e', false);
      expect(result.zeroType).toBe('string');
      expect(result.isEnum).toBe(true);
    });
  });

  describe('unsupported types', () => {
    test.each(['bytea', 'inet', 'point', 'tsvector', 'xml'])(
      '%s -> string with warning',
      dataType => {
        const result = mapPgTypeToZero(dataType, dataType, 'b', false);
        expect(result.zeroType).toBe('string');
        expect(result.warning).toContain('Unsupported');
      },
    );
  });

  describe('unknown types', () => {
    test('unknown type -> string with warning', () => {
      const result = mapPgTypeToZero('custom_type', 'custom_type', 'b', false);
      expect(result.zeroType).toBe('string');
      expect(result.warning).toContain('Unknown');
    });
  });
});

describe('isColumnOptional', () => {
  const makeColumn = (
    overrides: Partial<IntrospectedColumn>,
  ): IntrospectedColumn => ({
    name: 'test',
    position: 1,
    dataType: 'text',
    udtName: 'text',
    isNullable: false,
    characterMaxLength: null,
    numericPrecision: null,
    numericScale: null,
    defaultValue: null,
    pgTypeClass: 'b',
    isArray: false,
    arrayElementTypeClass: null,
    ...overrides,
  });

  test('nullable column is optional by default', () => {
    const col = makeColumn({isNullable: true});
    expect(isColumnOptional(col, {})).toBe(true);
  });

  test('column with default is optional by default', () => {
    const col = makeColumn({defaultValue: "'default'"});
    expect(isColumnOptional(col, {})).toBe(true);
  });

  test('non-nullable column without default is required', () => {
    const col = makeColumn({isNullable: false, defaultValue: null});
    expect(isColumnOptional(col, {})).toBe(false);
  });

  test('nullableAsOptional: false makes nullable columns required', () => {
    const col = makeColumn({isNullable: true});
    expect(isColumnOptional(col, {nullableAsOptional: false})).toBe(false);
  });

  test('defaultAsOptional: false makes columns with defaults required', () => {
    const col = makeColumn({defaultValue: "'default'"});
    expect(isColumnOptional(col, {defaultAsOptional: false})).toBe(false);
  });
});

describe('mapColumn', () => {
  const makeColumn = (
    overrides: Partial<IntrospectedColumn>,
  ): IntrospectedColumn => ({
    name: 'test',
    position: 1,
    dataType: 'text',
    udtName: 'text',
    isNullable: false,
    characterMaxLength: null,
    numericPrecision: null,
    numericScale: null,
    defaultValue: null,
    pgTypeClass: 'b',
    isArray: false,
    arrayElementTypeClass: null,
    ...overrides,
  });

  test('maps basic column', () => {
    const col = makeColumn({name: 'title', dataType: 'text', udtName: 'text'});
    const result = mapColumn(col, 'posts', [], {});

    expect(result).toMatchObject({
      name: 'title',
      zeroType: 'string',
      isOptional: false,
      isEnum: false,
      enumName: null,
    });
  });

  test('maps enum column with enum info', () => {
    const col = makeColumn({
      name: 'status',
      dataType: 'USER-DEFINED',
      udtName: 'status_enum',
      pgTypeClass: 'e',
    });

    const enums = [
      {
        schema: 'public',
        name: 'status_enum',
        values: ['draft', 'published', 'archived'],
      },
    ];

    const result = mapColumn(col, 'posts', enums, {});

    expect(result).toMatchObject({
      name: 'status',
      zeroType: 'string',
      isEnum: true,
      enumName: 'status_enum',
      enumValues: ['draft', 'published', 'archived'],
    });
  });

  test('respects type overrides', () => {
    const col = makeColumn({name: 'data', dataType: 'text', udtName: 'text'});
    const result = mapColumn(col, 'posts', [], {
      typeOverrides: {'posts.data': 'json'},
    });

    expect(result.zeroType).toBe('json');
  });
});

describe('mapTable', () => {
  test('maps table with all columns', () => {
    const table: IntrospectedTable = {
      schema: 'public',
      name: 'users',
      columns: [
        {
          name: 'id',
          position: 1,
          dataType: 'character varying',
          udtName: 'varchar',
          isNullable: false,
          characterMaxLength: null,
          numericPrecision: null,
          numericScale: null,
          defaultValue: null,
          pgTypeClass: 'b',
          isArray: false,
          arrayElementTypeClass: null,
        },
        {
          name: 'age',
          position: 2,
          dataType: 'integer',
          udtName: 'int4',
          isNullable: true,
          characterMaxLength: null,
          numericPrecision: 32,
          numericScale: 0,
          defaultValue: null,
          pgTypeClass: 'b',
          isArray: false,
          arrayElementTypeClass: null,
        },
      ],
      primaryKey: ['id'],
    };

    const result = mapTable(table, [], {});

    expect(result.name).toBe('users');
    expect(result.primaryKey).toEqual(['id']);
    expect(result.columns).toHaveLength(2);
    expect(result.columns[0]).toMatchObject({
      name: 'id',
      zeroType: 'string',
      isOptional: false,
    });
    expect(result.columns[1]).toMatchObject({
      name: 'age',
      zeroType: 'number',
      isOptional: true,
    });
  });
});

describe('mapSchema', () => {
  test('maps schema and collects warnings', () => {
    const schema: IntrospectedSchema = {
      schemaName: 'public',
      tables: [
        {
          schema: 'public',
          name: 'test',
          columns: [
            {
              name: 'id',
              position: 1,
              dataType: 'text',
              udtName: 'text',
              isNullable: false,
              characterMaxLength: null,
              numericPrecision: null,
              numericScale: null,
              defaultValue: null,
              pgTypeClass: 'b',
              isArray: false,
              arrayElementTypeClass: null,
            },
            {
              name: 'geo',
              position: 2,
              dataType: 'point',
              udtName: 'point',
              isNullable: true,
              characterMaxLength: null,
              numericPrecision: null,
              numericScale: null,
              defaultValue: null,
              pgTypeClass: 'b',
              isArray: false,
              arrayElementTypeClass: null,
            },
          ],
          primaryKey: ['id'],
        },
      ],
      enums: [],
      foreignKeys: [],
      uniqueConstraints: [],
    };

    const result = mapSchema(schema);

    expect(result.tables).toHaveLength(1);
    expect(result.warnings).toHaveLength(1);
    expect(result.warnings[0]).toMatchObject({
      table: 'test',
      column: 'geo',
    });
  });
});
```

## Acceptance Criteria

1. **Numeric types**: All PostgreSQL numeric types map to `number`
2. **Date/time types**: All date/time types map to `number` (timestamps)
3. **String types**: varchar, text, char, uuid map to `string`
4. **Boolean**: bool/boolean map to `boolean`
5. **JSON**: json/jsonb map to `json`
6. **Arrays**: All array types map to `json`
7. **Enums**: Enum types map to `string` with enum metadata preserved
8. **Optionality**: Nullable and default columns correctly marked optional
9. **Warnings**: Unsupported types generate warnings
10. **Overrides**: Custom type overrides work correctly
11. **Tests**: All unit tests pass

## Dependencies

- Phase 1 types
- vitest for testing

## Notes

- This phase has no database dependencies - all tests are unit tests
- Type mapping matches the existing `zero-cache/src/types/pg-data-type.ts` logic
- Warnings help users identify columns that may need manual attention
