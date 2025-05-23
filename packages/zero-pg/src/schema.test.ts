/* eslint-disable @typescript-eslint/naming-convention */
import {describe, expect, test} from 'vitest';
import {unreachable} from '../../shared/src/asserts.ts';
import type {
  ServerColumnSchema,
  ServerSchema,
} from '../../zero-schema/src/server-schema.ts';
import {pgToZqlTypeMap} from '../../zero-cache/src/types/pg.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  json,
  number,
  string,
  table,
  type ColumnBuilder,
} from '../../zero-schema/src/builder/table-builder.ts';
import type {
  SchemaValue,
  ValueType,
} from '../../zero-schema/src/table-schema.ts';
import {
  checkSchemasAreCompatible,
  type SchemaIncompatibilityError,
} from './schema.ts';

describe('checkSchemasAreCompatible', () => {
  test('should return empty array when schemas are compatible', () => {
    const schema = createSchema({
      tables: [
        table('test')
          .columns({
            id: string(),
            value: number(),
          })
          .primaryKey('id'),
      ],
      relationships: [],
    });

    const serverSchema: ServerSchema = {
      test: {
        id: {type: 'text', isEnum: false, isArray: false},
        value: {type: 'integer', isEnum: false, isArray: false},
      },
    };

    const errors = checkSchemasAreCompatible(schema, serverSchema);
    expect(errors).toEqual([]);
  });

  test('should detect missing table', () => {
    const schema = createSchema({
      tables: [
        table('test')
          .columns({
            id: string(),
            value: number(),
          })
          .primaryKey('id'),
      ],
      relationships: [],
    });

    const serverSchema: ServerSchema = {};

    const errors = checkSchemasAreCompatible(schema, serverSchema);
    expect(errors).toEqual([
      {
        type: 'missingTable',
        table: 'test',
      },
    ]);
  });

  test('should detect missing column', () => {
    const schema = createSchema({
      tables: [
        table('test')
          .columns({
            id: string(),
            value: number(),
          })
          .primaryKey('id'),
      ],
      relationships: [],
    });

    const serverSchema: ServerSchema = {
      test: {
        id: {type: 'text', isEnum: false, isArray: false},
      },
    };

    const errors = checkSchemasAreCompatible(schema, serverSchema);
    expect(errors).toEqual([
      {
        type: 'missingColumn',
        table: 'test',
        column: 'value',
      },
    ]);
  });

  test('should detect type mismatch when declared type is wrong', () => {
    const schema = createSchema({
      tables: [
        table('test')
          .columns({
            id: string(),
            value: string(), // Declared as string but should be number
          })
          .primaryKey('id'),
      ],
      relationships: [],
    });

    const serverSchema: ServerSchema = {
      test: {
        id: {type: 'text', isEnum: false, isArray: false},
        value: {type: 'integer', isEnum: false, isArray: false}, // PostgreSQL has it as integer
      },
    };

    const errors = checkSchemasAreCompatible(schema, serverSchema);
    expect(errors).toEqual([
      {
        type: 'typeError',
        table: 'test',
        column: 'value',
        pgType: 'integer', // The actual PostgreSQL type
        declaredType: 'string', // What the user declared in schema
        requiredType: 'number', // What it should have been based on pgType
      },
    ]);
  });

  test('should handle enum types correctly', () => {
    const schema = createSchema({
      tables: [
        table('test')
          .columns({
            id: string(),
            status: string(), // Declared as string
          })
          .primaryKey('id'),
      ],
      relationships: [],
    });

    const serverSchema: ServerSchema = {
      test: {
        id: {type: 'text', isEnum: false, isArray: false},
        status: {type: 'status_enum', isEnum: true, isArray: false}, // PostgreSQL has it as enum
      },
    };

    const errors = checkSchemasAreCompatible(schema, serverSchema);
    expect(errors).toEqual([]);
  });

  test('should handle multiple errors across tables', () => {
    const schema = createSchema({
      tables: [
        table('test1')
          .columns({
            id: string(),
            value: number(),
          })
          .primaryKey('id'),
        table('test2')
          .columns({
            id: string(),
            value: string(), // Declared as string but should be number
          })
          .primaryKey('id'),
      ],
      relationships: [],
    });

    const serverSchema: ServerSchema = {
      test1: {
        id: {type: 'text', isEnum: false, isArray: false},
      },
      test2: {
        id: {type: 'text', isEnum: false, isArray: false},
        value: {type: 'integer', isEnum: false, isArray: false}, // PostgreSQL has it as integer
      },
    };

    const errors = checkSchemasAreCompatible(schema, serverSchema);
    expect(errors).toEqual([
      {
        type: 'missingColumn',
        table: 'test1',
        column: 'value',
      },
      {
        type: 'typeError',
        table: 'test2',
        column: 'value',
        pgType: 'integer', // The actual PostgreSQL type
        declaredType: 'string', // What the user declared in schema
        requiredType: 'number', // What it should have been based on pgType
      },
    ]);
  });

  test('should handle server name mapping', () => {
    const schema = createSchema({
      tables: [
        table('test')
          .from('server_test')
          .columns({
            id: string(),
            value: number(),
          })
          .primaryKey('id'),
      ],
      relationships: [],
    });

    const serverSchema: ServerSchema = {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      server_test: {
        id: {type: 'text', isEnum: false, isArray: false},
        value: {type: 'integer', isEnum: false, isArray: false},
      },
    };

    const errors = checkSchemasAreCompatible(schema, serverSchema);
    expect(errors).toEqual([]);
  });

  test('should handle schema-qualified table names', () => {
    const schema = createSchema({
      tables: [
        table('test')
          .from('custom_schema.test')
          .columns({
            id: string(),
            value: number(),
          })
          .primaryKey('id'),
      ],
      relationships: [],
    });

    const serverSchema: ServerSchema = {
      'custom_schema.test': {
        id: {type: 'text', isEnum: false, isArray: false},
        value: {type: 'integer', isEnum: false, isArray: false},
      },
    };

    const errors = checkSchemasAreCompatible(schema, serverSchema);
    expect(errors).toEqual([]);
  });

  test('should handle all PostgreSQL types correctly', () => {
    // Generate column definitions for incompatible schema (all types wrong)
    const incompatibleColumns: Record<
      string,
      ColumnBuilder<SchemaValue<unknown>>
    > = {};
    const compatibleColumns: Record<
      string,
      ColumnBuilder<SchemaValue<unknown>>
    > = {};
    const serverColumns: Record<string, ServerColumnSchema> = {};

    // Helper to get the wrong type for a given PostgreSQL type
    const getWrongType = (correctType: ValueType) => {
      switch (correctType) {
        case 'number':
          return string();
        case 'string':
          return number();
        case 'boolean':
          return string();
        case 'json':
          return string();
        case 'null':
          throw new Error('invalid');
        default:
          unreachable(correctType);
      }
    };

    const getCorrectType = (correctType: ValueType) => {
      switch (correctType) {
        case 'number':
          return number();
        case 'string':
          return string();
        case 'boolean':
          return boolean();
        case 'json':
          return json();
        case 'null':
          throw new Error('invalid');
        default:
          unreachable(correctType);
      }
    };

    // Generate columns for all PostgreSQL types
    Object.entries(pgToZqlTypeMap).forEach(([pgType, zqlType]) => {
      const columnName = `${pgType.replace(/\s+/g, '_')}_col`;

      // Add to incompatible schema with wrong type
      incompatibleColumns[columnName] = getWrongType(zqlType);

      // Add to compatible schema with correct type
      compatibleColumns[columnName] = getCorrectType(zqlType);

      // Add to server schema
      serverColumns[columnName] = {
        type: pgType,
        isEnum: false,
        isArray: false,
      };
    });

    // Generate expected errors
    const expectedErrors: SchemaIncompatibilityError[] = Object.entries(
      pgToZqlTypeMap,
    ).map(([pgType, zqlType]) => ({
      type: 'typeError' as const,
      table: 'test',
      column: `${pgType.replace(/\s+/g, '_')}_col`,
      pgType,
      declaredType: getWrongType(zqlType).schema.type,
      requiredType: zqlType,
    }));

    // Add enum type separately since it's not in pgToZqlTypeMap
    const enumColName = 'enum_col';
    incompatibleColumns[enumColName] = boolean();
    compatibleColumns[enumColName] = string();
    serverColumns[enumColName] = {
      type: 'test_enum',
      isEnum: true,
      isArray: false,
    };

    // Add enum error
    expectedErrors.push({
      type: 'typeError' as const,
      table: 'test',
      column: 'enum_col',
      pgType: 'test_enum',
      declaredType: 'boolean',
      requiredType: 'string',
    });

    // Add string type variants with args
    for (const pgType of [
      'bpchar',
      'character',
      'character varying',
      'varchar',
    ]) {
      const pgTypeWithArg = `${pgType}(10)`;
      const columnName = `${pgTypeWithArg.replace(/\s+/g, '_')}_col`;

      // Add to incompatible schema with wrong type
      const wrongType = getWrongType('string');
      incompatibleColumns[columnName] = wrongType;

      // Add to compatible schema with correct type
      compatibleColumns[columnName] = getCorrectType('string');

      // Add to server schema
      serverColumns[columnName] = {
        type: pgTypeWithArg,
        isEnum: false,
        isArray: false,
      };

      expectedErrors.push({
        type: 'typeError' as const,
        table: 'test',
        column: columnName,
        pgType: pgTypeWithArg,
        declaredType: wrongType.schema.type,
        requiredType: 'string',
      });
    }

    // Add number type variants with args
    for (const pgType of ['numeric', 'decimal']) {
      const pgTypeWithArg = `${pgType}(10, 5)`;
      const columnName = `${pgTypeWithArg.replace(/\s+/g, '_')}_col`;

      // Add to incompatible schema with wrong type
      const wrongType = getWrongType('number');
      incompatibleColumns[columnName] = wrongType;

      // Add to compatible schema with correct type
      compatibleColumns[columnName] = getCorrectType('number');

      // Add to server schema
      serverColumns[columnName] = {
        type: pgTypeWithArg,
        isEnum: false,
        isArray: false,
      };

      expectedErrors.push({
        type: 'typeError' as const,
        table: 'test',
        column: columnName,
        pgType: pgTypeWithArg,
        declaredType: wrongType.schema.type,
        requiredType: 'number',
      });
    }

    // Create schemas
    const incompatibleSchema = createSchema({
      tables: [
        table('test').columns(incompatibleColumns).primaryKey('text_col'),
      ],
      relationships: [],
    });

    const compatibleSchema = createSchema({
      tables: [table('test').columns(compatibleColumns).primaryKey('text_col')],
      relationships: [],
    });

    const serverSchema: ServerSchema = {
      test: serverColumns,
    };

    // Test incompatible schema
    const incompatibleErrors = checkSchemasAreCompatible(
      incompatibleSchema,
      serverSchema,
    );

    expect(incompatibleErrors).toEqual(expectedErrors);

    // Test compatible schema
    const compatibleErrors = checkSchemasAreCompatible(
      compatibleSchema,
      serverSchema,
    );
    expect(compatibleErrors).toEqual([]);
  });
});
