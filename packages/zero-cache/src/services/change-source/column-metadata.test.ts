import {describe, expect, test} from 'vitest';
import {Database} from '../../../../zqlite/src/db.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {
  ColumnMetadataStore,
  CREATE_COLUMN_METADATA_TABLE,
  liteTypeStringToMetadata,
  metadataToLiteTypeString,
} from './column-metadata.ts';
import type {LiteTableSpec} from '../../db/specs.ts';

function createTestDb(): Database {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(CREATE_COLUMN_METADATA_TABLE);
  return db;
}

function createTestStore(): ColumnMetadataStore {
  const db = createTestDb();
  const store = ColumnMetadataStore.getInstance(db);
  if (!store) {
    throw new Error('Failed to create metadata store - table should exist');
  }
  return store;
}

describe('column-metadata', () => {
  test('creates table and enforces primary key', () => {
    const store = createTestStore();

    expect(store.hasTable()).toBe(true);

    store.insert('users', 'id', {
      upstreamType: 'int8',
      isNotNull: true,
      isEnum: false,
      isArray: false,
    });

    expect(() => {
      store.insert('users', 'id', {
        upstreamType: 'int4',
        isNotNull: false,
        isEnum: false,
        isArray: false,
      });
    }).toThrow();
  });

  test('insert and read metadata', () => {
    const store = createTestStore();

    const orig = {
      upstreamType: 'int8',
      isNotNull: true,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
    };
    store.insert('users', 'id', orig);

    expect(store.getColumn('users', 'id')).toEqual(orig);
  });

  test('update column metadata', () => {
    const store = createTestStore();
    store.insert('users', 'name', {
      upstreamType: 'varchar',
      isNotNull: false,
      isEnum: false,
      isArray: false,
    });

    store.update('users', 'name', 'full_name', {
      upstreamType: 'varchar',
      isNotNull: true,
      isEnum: false,
      isArray: false,
      characterMaxLength: 200,
    });

    expect(store.getColumn('users', 'full_name')).toMatchInlineSnapshot(`
      {
        "characterMaxLength": 200,
        "isArray": false,
        "isEnum": false,
        "isNotNull": true,
        "upstreamType": "varchar",
      }
    `);
  });

  test('delete column metadata', () => {
    const store = createTestStore();
    store.insert('users', 'id', {
      upstreamType: 'int8',
      isNotNull: false,
      isEnum: false,
      isArray: false,
    });
    store.insert('users', 'name', {
      upstreamType: 'varchar',
      isNotNull: false,
      isEnum: false,
      isArray: false,
    });

    store.deleteColumn('users', 'name');

    expect(store.getTable('users').size).toBe(1);
  });

  test('delete and rename table metadata', () => {
    const store = createTestStore();
    store.insert('users', 'id', {
      upstreamType: 'int8',
      isNotNull: false,
      isEnum: false,
      isArray: false,
    });
    store.insert('posts', 'id', {
      upstreamType: 'int8',
      isNotNull: false,
      isEnum: false,
      isArray: false,
    });

    store.renameTable('users', 'people');
    expect(store.getTable('people').size).toBe(1);

    store.deleteTable('people');
    expect(store.getTable('people').size).toBe(0);
    expect(store.getTable('posts').size).toBe(1);
  });

  test('converts pipe notation to structured metadata', () => {
    expect(liteTypeStringToMetadata('int8')).toEqual({
      upstreamType: 'int8',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
    });

    expect(liteTypeStringToMetadata('varchar|NOT_NULL', 255)).toEqual({
      upstreamType: 'varchar',
      isNotNull: true,
      isEnum: false,
      isArray: false,
      characterMaxLength: 255,
    });

    expect(liteTypeStringToMetadata('user_role|TEXT_ENUM')).toEqual({
      upstreamType: 'user_role',
      isNotNull: false,
      isEnum: true,
      isArray: false,
      characterMaxLength: null,
    });

    expect(liteTypeStringToMetadata('text[]')).toEqual({
      upstreamType: 'text[]',
      isNotNull: false,
      isEnum: false,
      isArray: true,
      characterMaxLength: null,
    });

    expect(liteTypeStringToMetadata('int4|NOT_NULL[]')).toEqual({
      upstreamType: 'int4[]',
      isNotNull: true,
      isEnum: false,
      isArray: true,
      characterMaxLength: null,
    });
  });

  describe('populateFromExistingTables', () => {
    test('populates metadata from LiteTableSpec array', () => {
      const store = createTestStore();

      const tables: LiteTableSpec[] = [
        {
          name: 'users',
          columns: {
            id: {
              pos: 1,
              dataType: 'int8|NOT_NULL',
              characterMaximumLength: null,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
            email: {
              pos: 2,
              dataType: 'varchar',
              characterMaximumLength: 255,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
            tags: {
              pos: 3,
              dataType: 'text[]',
              characterMaximumLength: null,
              notNull: false,
              dflt: null,
              elemPgTypeClass: null,
            },
          },
          primaryKey: ['id'],
        },
        {
          name: 'posts',
          columns: {
            id: {
              pos: 1,
              dataType: 'int8|NOT_NULL',
              characterMaximumLength: null,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
            status: {
              pos: 2,
              dataType: 'post_status|NOT_NULL|TEXT_ENUM',
              characterMaximumLength: null,
              notNull: true,
              dflt: null,
              elemPgTypeClass: null,
            },
          },
          primaryKey: ['id'],
        },
      ];

      store.populateFromExistingTables(tables);

      expect(store.getColumn('posts', 'id')).toEqual({
        upstreamType: 'int8',
        isNotNull: true,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      });

      expect(store.getColumn('posts', 'status')).toEqual({
        upstreamType: 'post_status',
        isNotNull: true,
        isEnum: true,
        isArray: false,
        characterMaxLength: null,
      });

      expect(store.getColumn('users', 'email')).toEqual({
        upstreamType: 'varchar',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: 255,
      });

      expect(store.getColumn('users', 'id')).toEqual({
        upstreamType: 'int8',
        isNotNull: true,
        isEnum: false,
        isArray: false,
        characterMaxLength: null,
      });

      expect(store.getColumn('users', 'tags')).toEqual({
        upstreamType: 'text[]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      });
    });

    test('handles empty table list', () => {
      const store = createTestStore();

      store.populateFromExistingTables([]);

      expect(store.getTable('users').size).toBe(0);
    });

    test('handles table with no columns', () => {
      const store = createTestStore();

      const tables: LiteTableSpec[] = [
        {
          name: 'empty_table',
          columns: {},
        },
      ];

      store.populateFromExistingTables(tables);

      expect(store.getTable('empty_table').size).toBe(0);
    });
  });

  describe('edge cases', () => {
    test('handles array of enums with new-style format', () => {
      // New-style format: 'user_role[]|TEXT_ENUM'
      const metadata = liteTypeStringToMetadata('user_role[]|TEXT_ENUM');

      expect(metadata).toEqual({
        upstreamType: 'user_role[]',
        isNotNull: false,
        isEnum: true,
        isArray: true,
        characterMaxLength: null,
      });
    });

    test('handles old-style array format with attributes', () => {
      // Old-style format: 'int4|NOT_NULL[]' (attributes before brackets)
      const metadata = liteTypeStringToMetadata('int4|NOT_NULL[]');

      expect(metadata).toEqual({
        upstreamType: 'int4[]',
        isNotNull: true,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      });
    });

    test('handles new-style array format with attributes', () => {
      // New-style format: 'int4[]|NOT_NULL' (attributes after brackets)
      const metadata = liteTypeStringToMetadata('int4[]|NOT_NULL');

      expect(metadata).toEqual({
        upstreamType: 'int4[]',
        isNotNull: true,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      });
    });

    test('handles complex combinations: array of enum with NOT_NULL', () => {
      // This tests the most complex case: array + enum + not null
      const metadata = liteTypeStringToMetadata('status[]|NOT_NULL|TEXT_ENUM');

      expect(metadata).toEqual({
        upstreamType: 'status[]',
        isNotNull: true,
        isEnum: true,
        isArray: true,
        characterMaxLength: null,
      });
    });

    test('handles multidimensional arrays', () => {
      // PostgreSQL supports multidimensional arrays like 'int4[][]'
      const metadata = liteTypeStringToMetadata('int4[][]');

      expect(metadata).toEqual({
        upstreamType: 'int4[][]',
        isNotNull: false,
        isEnum: false,
        isArray: true,
        characterMaxLength: null,
      });
    });
  });

  describe('round-trip conversions', () => {
    test('simple types remain consistent', () => {
      const cases = [
        'int8',
        'int4',
        'varchar',
        'text',
        'int8|NOT_NULL',
        'varchar|NOT_NULL',
      ];

      for (const input of cases) {
        const metadata = liteTypeStringToMetadata(input);
        const output = metadataToLiteTypeString(metadata);
        expect(output).toBe(input);
      }
    });

    test('enum types remain consistent', () => {
      const cases = [
        'user_role|TEXT_ENUM',
        'status|TEXT_ENUM',
        'user_role|NOT_NULL|TEXT_ENUM',
      ];

      for (const input of cases) {
        const metadata = liteTypeStringToMetadata(input);
        const output = metadataToLiteTypeString(metadata);
        expect(output).toBe(input);
      }
    });

    test('new-style array formats remain consistent', () => {
      const cases = [
        'text[]',
        'int4[]',
        'int4[]|NOT_NULL',
        'varchar[]',
        'int4[][]', // multidimensional
      ];

      for (const input of cases) {
        const metadata = liteTypeStringToMetadata(input);
        const output = metadataToLiteTypeString(metadata);
        expect(output).toBe(input);
      }
    });

    test('old-style array formats normalize to new-style', () => {
      // Old-style format: attributes before brackets, e.g., 'int4|NOT_NULL[]'
      // Should normalize to: 'int4[]|NOT_NULL'
      const oldStyleCases: Array<{input: string; expected: string}> = [
        {input: 'int4|NOT_NULL[]', expected: 'int4[]|NOT_NULL'},
        {input: 'text[]', expected: 'text[]'}, // Already new-style
        {input: 'varchar|NOT_NULL[]', expected: 'varchar[]|NOT_NULL'},
      ];

      for (const {input, expected} of oldStyleCases) {
        const metadata = liteTypeStringToMetadata(input);
        const output = metadataToLiteTypeString(metadata);
        expect(output).toBe(expected);
      }
    });

    test('array of enums converts correctly', () => {
      const cases: Array<{input: string; expected: string}> = [
        {input: 'user_role[]|TEXT_ENUM', expected: 'user_role[]|TEXT_ENUM'},
        {
          input: 'user_role[]|NOT_NULL|TEXT_ENUM',
          expected: 'user_role[]|NOT_NULL|TEXT_ENUM',
        },
        {
          input: 'status[]|TEXT_ENUM',
          expected: 'status[]|TEXT_ENUM',
        },
      ];

      for (const {input, expected} of cases) {
        const metadata = liteTypeStringToMetadata(input);
        const output = metadataToLiteTypeString(metadata);
        expect(output).toBe(expected);
      }
    });

    test('complex combinations preserve all attributes', () => {
      const input = 'status[]|NOT_NULL|TEXT_ENUM';

      const metadata = liteTypeStringToMetadata(input);
      const output = metadataToLiteTypeString(metadata);

      // Verify metadata was parsed correctly
      expect(metadata).toEqual({
        upstreamType: 'status[]',
        isNotNull: true,
        isEnum: true,
        isArray: true,
        characterMaxLength: null,
      });

      // Verify round-trip produces the same format
      expect(output).toBe(input);
    });

    test('character max length is preserved in metadata but not in type string', () => {
      // Character max length is stored separately in the metadata table
      // and is not part of the type string format
      const input = 'varchar';
      const charMaxLength = 255;

      const metadata = liteTypeStringToMetadata(input, charMaxLength);
      const output = metadataToLiteTypeString(metadata);

      expect(metadata.characterMaxLength).toBe(charMaxLength);
      expect(output).toBe('varchar');
    });

    test('all metadata fields are correctly preserved', () => {
      const testCases = [
        {
          input: 'int8',
          expectedMetadata: {
            upstreamType: 'int8',
            isNotNull: false,
            isEnum: false,
            isArray: false,
            characterMaxLength: null,
          },
        },
        {
          input: 'varchar|NOT_NULL',
          characterMaxLength: 200,
          expectedMetadata: {
            upstreamType: 'varchar',
            isNotNull: true,
            isEnum: false,
            isArray: false,
            characterMaxLength: 200,
          },
        },
        {
          input: 'user_role|TEXT_ENUM',
          expectedMetadata: {
            upstreamType: 'user_role',
            isNotNull: false,
            isEnum: true,
            isArray: false,
            characterMaxLength: null,
          },
        },
        {
          input: 'int4[]|NOT_NULL',
          expectedMetadata: {
            upstreamType: 'int4[]',
            isNotNull: true,
            isEnum: false,
            isArray: true,
            characterMaxLength: null,
          },
        },
        {
          input: 'status[]|NOT_NULL|TEXT_ENUM',
          expectedMetadata: {
            upstreamType: 'status[]',
            isNotNull: true,
            isEnum: true,
            isArray: true,
            characterMaxLength: null,
          },
        },
      ];

      for (const {input, characterMaxLength, expectedMetadata} of testCases) {
        const metadata = liteTypeStringToMetadata(input, characterMaxLength);
        expect(metadata).toEqual(expectedMetadata);

        // Verify round-trip back to string
        const output = metadataToLiteTypeString(metadata);
        expect(output).toBe(input);
      }
    });
  });
});
