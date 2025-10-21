import {describe, expect, test} from 'vitest';
import {Database} from '../../../../zqlite/src/db.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {
  CREATE_COLUMN_METADATA_TABLE,
  deleteColumnMetadata,
  deleteTableMetadata,
  hasColumnMetadataTable,
  insertColumnMetadata,
  liteTypeStringToMetadata,
  populateColumnMetadataFromExistingTables,
  renameTableMetadata,
  updateColumnMetadata,
} from './column-metadata.ts';
import type {LiteTableSpec} from '../../db/specs.ts';

function createTestDb(): Database {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(CREATE_COLUMN_METADATA_TABLE);
  return db;
}

describe('column-metadata', () => {
  test('creates table and enforces primary key', () => {
    const db = createTestDb();

    expect(hasColumnMetadataTable(db)).toBe(true);

    insertColumnMetadata(db, 'users', 'id', {
      upstreamType: 'int8',
      isNotNull: true,
      isEnum: false,
      isArray: false,
    });

    expect(() => {
      insertColumnMetadata(db, 'users', 'id', {
        upstreamType: 'int4',
        isNotNull: false,
        isEnum: false,
        isArray: false,
      });
    }).toThrow();
  });

  test('insert and read metadata', () => {
    const db = createTestDb();

    insertColumnMetadata(db, 'users', 'id', {
      upstreamType: 'int8',
      isNotNull: true,
      isEnum: false,
      isArray: false,
    });

    const row = db
      .prepare(
        'SELECT * FROM "_zero.column_metadata" WHERE table_name = ? AND column_name = ?',
      )
      .get('users', 'id') as Record<string, unknown>;

    expect(row).toEqual({
      table_name: 'users',
      column_name: 'id',
      upstream_type: 'int8',
      is_not_null: 1,
      is_enum: 0,
      is_array: 0,
      character_max_length: null,
    });
  });

  test('handles various column types', () => {
    const db = createTestDb();

    // Nullable with character length
    insertColumnMetadata(db, 'users', 'email', {
      upstreamType: 'varchar',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: 255,
    });

    // Enum
    insertColumnMetadata(db, 'users', 'role', {
      upstreamType: 'user_role',
      isNotNull: false,
      isEnum: true,
      isArray: false,
    });

    // Array
    insertColumnMetadata(db, 'users', 'tags', {
      upstreamType: 'text[]',
      isNotNull: false,
      isEnum: false,
      isArray: true,
    });

    const email = db
      .prepare('SELECT * FROM "_zero.column_metadata" WHERE column_name = ?')
      .get('email') as Record<string, unknown>;
    expect(email.character_max_length).toBe(255);

    const role = db
      .prepare('SELECT * FROM "_zero.column_metadata" WHERE column_name = ?')
      .get('role') as Record<string, unknown>;
    expect(role.is_enum).toBe(1);

    const tags = db
      .prepare('SELECT * FROM "_zero.column_metadata" WHERE column_name = ?')
      .get('tags') as Record<string, unknown>;
    expect(tags.is_array).toBe(1);
  });

  test('update column metadata', () => {
    const db = createTestDb();
    insertColumnMetadata(db, 'users', 'name', {
      upstreamType: 'varchar',
      isNotNull: false,
      isEnum: false,
      isArray: false,
    });

    updateColumnMetadata(db, 'users', 'name', 'full_name', {
      upstreamType: 'varchar',
      isNotNull: true,
      isEnum: false,
      isArray: false,
      characterMaxLength: 200,
    });

    const row = db
      .prepare(
        'SELECT * FROM "_zero.column_metadata" WHERE table_name = ? AND column_name = ?',
      )
      .get('users', 'full_name') as Record<string, unknown>;

    expect(row.column_name).toBe('full_name');
    expect(row.is_not_null).toBe(1);
    expect(row.character_max_length).toBe(200);
  });

  test('delete column metadata', () => {
    const db = createTestDb();
    insertColumnMetadata(db, 'users', 'id', {
      upstreamType: 'int8',
      isNotNull: false,
      isEnum: false,
      isArray: false,
    });
    insertColumnMetadata(db, 'users', 'name', {
      upstreamType: 'varchar',
      isNotNull: false,
      isEnum: false,
      isArray: false,
    });

    deleteColumnMetadata(db, 'users', 'name');

    const rows = db
      .prepare('SELECT * FROM "_zero.column_metadata" WHERE table_name = ?')
      .all('users');
    expect(rows).toHaveLength(1);
  });

  test('delete and rename table metadata', () => {
    const db = createTestDb();
    insertColumnMetadata(db, 'users', 'id', {
      upstreamType: 'int8',
      isNotNull: false,
      isEnum: false,
      isArray: false,
    });
    insertColumnMetadata(db, 'posts', 'id', {
      upstreamType: 'int8',
      isNotNull: false,
      isEnum: false,
      isArray: false,
    });

    renameTableMetadata(db, 'users', 'people');
    expect(
      db
        .prepare('SELECT * FROM "_zero.column_metadata" WHERE table_name = ?')
        .all('people'),
    ).toHaveLength(1);

    deleteTableMetadata(db, 'people');
    expect(
      db
        .prepare('SELECT * FROM "_zero.column_metadata" WHERE table_name = ?')
        .all('people'),
    ).toHaveLength(0);
    expect(
      db
        .prepare('SELECT * FROM "_zero.column_metadata" WHERE table_name = ?')
        .all('posts'),
    ).toHaveLength(1);
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

  describe('populateColumnMetadataFromExistingTables', () => {
    test('populates metadata from LiteTableSpec array', () => {
      const db = createTestDb();

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

      populateColumnMetadataFromExistingTables(db, tables);

      const allRows = db
        .prepare(
          'SELECT * FROM "_zero.column_metadata" ORDER BY table_name, column_name',
        )
        .all() as Array<Record<string, unknown>>;

      expect(allRows).toHaveLength(5);

      // Check users table
      const usersId = allRows.find(
        r => r.table_name === 'users' && r.column_name === 'id',
      );
      expect(usersId).toEqual({
        table_name: 'users',
        column_name: 'id',
        upstream_type: 'int8',
        is_not_null: 1,
        is_enum: 0,
        is_array: 0,
        character_max_length: null,
      });

      const usersEmail = allRows.find(
        r => r.table_name === 'users' && r.column_name === 'email',
      );
      expect(usersEmail?.character_max_length).toBe(255);

      const usersTags = allRows.find(
        r => r.table_name === 'users' && r.column_name === 'tags',
      );
      expect(usersTags?.is_array).toBe(1);

      // Check posts table
      const postsStatus = allRows.find(
        r => r.table_name === 'posts' && r.column_name === 'status',
      );
      expect(postsStatus?.is_enum).toBe(1);
      expect(postsStatus?.is_not_null).toBe(1);
    });

    test('handles empty table list', () => {
      const db = createTestDb();

      populateColumnMetadataFromExistingTables(db, []);

      const rows = db.prepare('SELECT * FROM "_zero.column_metadata"').all();
      expect(rows).toHaveLength(0);
    });

    test('handles table with no columns', () => {
      const db = createTestDb();

      const tables: LiteTableSpec[] = [
        {
          name: 'empty_table',
          columns: {},
        },
      ];

      populateColumnMetadataFromExistingTables(db, tables);

      const rows = db.prepare('SELECT * FROM "_zero.column_metadata"').all();
      expect(rows).toHaveLength(0);
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

    test('preserves character max length through round-trip conversion', () => {
      const db = createTestDb();

      // Insert metadata with character max length
      insertColumnMetadata(db, 'users', 'email', {
        upstreamType: 'varchar',
        isNotNull: false,
        isEnum: false,
        isArray: false,
        characterMaxLength: 255,
      });

      // Read it back
      const row = db
        .prepare(
          'SELECT * FROM "_zero.column_metadata" WHERE table_name = ? AND column_name = ?',
        )
        .get('users', 'email') as Record<string, unknown>;

      expect(row.character_max_length).toBe(255);

      // Convert to LiteTypeString and back
      const liteType = liteTypeStringToMetadata('varchar', 255);
      expect(liteType.characterMaxLength).toBe(255);
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

    test('round-trip conversion preserves all metadata', () => {
      const original = {
        upstreamType: 'varchar',
        isNotNull: true,
        isEnum: false,
        isArray: false,
        characterMaxLength: 100,
      };

      const db = createTestDb();
      insertColumnMetadata(db, 'test', 'col', original);

      const row = db
        .prepare(
          'SELECT * FROM "_zero.column_metadata" WHERE table_name = ? AND column_name = ?',
        )
        .get('test', 'col') as {
        upstream_type: string;
        is_not_null: number;
        is_enum: number;
        is_array: number;
        character_max_length: number | null;
      };

      const reconstructed = {
        upstreamType: row.upstream_type,
        isNotNull: row.is_not_null !== 0,
        isEnum: row.is_enum !== 0,
        isArray: row.is_array !== 0,
        characterMaxLength: row.character_max_length,
      };

      expect(reconstructed).toEqual(original);
    });
  });
});
