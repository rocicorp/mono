import {describe, expect, test} from 'vitest';
import {Database} from '../../../../zqlite/src/db.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {schemaVersionMigrationMap} from './replica-schema.ts';
import {hasColumnMetadataTable, getTableMetadata} from './column-metadata.ts';

describe('replica-schema/migration-v6', () => {
  const migration = schemaVersionMigrationMap[6]!;

  test('creates metadata table when none exists', () => {
    const lc = createSilentLogContext();
    const db = new Database(lc, ':memory:');

    // Run the migration v6 schema part
    migration.migrateSchema!(lc, db);

    expect(hasColumnMetadataTable(db)).toBe(true);

    // Verify table structure
    const schema = db
      .prepare(
        `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '_zero.column_metadata'`,
      )
      .get() as {sql: string} | undefined;

    expect(schema).toBeDefined();
    expect(schema!.sql).toContain('table_name TEXT NOT NULL');
    expect(schema!.sql).toContain('column_name TEXT NOT NULL');
    expect(schema!.sql).toContain('upstream_type TEXT NOT NULL');
    expect(schema!.sql).toContain('is_not_null INTEGER NOT NULL');
    expect(schema!.sql).toContain('is_enum INTEGER NOT NULL');
    expect(schema!.sql).toContain('is_array INTEGER NOT NULL');
    expect(schema!.sql).toContain('character_max_length INTEGER');
  });

  test('populates metadata from existing tables with pipe notation', () => {
    const lc = createSilentLogContext();
    const db = new Database(lc, ':memory:');

    // Create tables with old-style pipe-delimited notation
    db.exec(`
      CREATE TABLE users (
        id "int8|NOT_NULL" PRIMARY KEY,
        email "varchar" NOT NULL,
        role "user_role|TEXT_ENUM",
        tags "text[]",
        _0_version TEXT
      );

      CREATE TABLE posts (
        id "int8|NOT_NULL" PRIMARY KEY,
        title TEXT,
        counts "int4|NOT_NULL[]",
        _0_version TEXT
      );
    `);

    // Create the metadata table
    migration.migrateSchema!(lc, db);

    // Populate metadata from existing tables
    if (migration.migrateData) {
      migration.migrateData(lc, db);
    }

    // Verify users table metadata
    const usersMetadata = getTableMetadata(db, 'users');
    expect(usersMetadata.get('id')).toEqual({
      upstreamType: 'int8',
      isNotNull: true,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
    });

    expect(usersMetadata.get('email')).toEqual({
      upstreamType: 'varchar',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
    });

    expect(usersMetadata.get('role')).toEqual({
      upstreamType: 'user_role',
      isNotNull: false,
      isEnum: true,
      isArray: false,
      characterMaxLength: null,
    });

    expect(usersMetadata.get('tags')).toEqual({
      upstreamType: 'text[]',
      isNotNull: false,
      isEnum: false,
      isArray: true,
      characterMaxLength: null,
    });

    // Verify posts table metadata
    const postsMetadata = getTableMetadata(db, 'posts');
    expect(postsMetadata.get('id')).toEqual({
      upstreamType: 'int8',
      isNotNull: true,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
    });

    // Check the old-style array format: 'int4|NOT_NULL[]'
    // After conversion, this becomes 'int4[]' with isNotNull=true, isArray=true
    expect(postsMetadata.get('counts')).toEqual({
      upstreamType: 'int4[]',
      isNotNull: true,
      isEnum: false,
      isArray: true,
      characterMaxLength: null,
    });
  });

  test('handles empty database', () => {
    const lc = createSilentLogContext();
    const db = new Database(lc, ':memory:');

    // Run migration on empty database
    migration.migrateSchema!(lc, db);
    if (migration.migrateData) {
      migration.migrateData(lc, db);
    }

    // Metadata table should exist but be empty
    expect(hasColumnMetadataTable(db)).toBe(true);

    const rowCount = db
      .prepare('SELECT COUNT(*) as count FROM "_zero.column_metadata"')
      .get() as {count: number};

    expect(rowCount.count).toBe(0);
  });

  test('handles various column types during population', () => {
    const lc = createSilentLogContext();
    const db = new Database(lc, ':memory:');

    // Create table with various types
    db.exec(`
      CREATE TABLE test_types (
        col_int8 "int8|NOT_NULL",
        col_varchar "varchar",
        col_text TEXT,
        col_bool BOOL,
        col_real REAL,
        col_json JSON,
        col_enum "status|TEXT_ENUM",
        col_array_simple "int4[]",
        col_array_notnull "int4|NOT_NULL[]",
        col_array_enum "role[]|TEXT_ENUM",
        _0_version TEXT
      );
    `);

    // Create metadata table and populate
    migration.migrateSchema!(lc, db);
    if (migration.migrateData) {
      migration.migrateData(lc, db);
    }

    const metadata = getTableMetadata(db, 'test_types');

    // Verify each type
    expect(metadata.get('col_int8')).toMatchObject({
      upstreamType: 'int8',
      isNotNull: true,
      isEnum: false,
      isArray: false,
    });

    expect(metadata.get('col_varchar')).toMatchObject({
      upstreamType: 'varchar',
      isNotNull: false,
      isEnum: false,
      isArray: false,
    });

    expect(metadata.get('col_text')).toMatchObject({
      upstreamType: 'TEXT',
      isNotNull: false,
      isEnum: false,
      isArray: false,
    });

    expect(metadata.get('col_enum')).toMatchObject({
      upstreamType: 'status',
      isNotNull: false,
      isEnum: true,
      isArray: false,
    });

    expect(metadata.get('col_array_simple')).toMatchObject({
      upstreamType: 'int4[]',
      isNotNull: false,
      isEnum: false,
      isArray: true,
    });

    expect(metadata.get('col_array_notnull')).toMatchObject({
      upstreamType: 'int4[]',
      isNotNull: true,
      isEnum: false,
      isArray: true,
    });

    expect(metadata.get('col_array_enum')).toMatchObject({
      upstreamType: 'role[]',
      isNotNull: false,
      isEnum: true,
      isArray: true,
    });
  });

  test('handles multiple tables during population', () => {
    const lc = createSilentLogContext();
    const db = new Database(lc, ':memory:');

    // Create multiple tables
    db.exec(`
      CREATE TABLE table1 (id "int8|NOT_NULL", name TEXT, _0_version TEXT);
      CREATE TABLE table2 (id "int8|NOT_NULL", email TEXT, _0_version TEXT);
      CREATE TABLE table3 (id "int8|NOT_NULL", age INTEGER, _0_version TEXT);
    `);

    // Create metadata table and populate
    migration.migrateSchema!(lc, db);
    if (migration.migrateData) {
      migration.migrateData(lc, db);
    }

    // Verify all tables have metadata
    expect(getTableMetadata(db, 'table1').size).toBeGreaterThan(0);
    expect(getTableMetadata(db, 'table2').size).toBeGreaterThan(0);
    expect(getTableMetadata(db, 'table3').size).toBeGreaterThan(0);

    // Verify total metadata count
    const totalCount = db
      .prepare('SELECT COUNT(*) as count FROM "_zero.column_metadata"')
      .get() as {count: number};

    // Each table has id, a data column, and _0_version = 3 columns × 3 tables = 9
    expect(totalCount.count).toBe(9);
  });

  test('index is created on table_name column', () => {
    const lc = createSilentLogContext();
    const db = new Database(lc, ':memory:');

    migration.migrateSchema!(lc, db);

    // Verify index exists
    const index = db
      .prepare(
        `SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_column_metadata_table'`,
      )
      .get() as {name: string} | undefined;

    expect(index).toBeDefined();
    expect(index!.name).toBe('idx_column_metadata_table');
  });
});
