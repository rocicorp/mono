import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import postgres from 'postgres';
import {introspect} from './introspector.ts';
import {getConnectionString} from './test/db.ts';

describe('introspector', () => {
  let sql: postgres.Sql;
  const TEST_SCHEMA = 'sql_to_zql_test';

  beforeAll(async () => {
    sql = postgres(getConnectionString());

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

    // Task -> User FK with SET NULL
    const taskAssigneeFK = result.foreignKeys.find(
      fk =>
        fk.sourceTable === 'tasks' && fk.sourceColumns.includes('assignee_id'),
    );
    expect(taskAssigneeFK).toMatchObject({
      onDelete: 'SET NULL',
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

  test('works with connection string', async () => {
    const result = await introspect({
      connectionString: getConnectionString(),
      schema: TEST_SCHEMA,
    });

    expect(result.tables).toHaveLength(4);
  });

  test('throws error when no connection info provided', async () => {
    await expect(introspect({schema: TEST_SCHEMA})).rejects.toThrow(
      'Either connectionString or client must be provided',
    );
  });
});
