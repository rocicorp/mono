import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {
  ColumnMetadataStore,
  CREATE_COLUMN_METADATA_TABLE,
} from '../services/replicator/schema/column-metadata.ts';
import {computeZqlSpecs, listIndexes, listTables} from './lite-tables.ts';
import type {LiteIndexSpec, LiteTableSpec} from './specs.ts';

describe('lite/tables', () => {
  type Case = {
    name: string;
    setupQuery: string;
    expectedResult: LiteTableSpec[];
  };

  const cases: Case[] = [
    {
      name: 'No tables',
      setupQuery: ``,
      expectedResult: [],
    },
    {
      name: 'zero.clients',
      setupQuery: `
      CREATE TABLE "zero.clients" (
        "clientID" VARCHAR (180) PRIMARY KEY,
        "lastMutationID" BIGINT
      );
      `,
      expectedResult: [
        {
          name: 'zero.clients',
          columns: {
            clientID: {
              pos: 1,
              dataType: 'VARCHAR (180)',
              characterMaximumLength: null,
              elemPgTypeClass: null,
              notNull: false,
              dflt: null,
            },
            lastMutationID: {
              pos: 2,
              dataType: 'BIGINT',
              characterMaximumLength: null,
              elemPgTypeClass: null,
              notNull: false,
              dflt: null,
            },
          },
          primaryKey: ['clientID'],
        },
      ],
    },
    {
      name: 'types and array types',
      setupQuery: `
      CREATE TABLE users (
        user_id INTEGER PRIMARY KEY,
        handle text DEFAULT 'foo',
        address text[],
        bigint BIGINT DEFAULT '2147483648',
        bool_array BOOL[],
        real_array REAL[],
        int_array INTEGER[] DEFAULT '{1, 2, 3}',
        json_val JSONB,
        time_value TIME,
        time_array TIME[]
      );
      `,
      expectedResult: [
        {
          name: 'users',
          columns: {
            ['user_id']: {
              pos: 1,
              dataType: 'INTEGER',
              elemPgTypeClass: null,
              characterMaximumLength: null,
              notNull: false,
              dflt: null,
            },
            handle: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'TEXT',
              elemPgTypeClass: null,
              notNull: false,
              dflt: "'foo'",
            },
            address: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'text[]',
              elemPgTypeClass: 'b',
              notNull: false,
              dflt: null,
            },
            bigint: {
              pos: 4,
              characterMaximumLength: null,
              dataType: 'BIGINT',
              elemPgTypeClass: null,
              notNull: false,
              dflt: "'2147483648'",
            },
            ['bool_array']: {
              pos: 5,
              characterMaximumLength: null,
              dataType: 'BOOL[]',
              elemPgTypeClass: 'b',
              notNull: false,
              dflt: null,
            },
            ['real_array']: {
              pos: 6,
              characterMaximumLength: null,
              dataType: 'REAL[]',
              elemPgTypeClass: 'b',
              notNull: false,
              dflt: null,
            },
            ['int_array']: {
              pos: 7,
              dataType: 'INTEGER[]',
              characterMaximumLength: null,
              elemPgTypeClass: 'b',
              notNull: false,
              dflt: "'{1, 2, 3}'",
            },
            ['json_val']: {
              pos: 8,
              dataType: 'JSONB',
              characterMaximumLength: null,
              elemPgTypeClass: null,
              notNull: false,
              dflt: null,
            },
            ['time_value']: {
              pos: 9,
              dataType: 'TIME',
              characterMaximumLength: null,
              elemPgTypeClass: null,
              notNull: false,
              dflt: null,
            },
            ['time_array']: {
              pos: 10,
              dataType: 'TIME[]',
              characterMaximumLength: null,
              elemPgTypeClass: 'b',
              notNull: false,
              dflt: null,
            },
          },
          primaryKey: ['user_id'],
        },
      ],
    },
    {
      name: 'primary key columns (ignored)',
      setupQuery: `
      CREATE TABLE issues (
        issue_id INTEGER,
        description TEXT,
        org_id INTEGER NOT NULL,
        component_id INTEGER,
        PRIMARY KEY (org_id, component_id, issue_id)
      );
      `,
      expectedResult: [
        {
          name: 'issues',
          columns: {
            ['issue_id']: {
              pos: 1,
              dataType: 'INTEGER',
              characterMaximumLength: null,
              elemPgTypeClass: null,
              notNull: false,
              dflt: null,
            },
            ['description']: {
              pos: 2,
              dataType: 'TEXT',
              characterMaximumLength: null,
              elemPgTypeClass: null,
              notNull: false,
              dflt: null,
            },
            ['org_id']: {
              pos: 3,
              dataType: 'INTEGER',
              characterMaximumLength: null,
              elemPgTypeClass: null,
              notNull: true,
              dflt: null,
            },
            ['component_id']: {
              pos: 4,
              dataType: 'INTEGER',
              characterMaximumLength: null,
              elemPgTypeClass: null,
              notNull: false,
              dflt: null,
            },
          },
          primaryKey: ['org_id', 'component_id', 'issue_id'],
        },
      ],
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      const db = new Database(createSilentLogContext(), ':memory:');
      db.exec(c.setupQuery);

      const tables = listTables(db);
      expect(tables).toEqual(c.expectedResult);
    });
  }
});

describe('lite/indexes', () => {
  type Case = {
    name: string;
    setupQuery: string;
    expectedResult: LiteIndexSpec[];
  };

  const cases: Case[] = [
    {
      name: 'primary key',
      setupQuery: `
    CREATE TABLE "zero.clients" (
      "clientID" VARCHAR (180) PRIMARY KEY,
      "lastMutationID" BIGINT
    );
    `,
      expectedResult: [
        {
          name: 'sqlite_autoindex_zero.clients_1',
          tableName: 'zero.clients',
          unique: true,
          columns: {clientID: 'ASC'},
        },
      ],
    },
    {
      name: 'unique',
      setupQuery: `
    CREATE TABLE users (
      userID VARCHAR (180) PRIMARY KEY,
      handle TEXT UNIQUE
    );
    `,
      expectedResult: [
        {
          name: 'sqlite_autoindex_users_1',
          tableName: 'users',
          unique: true,
          columns: {userID: 'ASC'},
        },
        {
          name: 'sqlite_autoindex_users_2',
          tableName: 'users',
          unique: true,
          columns: {handle: 'ASC'},
        },
      ],
    },
    {
      name: 'multiple columns',
      setupQuery: `
    CREATE TABLE users (
      userID VARCHAR (180) PRIMARY KEY,
      first TEXT,
      last TEXT,
      handle TEXT UNIQUE
    );
    CREATE INDEX full_name ON users (last desc, first);
    `,
      expectedResult: [
        {
          name: 'full_name',
          tableName: 'users',
          unique: false,
          columns: {
            last: 'DESC',
            first: 'ASC',
          },
        },
        {
          name: 'sqlite_autoindex_users_1',
          tableName: 'users',
          unique: true,
          columns: {userID: 'ASC'},
        },
        {
          name: 'sqlite_autoindex_users_2',
          tableName: 'users',
          unique: true,
          columns: {handle: 'ASC'},
        },
      ],
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      const db = new Database(createSilentLogContext(), ':memory:');
      db.exec(c.setupQuery);

      const tables = listIndexes(db);
      expect(tables).toEqual(c.expectedResult);
    });
  }
});

describe('computeZqlSpec', () => {
  function t(setup: string) {
    const db = new Database(createSilentLogContext(), ':memory:');
    db.exec(setup);
    return [...computeZqlSpecs(createSilentLogContext(), db).values()];
  }

  test('plain primary key', () => {
    expect(
      t(`
    CREATE TABLE nopk(a INT, b INT, c INT, d INT);
    CREATE TABLE foo(a INT, b "INT|NOT_NULL", c INT, d INT);
    CREATE UNIQUE INDEX foo_pkey ON foo(b ASC);
    `),
    ).toMatchInlineSnapshot(`
      [
        {
          "tableSpec": {
            "allPotentialPrimaryKeys": [
              [
                "b",
              ],
            ],
            "columns": {
              "a": {
                "characterMaximumLength": null,
                "dataType": "INT",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 1,
              },
              "b": {
                "characterMaximumLength": null,
                "dataType": "INT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 2,
              },
              "c": {
                "characterMaximumLength": null,
                "dataType": "INT",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 3,
              },
              "d": {
                "characterMaximumLength": null,
                "dataType": "INT",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 4,
              },
            },
            "name": "foo",
            "primaryKey": [
              "b",
            ],
            "uniqueKeys": [
              [
                "b",
              ],
            ],
          },
          "zqlSpec": {
            "a": {
              "type": "number",
            },
            "b": {
              "type": "number",
            },
            "c": {
              "type": "number",
            },
            "d": {
              "type": "number",
            },
          },
        },
      ]
    `);
  });

  test('unsupported columns (MACADDR8) are excluded', () => {
    expect(
      t(`
    CREATE TABLE foo(a INT, b "TEXT|NOT_NULL", c MACADDR8, d BYTEA);
    CREATE UNIQUE INDEX foo_pkey ON foo(b ASC);
    `),
    ).toMatchInlineSnapshot(`
      [
        {
          "tableSpec": {
            "allPotentialPrimaryKeys": [
              [
                "b",
              ],
            ],
            "columns": {
              "a": {
                "characterMaximumLength": null,
                "dataType": "INT",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 1,
              },
              "b": {
                "characterMaximumLength": null,
                "dataType": "TEXT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 2,
              },
            },
            "name": "foo",
            "primaryKey": [
              "b",
            ],
            "uniqueKeys": [
              [
                "b",
              ],
            ],
          },
          "zqlSpec": {
            "a": {
              "type": "number",
            },
            "b": {
              "type": "string",
            },
          },
        },
      ]
    `);
  });

  test('indexes with unsupported columns (MACADDR8) are excluded', () => {
    expect(
      t(`
    CREATE TABLE foo(a "INT|NOT_NULL", b "TEXT|NOT_NULL", c "MACADDR8|NOT_NULL", d "TEXT|NOT_NULL");
    CREATE UNIQUE INDEX foo_pkey ON foo(a ASC, c DESC);
    CREATE UNIQUE INDEX foo_other_key ON foo(b ASC, d ASC, a DESC);
    `),
    ).toMatchInlineSnapshot(`
      [
        {
          "tableSpec": {
            "allPotentialPrimaryKeys": [
              [
                "a",
                "b",
                "d",
              ],
            ],
            "columns": {
              "a": {
                "characterMaximumLength": null,
                "dataType": "INT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 1,
              },
              "b": {
                "characterMaximumLength": null,
                "dataType": "TEXT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 2,
              },
              "d": {
                "characterMaximumLength": null,
                "dataType": "TEXT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 4,
              },
            },
            "name": "foo",
            "primaryKey": [
              "a",
              "b",
              "d",
            ],
            "uniqueKeys": [
              [
                "a",
                "b",
                "d",
              ],
              [
                "a",
                "c",
              ],
            ],
          },
          "zqlSpec": {
            "a": {
              "type": "number",
            },
            "b": {
              "type": "string",
            },
            "d": {
              "type": "string",
            },
          },
        },
      ]
    `);
  });

  test('indexes with nullable columns are excluded', () => {
    expect(
      t(`
    CREATE TABLE foo(a "INT|NOT_NULL", b "TEXT|NOT_NULL", c TEXT, d "TEXT|NOT_NULL");
    CREATE UNIQUE INDEX foo_pkey ON foo(a ASC, c DESC);
    CREATE UNIQUE INDEX foo_other_key ON foo(b ASC, d ASC, a DESC);
    `),
    ).toMatchInlineSnapshot(`
      [
        {
          "tableSpec": {
            "allPotentialPrimaryKeys": [
              [
                "a",
                "b",
                "d",
              ],
            ],
            "columns": {
              "a": {
                "characterMaximumLength": null,
                "dataType": "INT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 1,
              },
              "b": {
                "characterMaximumLength": null,
                "dataType": "TEXT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 2,
              },
              "c": {
                "characterMaximumLength": null,
                "dataType": "TEXT",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 3,
              },
              "d": {
                "characterMaximumLength": null,
                "dataType": "TEXT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 4,
              },
            },
            "name": "foo",
            "primaryKey": [
              "a",
              "b",
              "d",
            ],
            "uniqueKeys": [
              [
                "a",
                "b",
                "d",
              ],
              [
                "a",
                "c",
              ],
            ],
          },
          "zqlSpec": {
            "a": {
              "type": "number",
            },
            "b": {
              "type": "string",
            },
            "c": {
              "type": "string",
            },
            "d": {
              "type": "string",
            },
          },
        },
      ]
    `);
  });

  test('compound key is sorted', () => {
    expect(
      t(`
    CREATE TABLE foo(a "INT|NOT_NULL", b "INT|NOT_NULL", c "INT|NOT_NULL", d "INT|NOT_NULL");
    CREATE UNIQUE INDEX foo_pkey ON foo(d ASC, a ASC, c ASC);
    `),
    ).toMatchInlineSnapshot(`
      [
        {
          "tableSpec": {
            "allPotentialPrimaryKeys": [
              [
                "a",
                "c",
                "d",
              ],
            ],
            "columns": {
              "a": {
                "characterMaximumLength": null,
                "dataType": "INT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 1,
              },
              "b": {
                "characterMaximumLength": null,
                "dataType": "INT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 2,
              },
              "c": {
                "characterMaximumLength": null,
                "dataType": "INT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 3,
              },
              "d": {
                "characterMaximumLength": null,
                "dataType": "INT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 4,
              },
            },
            "name": "foo",
            "primaryKey": [
              "a",
              "c",
              "d",
            ],
            "uniqueKeys": [
              [
                "a",
                "c",
                "d",
              ],
            ],
          },
          "zqlSpec": {
            "a": {
              "type": "number",
            },
            "b": {
              "type": "number",
            },
            "c": {
              "type": "number",
            },
            "d": {
              "type": "number",
            },
          },
        },
      ]
    `);
  });

  test('additional unique key', () => {
    expect(
      t(`
    CREATE TABLE foo(a "INT|NOT_NULL", b "INT|NOT_NULL", c "INT|NOT_NULL", d INT);
    CREATE UNIQUE INDEX foo_pkey ON foo(b ASC);
    CREATE UNIQUE INDEX foo_unique_key ON foo(c ASC, a DESC);
    `),
    ).toMatchInlineSnapshot(`
      [
        {
          "tableSpec": {
            "allPotentialPrimaryKeys": [
              [
                "b",
              ],
              [
                "a",
                "c",
              ],
            ],
            "columns": {
              "a": {
                "characterMaximumLength": null,
                "dataType": "INT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 1,
              },
              "b": {
                "characterMaximumLength": null,
                "dataType": "INT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 2,
              },
              "c": {
                "characterMaximumLength": null,
                "dataType": "INT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 3,
              },
              "d": {
                "characterMaximumLength": null,
                "dataType": "INT",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 4,
              },
            },
            "name": "foo",
            "primaryKey": [
              "b",
            ],
            "uniqueKeys": [
              [
                "b",
              ],
              [
                "a",
                "c",
              ],
            ],
          },
          "zqlSpec": {
            "a": {
              "type": "number",
            },
            "b": {
              "type": "number",
            },
            "c": {
              "type": "number",
            },
            "d": {
              "type": "number",
            },
          },
        },
      ]
    `);
  });

  test('shorter key is chosen over primary key', () => {
    expect(
      t(`
    CREATE TABLE foo(a INT, b "INT|NOT_NULL", c "INT|NOT_NULL", d "INT|NOT_NULL");
    CREATE UNIQUE INDEX foo_pkey ON foo(b ASC, d DESC);
    CREATE UNIQUE INDEX foo_z_key ON foo(c ASC);
    `),
    ).toMatchInlineSnapshot(`
      [
        {
          "tableSpec": {
            "allPotentialPrimaryKeys": [
              [
                "c",
              ],
              [
                "b",
                "d",
              ],
            ],
            "columns": {
              "a": {
                "characterMaximumLength": null,
                "dataType": "INT",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 1,
              },
              "b": {
                "characterMaximumLength": null,
                "dataType": "INT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 2,
              },
              "c": {
                "characterMaximumLength": null,
                "dataType": "INT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 3,
              },
              "d": {
                "characterMaximumLength": null,
                "dataType": "INT|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 4,
              },
            },
            "name": "foo",
            "primaryKey": [
              "c",
            ],
            "uniqueKeys": [
              [
                "b",
                "d",
              ],
              [
                "c",
              ],
            ],
          },
          "zqlSpec": {
            "a": {
              "type": "number",
            },
            "b": {
              "type": "number",
            },
            "c": {
              "type": "number",
            },
            "d": {
              "type": "number",
            },
          },
        },
      ]
    `);
  });

  test('unique constraints', () => {
    expect(
      t(/*sql*/ `
      CREATE TABLE "funk" (
          "id" "text|NOT_NULL",
          "name" "varchar|NOT_NULL",
          "order" "integer|NOT_NULL",
          "createdAt" "timestamp|NOT_NULL",
          "updatedAt" "timestamp|NOT_NULL",
          "title" "text"
      );
      CREATE UNIQUE INDEX funk_name_unique ON funk (name ASC);
      CREATE UNIQUE INDEX funk_order_unique ON funk ("order" ASC);
      CREATE UNIQUE INDEX funk_pkey ON funk (id ASC);
      CREATE UNIQUE INDEX funk_title_unique ON funk (title ASC);
      CREATE UNIQUE INDEX funk_name_title_unique ON funk (name ASC, title ASC);
    `),
    ).toMatchInlineSnapshot(`
      [
        {
          "tableSpec": {
            "allPotentialPrimaryKeys": [
              [
                "id",
              ],
              [
                "name",
              ],
              [
                "order",
              ],
            ],
            "columns": {
              "createdAt": {
                "characterMaximumLength": null,
                "dataType": "timestamp|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 4,
              },
              "id": {
                "characterMaximumLength": null,
                "dataType": "text|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 1,
              },
              "name": {
                "characterMaximumLength": null,
                "dataType": "varchar|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 2,
              },
              "order": {
                "characterMaximumLength": null,
                "dataType": "integer|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 3,
              },
              "title": {
                "characterMaximumLength": null,
                "dataType": "TEXT",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 6,
              },
              "updatedAt": {
                "characterMaximumLength": null,
                "dataType": "timestamp|NOT_NULL",
                "dflt": null,
                "elemPgTypeClass": null,
                "notNull": false,
                "pos": 5,
              },
            },
            "name": "funk",
            "primaryKey": [
              "id",
            ],
            "uniqueKeys": [
              [
                "name",
                "title",
              ],
              [
                "name",
              ],
              [
                "order",
              ],
              [
                "id",
              ],
              [
                "title",
              ],
            ],
          },
          "zqlSpec": {
            "createdAt": {
              "type": "number",
            },
            "id": {
              "type": "string",
            },
            "name": {
              "type": "string",
            },
            "order": {
              "type": "number",
            },
            "title": {
              "type": "string",
            },
            "updatedAt": {
              "type": "number",
            },
          },
        },
      ]
    `);
  });
});

describe('metadata table integration', () => {
  test('fallback: reads from column types when metadata table does not exist', () => {
    const db = new Database(createSilentLogContext(), ':memory:');

    // Create table with pipe notation in column types
    db.exec(`
      CREATE TABLE users (
        id "int8|NOT_NULL" PRIMARY KEY,
        name "varchar",
        tags "text[]|NOT_NULL",
        status "user_status|TEXT_ENUM"
      );
    `);

    // Metadata table doesn't exist, should read from column types
    const tables = listTables(db);

    expect(tables).toHaveLength(1);
    expect(tables[0].columns.id.dataType).toBe('int8|NOT_NULL');
    expect(tables[0].columns.name.dataType).toBe('varchar');
    expect(tables[0].columns.tags.dataType).toBe('text[]|NOT_NULL');
    expect(tables[0].columns.status.dataType).toBe('user_status|TEXT_ENUM');
  });

  test('reads from metadata table when available', () => {
    const db = new Database(createSilentLogContext(), ':memory:');

    // Create metadata table
    db.exec(CREATE_COLUMN_METADATA_TABLE);

    // Create table with plain SQLite types (no pipe notation)
    db.exec(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT,
        tags TEXT,
        status TEXT
      );
    `);

    // Populate metadata table
    const store = ColumnMetadataStore.getInstance(db);
    expect(store).toBeDefined();

    must(store).insert('users', 'id', {
      pos: 0,
      dataType: 'int8',
      notNull: true,
    });

    must(store).insert('users', 'name', {
      pos: 1,
      dataType: 'varchar',
    });

    must(store).insert('users', 'tags', {
      pos: 2,
      dataType: 'text[]',
      notNull: true,
      elemPgTypeClass: 'b',
    });

    must(store).insert('users', 'status', {
      pos: 3,
      dataType: 'user_status',
      pgTypeClass: 'e',
    });

    // Should read from metadata table
    const tables = listTables(db);

    expect(tables).toHaveLength(1);
    expect(tables[0].columns.id.dataType).toBe('int8|NOT_NULL');
    expect(tables[0].columns.name.dataType).toBe('varchar');
    expect(tables[0].columns.tags.dataType).toBe('text[]|NOT_NULL|TEXT_ARRAY');
    expect(tables[0].columns.status.dataType).toBe('user_status|TEXT_ENUM');
    expect(tables[0].columns.tags.elemPgTypeClass).toBe('b');
    expect(tables[0].columns.status.elemPgTypeClass).toBe(null);
  });

  test('partial metadata: reads from metadata table for some columns, fallback for others', () => {
    const db = new Database(createSilentLogContext(), ':memory:');

    // Create metadata table
    db.exec(CREATE_COLUMN_METADATA_TABLE);

    // Create table with pipe notation
    db.exec(`
      CREATE TABLE users (
        id "int8|NOT_NULL" PRIMARY KEY,
        name "varchar",
        email "varchar"
      );
    `);

    // Populate metadata for only some columns
    const store = ColumnMetadataStore.getInstance(db);
    expect(store).toBeDefined();

    // Only add metadata for 'id' and 'name', not 'email'
    must(store).insert('users', 'id', {
      pos: 0,
      dataType: 'int4', // Different from column type!
      notNull: true,
    });

    must(store).insert('users', 'name', {
      pos: 1,
      dataType: 'text', // Different from column type!
      notNull: true,
    });

    const tables = listTables(db);

    expect(tables).toHaveLength(1);
    // These should come from metadata table
    expect(tables[0].columns.id.dataType).toBe('int4|NOT_NULL');
    expect(tables[0].columns.name.dataType).toBe('text|NOT_NULL');
    // This should fall back to column type
    expect(tables[0].columns.email.dataType).toBe('varchar');
  });

  test('enum arrays with metadata table', () => {
    const db = new Database(createSilentLogContext(), ':memory:');

    // Create metadata table
    db.exec(CREATE_COLUMN_METADATA_TABLE);

    // Create table
    db.exec(`
      CREATE TABLE posts (
        id INTEGER PRIMARY KEY,
        statuses TEXT
      );
    `);

    // Populate metadata for enum array
    const store = ColumnMetadataStore.getInstance(db);
    must(store).insert('posts', 'id', {
      pos: 0,
      dataType: 'int8',
      notNull: true,
    });

    must(store).insert('posts', 'statuses', {
      pos: 1,
      dataType: 'status[]',
      notNull: true,
      pgTypeClass: 'e',
      elemPgTypeClass: 'e',
    });

    const tables = listTables(db);

    expect(tables).toHaveLength(1);
    expect(tables[0].columns.statuses.dataType).toBe(
      'status[]|NOT_NULL|TEXT_ENUM|TEXT_ARRAY',
    );
    expect(tables[0].columns.statuses.elemPgTypeClass).toBe('e');
  });
});
