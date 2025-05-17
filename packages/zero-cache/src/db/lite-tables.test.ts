import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
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
        json_val JSONB
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
              elemPgTypeClass: null,
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
              elemPgTypeClass: null,
              notNull: false,
              dflt: null,
            },
            ['real_array']: {
              pos: 6,
              characterMaximumLength: null,
              dataType: 'REAL[]',
              elemPgTypeClass: null,
              notNull: false,
              dflt: null,
            },
            ['int_array']: {
              pos: 7,
              dataType: 'INTEGER[]',
              characterMaximumLength: null,
              elemPgTypeClass: null,
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
            "unionKey": [
              "b",
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

  test('unsupported columns are excluded', () => {
    expect(
      t(`
    CREATE TABLE foo(a INT, b "TEXT|NOT_NULL", c TIME, d BYTEA);
    CREATE UNIQUE INDEX foo_pkey ON foo(b ASC);
    `),
    ).toMatchInlineSnapshot(`
      [
        {
          "tableSpec": {
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
            "unionKey": [
              "b",
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

  test('indexes with unsupported columns are excluded', () => {
    expect(
      t(`
    CREATE TABLE foo(a "INT|NOT_NULL", b "TEXT|NOT_NULL", c "TIME|NOT_NULL", d "TEXT|NOT_NULL");
    CREATE UNIQUE INDEX foo_pkey ON foo(a ASC, c DESC);
    CREATE UNIQUE INDEX foo_other_key ON foo(b ASC, d ASC, a DESC);
    `),
    ).toMatchInlineSnapshot(`
      [
        {
          "tableSpec": {
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
            "unionKey": [
              "a",
              "b",
              "d",
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
            "unionKey": [
              "a",
              "b",
              "d",
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
            "unionKey": [
              "a",
              "c",
              "d",
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
            "unionKey": [
              "a",
              "b",
              "c",
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
            "unionKey": [
              "b",
              "c",
              "d",
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
          "updatedAt" "timestamp|NOT_NULL"
      );
      CREATE UNIQUE INDEX funk_name_unique ON funk (name ASC);
      CREATE UNIQUE INDEX funk_order_unique ON funk ("order" ASC);
      CREATE UNIQUE INDEX funk_pkey ON funk (id ASC);
    `),
    ).toMatchInlineSnapshot(`
      [
        {
          "tableSpec": {
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
            "unionKey": [
              "id",
              "name",
              "order",
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
            "updatedAt": {
              "type": "number",
            },
          },
        },
      ]
    `);
  });
});
