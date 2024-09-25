import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {describe, expect, test} from 'vitest';
import {Database} from 'zqlite/src/db.js';
import {TableSpec} from '../types/specs.js';
import {listTables} from './lite-tables.js';

describe('tables/list', () => {
  type Case = {
    name: string;
    setupQuery: string;
    expectedResult: TableSpec[];
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
          schema: '',
          name: 'zero.clients',
          columns: {
            clientID: {
              pos: 1,
              dataType: 'VARCHAR (180)',
              characterMaximumLength: null,
              notNull: false,
            },
            lastMutationID: {
              pos: 2,
              dataType: 'BIGINT',
              characterMaximumLength: null,
              notNull: false,
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
        handle text,
        address text[],
        timez TIMESTAMPTZ[],
        bigint_array BIGINT[],
        bool_array BOOL[],
        real_array REAL[],
        int_array INTEGER[] DEFAULT '{1, 2, 3}',
        json_val JSONB
      );
      `,
      expectedResult: [
        {
          schema: '',
          name: 'users',
          columns: {
            ['user_id']: {
              pos: 1,
              dataType: 'INTEGER',
              characterMaximumLength: null,
              notNull: false,
            },
            handle: {
              pos: 2,
              characterMaximumLength: null,
              dataType: 'TEXT',
              notNull: false,
            },
            address: {
              pos: 3,
              characterMaximumLength: null,
              dataType: 'text[]',
              notNull: false,
            },
            ['timez']: {
              pos: 4,
              dataType: 'TIMESTAMPTZ[]',
              characterMaximumLength: null,
              notNull: false,
            },
            ['bigint_array']: {
              pos: 5,
              characterMaximumLength: null,
              dataType: 'BIGINT[]',
              notNull: false,
            },
            ['bool_array']: {
              pos: 6,
              characterMaximumLength: null,
              dataType: 'BOOL[]',
              notNull: false,
            },
            ['real_array']: {
              pos: 7,
              characterMaximumLength: null,
              dataType: 'REAL[]',
              notNull: false,
            },
            ['int_array']: {
              pos: 8,
              dataType: 'INTEGER[]',
              characterMaximumLength: null,
              notNull: false,
            },
            ['json_val']: {
              pos: 9,
              dataType: 'JSONB',
              characterMaximumLength: null,
              notNull: false,
            },
          },
          primaryKey: ['user_id'],
        },
      ],
    },
    {
      name: 'primary key columns',
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
          schema: '',
          name: 'issues',
          columns: {
            ['issue_id']: {
              pos: 1,
              dataType: 'INTEGER',
              characterMaximumLength: null,
              notNull: false,
            },
            ['description']: {
              pos: 2,
              dataType: 'TEXT',
              characterMaximumLength: null,
              notNull: false,
            },
            ['org_id']: {
              pos: 3,
              dataType: 'INTEGER',
              characterMaximumLength: null,
              notNull: true,
            },
            ['component_id']: {
              pos: 4,
              dataType: 'INTEGER',
              characterMaximumLength: null,
              notNull: false,
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
