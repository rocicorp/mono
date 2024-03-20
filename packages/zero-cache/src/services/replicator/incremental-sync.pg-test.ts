import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import type postgres from 'postgres';
import {expectTables, initDB, testDBs} from '../../test/db.js';
import {createSilentLogContext} from '../../test/logger.js';
import {setupReplicationTables} from './incremental-sync.js';
import {getPublicationInfo} from './tables/published.js';
import type {TableSpec} from './tables/specs.js';

describe('replicator/incremental-sync', () => {
  let db: postgres.Sql;

  beforeEach(async () => {
    db = await testDBs.create('incremental_sync_test');
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  type Case = {
    name: string;
    setupQuery?: string;
    specs: Record<string, TableSpec>;
    data: Record<string, object[]>;
  };

  const cases: Case[] = [
    {
      name: 'create tables',
      specs: {},
      data: {
        ['_zero.tx_log']: [],
        ['_zero.change_log']: [],
        ['_zero.invalidation_registry']: [],
        ['_zero.invalidation_index']: [],
      },
    },
    {
      name: 'alter version columns',
      setupQuery: `
      CREATE TABLE issues(
        issue_id INTEGER PRIMARY KEY,
        _0_version VARCHAR(38) DEFAULT '00'
      );
      CREATE PUBLICATION zero_data FOR TABLES IN SCHEMA public;

      CREATE SCHEMA zero;
      CREATE TABLE zero.clients(
        client_id TEXT PRIMARY KEY,
        last_mutation_id TEXT,
        _0_version VARCHAR(38) DEFAULT '00'
      );
      CREATE PUBLICATION zero_meta FOR TABLES IN SCHEMA zero;
      `,
      specs: {
        ['public.issues']: {
          schema: 'public',
          name: 'issues',
          columns: {
            ['issue_id']: {
              dataType: 'int4',
              characterMaximumLength: null,
              columnDefault: null,
            },
            ['_0_version']: {
              dataType: 'varchar',
              characterMaximumLength: 38,
              columnDefault: null, // Default should be cleared.
            },
          },
          primaryKey: ['issue_id'],
        },
        ['zero.clients']: {
          schema: 'zero',
          name: 'clients',
          columns: {
            ['client_id']: {
              dataType: 'text',
              characterMaximumLength: null,
              columnDefault: null,
            },
            ['last_mutation_id']: {
              dataType: 'text',
              characterMaximumLength: null,
              columnDefault: null,
            },
            ['_0_version']: {
              dataType: 'varchar',
              characterMaximumLength: 38,
              columnDefault: null, // Default should be cleared.
            },
          },
          primaryKey: ['client_id'],
        },
      },
      data: {
        ['_zero.tx_log']: [],
        ['_zero.change_log']: [],
        ['_zero.invalidation_registry']: [],
        ['_zero.invalidation_index']: [],
      },
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      await initDB(db, c.setupQuery);
      await db.begin(tx =>
        setupReplicationTables(
          createSilentLogContext(),
          'replica id',
          tx,
          'postresql:///unused',
        ),
      );

      const published = await getPublicationInfo(db, 'zero_');
      expect(published.tables).toEqual(c.specs);
      await expectTables(db, c.data);
    });
  }
});
