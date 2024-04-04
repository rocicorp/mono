import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import {Lock} from '@rocicorp/lock';
import type postgres from 'postgres';
import {expectTables, initDB, testDBs} from '../../test/db.js';
import {createSilentLogContext} from '../../test/logger.js';
import {
  NormalizedInvalidationFilterSpec,
  normalizeFilterSpec,
} from '../../types/invalidation.js';
import {CREATE_REPLICATION_TABLES} from './incremental-sync.js';
import {CREATE_INVALIDATION_TABLES, Invalidator} from './invalidation.js';

describe('replicator/invalidation', () => {
  let replica: postgres.Sql;
  let invalidator: Invalidator;

  const SPEC1 = normalizeFilterSpec({
    schema: 'public',
    table: 'foo',
    filteredColumns: {id: '='},
  });

  const SPEC2 = normalizeFilterSpec({
    schema: 'public',
    table: 'foo',
    filteredColumns: {id: '='},
    selectedColumns: ['id', 'created'],
  });

  const DATE1 = new Date(Date.UTC(2024, 2, 27, 1, 2, 3));
  const DATE2 = new Date(Date.UTC(2024, 2, 1, 2, 3, 4));
  const NOW = new Date(Date.UTC(2024, 4, 1, 2, 3, 4));

  beforeEach(async () => {
    replica = await testDBs.create('invalidation_test');
    await replica.unsafe(
      `CREATE SCHEMA _zero;` +
        CREATE_INVALIDATION_TABLES +
        CREATE_REPLICATION_TABLES,
    );

    invalidator = new Invalidator(replica, new Lock());
  });

  afterEach(async () => {
    await testDBs.drop(replica);
  });

  type RegistrationTestCase = {
    name: string;
    specs: NormalizedInvalidationFilterSpec[];
    version: string;
    setup?: Record<string, Record<string, unknown>[]>;
    expected?: Record<string, Record<string, unknown>[]>;
  };

  const regCases: RegistrationTestCase[] = [
    {
      name: 'empty registry, no transactions',
      specs: [SPEC1, SPEC2],
      version: '00',
      expected: {
        ['_zero.InvalidationRegistryVersion']: [
          {stateVersionAtLastSpecChange: '00', lock: 'v'},
        ],
        ['_zero.InvalidationRegistry']: [
          {
            id: SPEC2.id,
            spec: SPEC2,
            fromStateVersion: '00',
            lastRequested: NOW,
          },
          {
            id: SPEC1.id,
            spec: SPEC1,
            fromStateVersion: '00',
            lastRequested: NOW,
          },
        ],
      },
    },
    {
      name: 'already registered',
      setup: {
        ['_zero.InvalidationRegistry']: [
          {
            id: SPEC1.id,
            spec: SPEC1,
            fromStateVersion: '03',
            lastRequested: DATE1,
          },
          {
            id: SPEC2.id,
            spec: SPEC2,
            fromStateVersion: '02',
            lastRequested: DATE2,
          },
        ],
      },
      specs: [SPEC1, SPEC2],
      version: '03',
    },
    {
      name: 'partially registered, existing changes',
      specs: [SPEC1, SPEC2],
      version: '04',
      setup: {
        ['_zero.TxLog']: [
          {stateVersion: '04', lsn: '0/023', time: DATE2, xid: 123},
        ],
        ['_zero.InvalidationRegistryVersion']: [
          {stateVersionAtLastSpecChange: '02', lock: 'v'},
        ],
        ['_zero.InvalidationRegistry']: [
          {
            id: SPEC2.id,
            spec: SPEC2,
            fromStateVersion: '02',
            lastRequested: DATE1,
          },
        ],
      },
      expected: {
        ['_zero.InvalidationRegistryVersion']: [
          {stateVersionAtLastSpecChange: '04', lock: 'v'},
        ],
        ['_zero.InvalidationRegistry']: [
          {
            id: SPEC2.id,
            spec: SPEC2,
            fromStateVersion: '02',
            lastRequested: DATE1,
          },
          {
            id: SPEC1.id,
            spec: SPEC1,
            fromStateVersion: '04',
            lastRequested: NOW,
          },
        ],
      },
    },
  ];

  for (const c of regCases) {
    test(c.name, async () => {
      await initDB(replica, undefined, c.setup);

      const lc = createSilentLogContext();
      const resp = await invalidator.registerInvalidationFilters(
        lc,
        {specs: c.specs},
        NOW,
      );

      expect(resp.invalidatingFromVersion).toBe(c.version);
      await expectTables(replica, c.expected);
    });
  }
});
