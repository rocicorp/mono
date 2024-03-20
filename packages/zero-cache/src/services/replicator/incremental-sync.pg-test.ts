import {afterAll, afterEach, beforeEach, describe, test} from '@jest/globals';
import type postgres from 'postgres';
import {TestDBs, expectTables} from '../../test/db.js';
import {CREATE_REPLICATION_TABLES} from './incremental-sync.js';

describe('replicator/incremental-sync', () => {
  const testDBs = new TestDBs();
  let db: postgres.Sql;

  beforeEach(async () => {
    db = await testDBs.create('incremental_sync_test');
    await db`CREATE SCHEMA IF NOT EXISTS zero`;
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  afterAll(async () => {
    await testDBs.end();
  });

  test('create tables', async () => {
    await db.unsafe(CREATE_REPLICATION_TABLES);

    await expectTables(db, {
      ['zero.tx_log']: [],
      ['zero.change_log']: [],
      ['zero.invalidation_registry']: [],
      ['zero.invalidation_index']: [],
    });
  });
});
