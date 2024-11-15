import {LogContext} from '@rocicorp/logger';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {TestLogSink} from '../../../shared/src/logging-test-utils.js';
import {sleep} from '../../../shared/src/sleep.js';
import {getConnectionURI, testDBs} from '../test/db.js';
import type {PostgresDB} from '../types/pg.js';
import {ShortLivedClient} from './short-lived-client.js';

describe('short-lived-client', () => {
  let db: PostgresDB;
  let dbConnStr: string;
  let logSink: TestLogSink;
  let lc: LogContext;

  beforeAll(async () => {
    db = await testDBs.create('short_lived_client_db');
    dbConnStr = getConnectionURI(db);
    logSink = new TestLogSink();
    lc = new LogContext('debug', {}, logSink);
  });

  afterAll(async () => {
    await testDBs.end();
  });

  test('short-lived-client', async () => {
    const client = new ShortLivedClient(lc, dbConnStr, 'foo app', 5);
    const db1 = client.db;

    expect(client.db).toBe(db1);

    // Keep the client alive by asking for it every 2 ms.
    for (let i = 0; i < 5; i++) {
      await sleep(2);
      expect(client.db).toBe(db1);
      expect(logSink.messages).toEqual([]);
    }

    // Now wait for more than 5 ms. Original client should have been shut down.
    await sleep(8);
    const db2 = client.db;
    expect(db2).not.toBe(db1);
    expect(logSink.messages[0]).toEqual([
      'debug',
      {},
      ['closing idle upstream connection'],
    ]);
  });
});
