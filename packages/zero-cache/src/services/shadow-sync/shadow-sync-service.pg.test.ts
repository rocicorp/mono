import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../../shared/src/logging-test-utils.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {getConnectionURI, type PgTest, test} from '../../test/db.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {ensureShardSchema} from '../change-source/pg/schema/init.ts';
import {ShadowSyncService} from './shadow-sync-service.ts';

const APP_ID = '1';
const SHARD_NUM = 24;

const SHARD = {
  appID: APP_ID,
  shardNum: SHARD_NUM,
  publications: [] as readonly string[],
} as const;

const CONTEXT = {foo: 'shadow-sync-service-test'};

const BASE_OPTIONS = {
  intervalMs: 50,
  sampleRate: 1,
  maxRowsPerTable: 10,
} as const;

function countMessagesStartingWith(sink: TestLogSink, prefix: string): number {
  return sink.messages.filter(([, , args]) =>
    args.some(a => typeof a === 'string' && a.startsWith(prefix)),
  ).length;
}

async function waitFor(predicate: () => boolean, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(25);
  }
}

describe('ShadowSyncService', () => {
  let upstream: PostgresDB;

  beforeEach<PgTest>(async ({testDBs}) => {
    upstream = await testDBs.create('shadow_sync_service_test');
    return () => testDBs.drop(upstream);
  });

  test('completes shadow sync runs and stops cleanly', async () => {
    await upstream`CREATE TABLE items(id int4 PRIMARY KEY)`;
    await upstream`
      INSERT INTO items(id) SELECT g FROM generate_series(1, 20) g`;
    await ensureShardSchema(createSilentLogContext(), upstream, SHARD);

    const sink = new TestLogSink();
    const lc = new LogContext('info', undefined, sink);

    const service = new ShadowSyncService(
      lc,
      SHARD,
      getConnectionURI(upstream),
      CONTEXT,
      BASE_OPTIONS,
    );

    const running = service.run();
    await waitFor(
      () =>
        countMessagesStartingWith(sink, 'shadow initial-sync completed') > 0,
      10_000,
    );
    expect(
      countMessagesStartingWith(sink, 'shadow initial-sync completed'),
    ).toBeGreaterThanOrEqual(1);
    expect(countMessagesStartingWith(sink, 'shadow initial-sync failed')).toBe(
      0,
    );

    await service.stop();
    await running;
  });

  test('keeps running after a failing run', async () => {
    const sink = new TestLogSink();
    const lc = new LogContext('info', undefined, sink);

    const service = new ShadowSyncService(
      lc,
      SHARD,
      // Bogus URI: the postgres client will reject without ever touching
      // an upstream server.
      'postgres://nonexistent.invalid:5/does_not_exist',
      CONTEXT,
      BASE_OPTIONS,
    );

    const running = service.run();
    await waitFor(
      () => countMessagesStartingWith(sink, 'shadow initial-sync failed') >= 2,
      15_000,
    );
    expect(
      countMessagesStartingWith(sink, 'shadow initial-sync failed'),
    ).toBeGreaterThanOrEqual(2);

    await service.stop();
    await running;
  });
});
