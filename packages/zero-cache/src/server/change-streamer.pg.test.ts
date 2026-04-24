import {expect, vi} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {StatementRunner} from '../db/statements.ts';
import {initializePostgresChangeSource} from '../services/change-source/pg/change-source.ts';
import {initChangeStreamerSchema} from '../services/change-streamer/schema/init.ts';
import {ensureReplicationConfig} from '../services/change-streamer/schema/tables.ts';
import type * as LifeCycle from '../services/life-cycle.ts';
import type * as LitestreamCommands from '../services/litestream/commands.ts';
import {getSubscriptionState} from '../services/replicator/schema/replication-state.ts';
import {getConnectionURI, test, type PgTest} from '../test/db.ts';
import {DbFile} from '../test/lite.ts';
import {inProcChannel} from '../types/processes.ts';
import {orTimeout} from '../types/timeout.ts';
import runWorker from './change-streamer.ts';

vi.mock('../services/litestream/commands.ts', async importOriginal => {
  const actual = await importOriginal<typeof LitestreamCommands>();
  return {
    ...actual,
    restoreReplica: vi.fn().mockResolvedValue(undefined),
  };
});

vi.mock('../services/life-cycle.ts', async importOriginal => {
  const actual = await importOriginal<typeof LifeCycle>();
  return {
    ...actual,
    exitAfter: vi.fn(),
    runUntilKilled: vi.fn().mockResolvedValue(undefined),
  };
});

// Regression test for the startup self-deadlock where an AutoResetSignal retry
// attempted to create a new replication slot while the startup purge-lock
// transaction was still open on the same Postgres (CHANGE_DB == UPSTREAM_DB).
// See https://www.notion.so/replicache/Zbugs-Cloudzero-Oncall-2b63bed8954580859470f6fa05ede908?source=copy_link#34c3bed8954580f8b680d1dbf68843d1
test('change-streamer startup does not deadlock on autoreset retry when change and upstream share postgres', async ({
  testDBs,
}: PgTest) => {
  const lc = createSilentLogContext();
  const shard = {appID: 'zoro', shardNum: 3, publications: []};
  const upstream = await testDBs.create(
    'change_streamer_worker_autoreset_deadlock_test_upstream',
    undefined,
    {sendStringAsJson: true},
  );
  const upstreamURI = getConnectionURI(upstream);
  const replicaFile = new DbFile('change-streamer-worker-autoreset-deadlock');

  let initialSource:
    | Awaited<ReturnType<typeof initializePostgresChangeSource>>['changeSource']
    | undefined;

  try {
    await upstream.unsafe(`
      CREATE TABLE foo(id TEXT PRIMARY KEY);
      INSERT INTO foo(id) VALUES ('seed');
    `);

    ({changeSource: initialSource} = await initializePostgresChangeSource(
      lc,
      upstreamURI,
      shard,
      replicaFile.path,
      {tableCopyWorkers: 5},
      {test: 'context'},
    ));

    const [{slot: oldSlot}] = await upstream<{slot: string}[]>`
      SELECT slot FROM ${upstream(`${shard.appID}_${shard.shardNum}.replicas`)}`;

    const restoredReplica = replicaFile.connect(lc);
    const subscriptionState = getSubscriptionState(
      new StatementRunner(restoredReplica),
    );
    restoredReplica.close();

    await initChangeStreamerSchema(lc, upstream, shard);
    await ensureReplicationConfig(lc, upstream, subscriptionState, shard, true);

    await initialSource.stop();
    await upstream`SELECT pg_drop_replication_slot(${oldSlot})`;

    const [worker, parent] = inProcChannel();
    const ready = new Promise<void>(resolve => {
      parent.onceMessageType('ready', () => {
        resolve();
      });
    });

    const originalNodeEnv = process.env.NODE_ENV;
    const originalSingleProcess = process.env.SINGLE_PROCESS;

    try {
      process.env.NODE_ENV = 'development';
      process.env.SINGLE_PROCESS = '1';

      const changeStreamerPort = 30_000 + Math.floor(Math.random() * 10_000);
      const litestreamPort = changeStreamerPort + 1;

      const env = {
        ...process.env,
        ZERO_TASK_ID: 'task-id',
        ZERO_UPSTREAM_DB: upstreamURI,
        ZERO_CHANGE_DB: upstreamURI,
        ZERO_CVR_DB: upstreamURI,
        ZERO_REPLICA_FILE: replicaFile.path,
        ZERO_APP_ID: shard.appID,
        ZERO_SHARD_NUM: String(shard.shardNum),
        ZERO_ENABLE_CRUD_MUTATIONS: 'false',
        ZERO_CHANGE_STREAMER_ADDRESS: `127.0.0.1:${changeStreamerPort}`,
        ZERO_LITESTREAM_PORT: String(litestreamPort),
        ZERO_NUM_SYNC_WORKERS: '1',
        ZERO_PORT: String(changeStreamerPort - 1),
        ZERO_CHANGE_STREAMER_PORT: String(changeStreamerPort),
      };

      const workerDone = runWorker(worker, env);
      const startup = await orTimeout(
        Promise.all([ready, workerDone]).then(() => true),
        7_500,
      );

      assert(startup !== 'timed-out', 'worker startup timed out');
      expect(startup).toBe(true);
    } finally {
      process.env.NODE_ENV = originalNodeEnv;
      process.env.SINGLE_PROCESS = originalSingleProcess;
    }

    const liveSlots = await upstream<{slot: string}[]>`
      SELECT slot_name as slot
        FROM pg_replication_slots
       WHERE slot_name LIKE ${`${shard.appID}\\_${shard.shardNum}\\_%`}
       ORDER BY slot_name`;
    expect(liveSlots).toHaveLength(1);
    expect(liveSlots[0].slot).not.toBe(oldSlot);
  } finally {
    await initialSource?.stop().catch(() => {});
    replicaFile.delete();
    await testDBs.drop(upstream);
  }
});
