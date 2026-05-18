import {gunzipSync} from 'node:zlib';
import {getLocal, type Mockttp} from 'mockttp';
import {expect, vi} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import {StatementRunner} from '../db/statements.ts';
import {publishCriticalEvent} from '../observability/events.ts';
import {initializePostgresChangeSource} from '../services/change-source/pg/change-source.ts';
import {initChangeStreamerSchema} from '../services/change-streamer/schema/init.ts';
import {ensureReplicationConfig} from '../services/change-streamer/schema/tables.ts';
import type * as LifeCycle from '../services/life-cycle.ts';
import type * as LitestreamCommands from '../services/litestream/commands.ts';
import {replicationStatusError} from '../services/replicator/replication-status.ts';
import {getSubscriptionState} from '../services/replicator/schema/replication-state.ts';
import {getConnectionURI, test, type PgTest} from '../test/db.ts';
import {DbFile} from '../test/lite.ts';
import {ConfigurationError} from '../types/configuration-error.ts';
import {inProcChannel} from '../types/processes.ts';
import {orTimeout} from '../types/timeout.ts';
import runWorker from './change-streamer.ts';

vi.mock('../services/litestream/commands.ts', async (importOriginal: <T>() => Promise<T>) => {
  const actual = await importOriginal<typeof LitestreamCommands>();
  return {
    ...actual,
    restoreReplica: vi.fn().mockResolvedValue(undefined),
  };
});

vi.mock('../services/life-cycle.ts', async (importOriginal: <T>() => Promise<T>) => {
  const actual = await importOriginal<typeof LifeCycle>();
  return {
    ...actual,
    exitAfter: vi.fn(),
    runUntilKilled: vi.fn().mockResolvedValue(undefined),
  };
});

test('change-streamer startup publishes Postgres config errors and rethrows ConfigurationError', async ({
  testDBs,
}: PgTest) => {
  const upstream = await testDBs.create(
    'change_streamer_worker_config_error_test_upstream',
    {typeOpts: {sendStringAsJson: true}},
  );
  const upstreamURI = getConnectionURI(upstream);
  const changeURI = new URL(upstreamURI);
  changeURI.pathname = '/missing_change_streamer_worker_config_error_db';
  const replicaFile = new DbFile('change-streamer-worker-config-error');
  const [worker] = inProcChannel();
  const eventSink: Mockttp = getLocal();

  const originalNodeEnv = process.env.NODE_ENV;
  const originalSingleProcess = process.env.SINGLE_PROCESS;
  const originalTestCloudEventSink = process.env.TEST_CLOUD_EVENT_SINK;

  try {
    await eventSink.start();
    const statusEvent = await eventSink.forPost().thenReply(200);

    process.env.NODE_ENV = 'development';
    process.env.SINGLE_PROCESS = '1';
    process.env.TEST_CLOUD_EVENT_SINK = eventSink.url;

    const changeStreamerPort = 30_000 + Math.floor(Math.random() * 10_000);
    const litestreamPort = changeStreamerPort + 1;
    const env = {
      ...process.env,
      TEST_CLOUD_EVENT_SINK: eventSink.url,
      ZERO_TASK_ID: 'task-id',
      ZERO_UPSTREAM_DB: upstreamURI,
      ZERO_CHANGE_DB: String(changeURI),
      ZERO_CVR_DB: upstreamURI,
      ZERO_REPLICA_FILE: replicaFile.path,
      ZERO_APP_ID: 'zoro',
      ZERO_SHARD_NUM: '3',
      ZERO_ENABLE_CRUD_MUTATIONS: 'false',
      ZERO_CHANGE_STREAMER_ADDRESS: `127.0.0.1:${changeStreamerPort}`,
      ZERO_LITESTREAM_PORT: String(litestreamPort),
      ZERO_NUM_SYNC_WORKERS: '1',
      ZERO_PORT: String(changeStreamerPort - 1),
      ZERO_CHANGE_STREAMER_PORT: String(changeStreamerPort),
      ZERO_LOG_FORMAT: 'json',
      ZERO_LOG_LEVEL: 'error',
      ZERO_CLOUD_EVENT_SINK_ENV: 'TEST_CLOUD_EVENT_SINK',
      ZERO_ENABLE_TELEMETRY: 'false',
    };

    const eventLogContext = createSilentLogContext();
    const startup = await orTimeout(
      runWorker(worker, env)
        .catch(async e => {
          await publishCriticalEvent(
            eventLogContext,
            replicationStatusError(eventLogContext, 'Initializing', e),
          );
          throw e;
        })
        .then(
          () => undefined,
          e => e,
        ),
      7_500,
    );

    assert(startup !== 'timed-out', 'worker startup timed out');
    expect(startup).toBeInstanceOf(ConfigurationError);

    const [request] = await statusEvent.getSeenRequests();
    expect(request.headers).toMatchObject({
      'ce-datacontentencoding': 'gzip',
      'ce-source': 'task-id',
      'ce-type': 'zero/events/status/replication/v1',
      'content-type': 'text/plain',
    });
    const event = JSON.parse(
      gunzipSync(
        Buffer.from(must(await request.body.getText()), 'base64'),
      ).toString(),
    ) as Record<string, unknown>;

    expect(event).toMatchObject({
      type: 'zero/events/status/replication/v1',
      component: 'replication',
      status: 'ERROR',
      stage: 'Initializing',
      description:
        'ConfigurationError: Unable to connect to Postgres. Check your database configuration.',
      errorDetails: {
        name: 'ConfigurationError',
        message:
          'Unable to connect to Postgres. Check your database configuration.',
        cause: {
          code: '3D000',
          message: expect.stringContaining(
            'missing_change_streamer_worker_config_error_db',
          ),
          name: 'PostgresError',
        },
      },
    });
  } finally {
    process.env.NODE_ENV = originalNodeEnv;
    process.env.SINGLE_PROCESS = originalSingleProcess;
    process.env.TEST_CLOUD_EVENT_SINK = originalTestCloudEventSink;
    await eventSink.stop().catch(() => {});
    replicaFile.delete();
    await testDBs.drop(upstream);
  }
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
    {typeOpts: {sendStringAsJson: true}},
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

    const [{id: oldID, slot: oldSlot}] = await upstream<
      {id: string; slot: string}[]
    >`
      SELECT id, slot FROM ${upstream(`${shard.appID}_${shard.shardNum}.replicas`)}`;

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
        ZERO_LOG_LEVEL: 'error', // silence logs from the worker
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

    const liveSlots = await upstream<{id: string; slot: string}[]>`
      SELECT id, slot, rank
        FROM pg_replication_slots
        JOIN ${upstream(`${shard.appID}_${shard.shardNum}.replicas`)} ON slot_name = slot
        ORDER BY rank DESC LIMIT 1;
    `;
    expect(liveSlots[0].slot).toBe(oldSlot); // same slot name
    expect(liveSlots[0].id).not.toBe(oldID); // was reused for new replica
  } finally {
    await initialSource?.stop().catch(() => {});
    replicaFile.delete();
    await testDBs.drop(upstream);
  }
});
