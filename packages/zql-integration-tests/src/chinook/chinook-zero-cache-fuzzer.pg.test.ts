import {expect} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {BigIntJSON} from '../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {initializePostgresChangeSource} from '../../../zero-cache/src/services/change-source/pg/change-source.ts';
import {initializeStreamer} from '../../../zero-cache/src/services/change-streamer/change-streamer-service.ts';
import type {
  ChangeStreamer,
  ChangeStreamerService,
  Downstream,
} from '../../../zero-cache/src/services/change-streamer/change-streamer.ts';
import {ReplicationStatusPublisher} from '../../../zero-cache/src/services/replicator/replication-status.ts';
import type {ReplicaState} from '../../../zero-cache/src/services/replicator/replicator.ts';
import {ReplicatorService} from '../../../zero-cache/src/services/replicator/replicator.ts';
import {ThreadWriteWorkerClient} from '../../../zero-cache/src/services/replicator/write-worker-client.ts';
import {
  getConnectionURI,
  test,
  type PgTest,
} from '../../../zero-cache/src/test/db.ts';
import {DbFile} from '../../../zero-cache/src/test/lite.ts';
import type {PostgresDB} from '../../../zero-cache/src/types/pg.ts';
import type {Source} from '../../../zero-cache/src/types/streams.ts';
import {
  getPragmaConfig,
  setupReplica,
} from '../../../zero-cache/src/workers/replicator.ts';
import {getServerSchema} from '../../../zero-server/src/schema.ts';
import {Transaction} from '../../../zero-server/src/test/util.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../../zqlite/src/test/source-factory.ts';
import '../helpers/comparePg.ts';
import {TestPGQueryDelegate} from '../helpers/runner.ts';
import {pkOf} from './fuzz/axes.ts';
import {CostModel} from './fuzz/cost.ts';
import {
  checkQueryCases,
  enumerate,
  l1QueryCases,
  mutationQueryCases,
  panicIfFailed,
  skeletonQueryCases,
  swarmQueryCases,
  tailQueryCases,
} from './fuzz/driver.ts';
import {Data} from './fuzz/literals.ts';
import {miniData, miniPgContent} from './fuzz/mini.ts';
import {builder, schema} from './schema.ts';

const lc = createSilentLogContext();

const APP_ID = 'zql_integration_zero_cache_fuzzer';
const SHARD_NUM = 0;
const TASK_ID = 'zql-integration-zero-cache-fuzzer';
const TIMEOUT_MS = 120_000;
const SEED = 0x00c0ffee;

const shard = {
  appID: APP_ID,
  shardNum: SHARD_NUM,
  publications: [],
};

const streamerOptions = {
  backPressureLimitHeapProportion: 0.04,
  flowControlConsensusPaddingSeconds: 1,
  statementTimeoutMs: 20_000,
  changeLogBatchSize: 2000,
};

const data = new Data(miniData, pkOf);
const L0_QUERY_CASES = skeletonQueryCases(
  enumerate({depth: 1, related: 1, exists: 1}),
);
const L1_QUERY_CASES = l1QueryCases(data);
const ZERO_CACHE_QUERY_CASES = [
  ...L0_QUERY_CASES,
  ...L1_QUERY_CASES.cases,
  ...swarmQueryCases(data, SEED, 16, 4),
  ...mutationQueryCases(
    enumerate({depth: 2, related: 1, exists: 1}).slice(0, 100),
    SEED ^ 0x5eed,
  ),
  ...tailQueryCases(CostModel.fromData(miniData, 1_000_000), SEED, 150).cases,
];

type ChinookSchema = typeof schema;

function parseStringifiedSource(source: Source<string>): Source<Downstream> {
  return {
    cancel: err => source.cancel(err),
    async *[Symbol.asyncIterator]() {
      for await (const msg of source) {
        yield BigIntJSON.parse(msg) as Downstream;
      }
    },
  };
}

function parseStringifiedChangeStreamer(
  streamer: ChangeStreamerService,
): ChangeStreamer {
  return {
    async subscribe(ctx) {
      return parseStringifiedSource(await streamer.subscribe(ctx));
    },
  };
}

async function withTimeout<T>(
  promise: Promise<T>,
  description: string,
): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () => reject(new Error(`timed out waiting for ${description}`)),
          TIMEOUT_MS,
        );
      }),
    ]);
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

async function startZeroCacheReplica(testDBs: PgTest['testDBs']) {
  const cleanup: (() => Promise<void> | void)[] = [];

  try {
    const upstream = await testDBs.create(
      'chinook_zero_cache_fuzzer_upstream',
      {typeOpts: false},
    );
    const changeDB = await testDBs.create('chinook_zero_cache_fuzzer_change', {
      typeOpts: {sendStringAsJson: true},
    });
    cleanup.push(() => testDBs.drop(upstream, changeDB));

    await upstream.unsafe(miniPgContent());

    const replicaDbFile = new DbFile('chinook-zero-cache-fuzzer');
    cleanup.push(() => replicaDbFile.delete());

    const {subscriptionState, changeSource} =
      await initializePostgresChangeSource(
        lc,
        getConnectionURI(upstream),
        shard,
        replicaDbFile.path,
        {tableCopyWorkers: 1},
        {suite: 'chinook-zero-cache-fuzzer'},
      );

    await setupReplica(lc, 'serving', {file: replicaDbFile.path});

    const changeStreamer = await initializeStreamer(
      lc,
      shard,
      TASK_ID,
      'change-streamer:zero-cache-fuzzer',
      'ws',
      changeDB,
      changeSource,
      ReplicationStatusPublisher.forReplicaFile(replicaDbFile.path, () =>
        Promise.resolve(),
      ),
      subscriptionState,
      null,
      true,
      streamerOptions,
    );
    const changeStreamerDone = changeStreamer.run();
    cleanup.push(async () => {
      await changeStreamer.stop();
      await changeStreamerDone;
    });

    const worker = new ThreadWriteWorkerClient();
    await worker.init(
      replicaDbFile.path,
      'serving',
      getPragmaConfig('serving'),
      {level: 'error', format: 'text'},
    );

    const replicator = new ReplicatorService(
      lc,
      TASK_ID,
      'chinook-zero-cache-fuzzer-replicator',
      'serving',
      parseStringifiedChangeStreamer(changeStreamer),
      worker,
      null,
    );
    const replicatorDone = replicator.run();
    cleanup.push(async () => {
      await replicator.stop();
      await replicatorDone;
    });

    const notifications = replicator.subscribe();
    const versions = notifications[Symbol.asyncIterator]();
    cleanup.push(() => notifications.cancel());
    await withTimeout(versions.next(), 'initial replica version');

    const replica = new Database(lc, replicaDbFile.path);
    cleanup.push(() => replica.close());
    const sqlite = newQueryDelegate(lc, testLogConfig, replica, schema);

    const serverSchema = await upstream.begin(tx =>
      getServerSchema(new Transaction(tx), schema),
    );
    const pg = new TestPGQueryDelegate(upstream, schema, serverSchema);

    return {
      upstream,
      pg,
      sqlite,
      async waitForReplicaVersion(description: string): Promise<ReplicaState> {
        const {done, value} = await withTimeout(
          versions.next(),
          `replica version after ${description}`,
        );
        if (done) {
          throw new Error(`replica notifications ended after ${description}`);
        }
        return value;
      },
      async cleanup() {
        for (const fn of cleanup.reverse()) {
          await fn();
        }
        cleanup.length = 0;
      },
    };
  } catch (e) {
    for (const fn of cleanup.reverse()) {
      await fn();
    }
    throw e;
  }
}

async function expectReplicaMatchesPG({
  pg,
  sqlite,
  query,
}: {
  pg: TestPGQueryDelegate;
  sqlite: ReturnType<typeof newQueryDelegate>;
  query: AnyQuery;
}) {
  const pgResult = await pg.run(query);
  const rootTable = asQueryInternals(query).ast
    .table as keyof ChinookSchema['tables'];
  const sqliteResult = mapResultToClientNames(
    await sqlite.run(query),
    schema,
    rootTable,
  );

  expect(sqliteResult).toEqualPg(pgResult);
}

async function insertTrack(upstream: PostgresDB) {
  await upstream`
    INSERT INTO track (
      track_id,
      name,
      album_id,
      media_type_id,
      genre_id,
      composer,
      milliseconds,
      bytes,
      unit_price
    ) VALUES (
      108,
      't-replicated',
      20,
      1,
      2,
      'Zero',
      123000,
      123456,
      0.99
    )`;
}

async function moveTrackOutOfQuery(upstream: PostgresDB) {
  await upstream`
    UPDATE track
       SET album_id = 10
     WHERE track_id = 108`;
}

async function deleteTrack(upstream: PostgresDB) {
  await upstream`
    DELETE FROM track
     WHERE track_id = 105`;
}

test(
  `zero-cache replica stays query-equivalent to PostgreSQL for ${ZERO_CACHE_QUERY_CASES.length} generated query cases and replicated writes`,
  {timeout: TIMEOUT_MS},
  async ({testDBs}: PgTest) => {
    const harness = await startZeroCacheReplica(testDBs);
    try {
      expect(L1_QUERY_CASES.coverage.fraction()).toBe(1);
      const report = await checkQueryCases(ZERO_CACHE_QUERY_CASES, query =>
        expectReplicaMatchesPG({...harness, query}),
      );
      expect(report.total).toBeGreaterThan(500);
      panicIfFailed(report, 12);

      const query = builder.album
        .where('id', '=', 20)
        .related('tracks', t => t.orderBy('id', 'asc'))
        .one();

      await expectReplicaMatchesPG({...harness, query});

      await insertTrack(harness.upstream);
      await harness.waitForReplicaVersion('track insert');
      await expectReplicaMatchesPG({...harness, query});

      await moveTrackOutOfQuery(harness.upstream);
      await harness.waitForReplicaVersion('track update');
      await expectReplicaMatchesPG({...harness, query});

      await deleteTrack(harness.upstream);
      await harness.waitForReplicaVersion('track delete');
      await expectReplicaMatchesPG({...harness, query});
    } finally {
      await harness.cleanup();
    }
  },
);
