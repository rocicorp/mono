import {LogContext} from '@rocicorp/logger';
import {expect} from 'vitest';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {getConnectionURI, test, type PgTest} from '../../test/db.ts';
import {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Source} from '../../types/streams.ts';
import {getPragmaConfig, setupReplica} from '../../workers/replicator.ts';
import type {
  ChangeSource,
  ChangeStream,
} from '../change-source/change-source.ts';
import {initializePostgresChangeSource} from '../change-source/pg/change-source.ts';
import {toBigInt} from '../change-source/pg/lsn.ts';
import type {
  BackfillRequest,
  ChangeSourceUpstream,
} from '../change-source/protocol/current.ts';
import {
  initializeStreamer,
  type TuningOptions,
} from '../change-streamer/change-streamer-service.ts';
import {
  type ChangeStreamer,
  type ChangeStreamerService,
  type Downstream,
} from '../change-streamer/change-streamer.ts';
import {ReplicationStatusPublisher} from './replication-status.ts';
import {ReplicatorService} from './replicator.ts';
import {getSubscriptionState} from './schema/replication-state.ts';
import {ThreadWriteWorkerClient} from './write-worker-client.ts';

const lc = new LogContext('error');

const APP_ID = 'replication_resumption';
const SHARD_NUM = 0;
const TASK_ID = 'replication-resumption-test';
const PUBLICATION = 'zero_replication_resumption';
const TIMEOUT_MS = 120_000;

const shard = {
  appID: APP_ID,
  shardNum: SHARD_NUM,
  publications: [PUBLICATION],
};

const streamerOptions: TuningOptions = {
  backPressureLimitHeapProportion: 0.04,
  flowControlConsensusPaddingSeconds: 1,
  statementTimeoutMs: 20_000,
  changeLogBatchSize: 100,
};

type SourceFault = 'drop-upstream-ack';

type FooRow = {
  id: string;
  value: string;
  n: number;
};

function isCommitAck(msg: ChangeSourceUpstream): boolean {
  return msg[0] === 'status' && 'tag' in msg[1] && msg[1].tag === 'commit';
}

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

class FaultyChangeSource implements ChangeSource {
  readonly #inner: ChangeSource;
  readonly #faults: SourceFault[] = [];

  constructor(inner: ChangeSource) {
    this.#inner = inner;
  }

  injectNextCommitAckFault(fault: SourceFault) {
    this.#faults.push(fault);
  }

  get pendingFaults() {
    return this.#faults.length;
  }

  startLagReporter() {
    return this.#inner.startLagReporter();
  }

  async startStream(
    afterWatermark: string,
    backfillRequests: BackfillRequest[] = [],
  ): Promise<ChangeStream> {
    const stream = await this.#inner.startStream(
      afterWatermark,
      backfillRequests,
    );
    return {
      changes: stream.changes,
      acks: {
        push: msg => {
          if (!isCommitAck(msg)) {
            stream.acks.push(msg);
            return;
          }

          const fault = this.#faults.shift();
          switch (fault) {
            case undefined:
              stream.acks.push(msg);
              return;

            case 'drop-upstream-ack':
              stream.changes.cancel(
                new Error(`injected fault: ${fault} at ${msg[2].watermark}`),
              );
              return;
          }
        },
      },
    };
  }

  stop() {
    return this.#inner.stop();
  }
}

function pgRows(upstream: PostgresDB): Promise<FooRow[]> {
  return upstream<FooRow[]>`
    SELECT id, value, n
      FROM foo
     ORDER BY id`;
}

function replicaRows(replicaDbFile: DbFile): FooRow[] {
  const replica = new Database(lc, replicaDbFile.path);
  try {
    return replica
      .prepare(
        `
        SELECT id, value, n
          FROM foo
         ORDER BY id`,
      )
      .all<FooRow>();
  } finally {
    replica.close();
  }
}

function replicaWatermark(replicaDbFile: DbFile): string {
  const replica = new Database(lc, replicaDbFile.path);
  try {
    return getSubscriptionState(new StatementRunner(replica)).watermark;
  } finally {
    replica.close();
  }
}

async function expectSlotHealthy(upstream: PostgresDB) {
  const [{slot}] = await upstream<{slot: string}[]>`
    SELECT slot
      FROM ${upstream(`${APP_ID}_${SHARD_NUM}.replicas`)}
     ORDER BY rank DESC
     LIMIT 1`;
  const rows = await upstream<
    {
      confirmedFlushLSN: string | null;
      restartLSN: string | null;
      walStatus: string | null;
    }[]
  >`
    SELECT confirmed_flush_lsn as "confirmedFlushLSN",
           restart_lsn as "restartLSN",
           wal_status as "walStatus"
      FROM pg_replication_slots
     WHERE slot_name = ${slot}`;

  expect(rows).toHaveLength(1);
  const [state] = rows;
  expect(state.confirmedFlushLSN).not.toBeNull();
  expect(state.restartLSN).not.toBeNull();
  expect(state.walStatus).not.toBe('lost');

  return {
    slot,
    confirmedFlushLSN: toBigInt(state.confirmedFlushLSN ?? '0/0'),
  };
}

async function eventuallyExpectReplicaMatches(
  upstream: PostgresDB,
  replicaDbFile: DbFile,
  description: string,
) {
  const deadline = Date.now() + 30_000;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      expect(replicaRows(replicaDbFile)).toEqual(await pgRows(upstream));
      await expectSlotHealthy(upstream);
      return;
    } catch (e) {
      lastError = e;
      await sleep(50);
    }
  }

  const watermark = replicaWatermark(replicaDbFile);
  const actualRows = replicaRows(replicaDbFile);
  const expectedRows = await pgRows(upstream);
  const details =
    lastError instanceof Error ? (lastError.stack ?? lastError.message) : '';
  throw new Error(
    `timed out waiting for replica to match PG after ${description} ` +
      `(replica watermark ${watermark})\n` +
      `replica rows: ${JSON.stringify(actualRows)}\n` +
      `pg rows: ${JSON.stringify(expectedRows)}\n` +
      details,
  );
}

async function startHarness(testDBs: PgTest['testDBs']) {
  const cleanup: (() => Promise<void> | void)[] = [];

  try {
    const upstream = await testDBs.create('replication_resumption_upstream', {
      typeOpts: false,
    });
    const changeDB = await testDBs.create('replication_resumption_change', {
      typeOpts: {sendStringAsJson: true},
    });
    cleanup.push(() => testDBs.drop(upstream, changeDB));

    await upstream.unsafe(`
      CREATE TABLE foo(
        id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        n INT4 NOT NULL
      );

      INSERT INTO foo(id, value, n) VALUES
        ('a', 'initial-a', 1),
        ('z', 'initial-z', 26);

      CREATE PUBLICATION ${PUBLICATION} FOR TABLE foo;
    `);

    const replicaDbFile = new DbFile('replication-resumption');
    cleanup.push(() => replicaDbFile.delete());

    const {subscriptionState, changeSource} =
      await initializePostgresChangeSource(
        lc,
        getConnectionURI(upstream),
        shard,
        replicaDbFile.path,
        {tableCopyWorkers: 1},
        {suite: 'replication-resumption'},
      );

    await setupReplica(lc, 'serving', {file: replicaDbFile.path});

    const faultyChangeSource = new FaultyChangeSource(changeSource);
    const changeStreamer = await initializeStreamer(
      lc,
      shard,
      TASK_ID,
      'change-streamer:replication-resumption',
      'ws',
      changeDB,
      faultyChangeSource,
      ReplicationStatusPublisher.forTesting(),
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

    let replicator: ReplicatorService | undefined;
    let replicatorDone: Promise<void> | undefined;

    async function stopReplicator() {
      if (!replicator || !replicatorDone) {
        return;
      }
      const current = replicator;
      const currentDone = replicatorDone;
      replicator = undefined;
      replicatorDone = undefined;
      await current.stop();
      await currentDone;
    }

    async function startReplicator() {
      const worker = new ThreadWriteWorkerClient();
      await worker.init(
        replicaDbFile.path,
        'serving',
        getPragmaConfig('serving'),
        {level: 'error', format: 'text'},
      );
      replicator = new ReplicatorService(
        lc,
        TASK_ID,
        'replication-resumption-replicator',
        'serving',
        parseStringifiedChangeStreamer(changeStreamer),
        worker,
        null,
      );
      replicatorDone = replicator.run();
    }

    await startReplicator();
    cleanup.push(stopReplicator);

    await eventuallyExpectReplicaMatches(upstream, replicaDbFile, 'startup');

    return {
      upstream,
      replicaDbFile,
      faultyChangeSource,
      async restartReplicator() {
        await stopReplicator();
        await startReplicator();
        await eventuallyExpectReplicaMatches(
          upstream,
          replicaDbFile,
          'replicator restart',
        );
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

test(
  'replication resumes from durable state after injected PG and replicator faults',
  {timeout: TIMEOUT_MS},
  async ({testDBs}: PgTest) => {
    const harness = await startHarness(testDBs);
    let previousConfirmedFlushLSN = 0n;

    async function mutateAndVerify(
      description: string,
      mutate: () => Promise<unknown>,
    ) {
      await mutate();
      await eventuallyExpectReplicaMatches(
        harness.upstream,
        harness.replicaDbFile,
        description,
      );
      const {confirmedFlushLSN} = await expectSlotHealthy(harness.upstream);
      expect(confirmedFlushLSN).toBeGreaterThanOrEqual(
        previousConfirmedFlushLSN,
      );
      previousConfirmedFlushLSN = confirmedFlushLSN;
    }

    try {
      await mutateAndVerify(
        'baseline insert',
        () => harness.upstream`
        INSERT INTO foo(id, value, n) VALUES ('b', 'baseline', 2)`,
      );

      harness.faultyChangeSource.injectNextCommitAckFault('drop-upstream-ack');
      await mutateAndVerify(
        'commit stored before upstream ACK is dropped',
        () =>
          harness.upstream`
          UPDATE foo SET value = 'ack-dropped', n = n + 10 WHERE id = 'a'`,
      );

      harness.faultyChangeSource.injectNextCommitAckFault('drop-upstream-ack');
      await mutateAndVerify(
        'second commit stored before upstream ACK is dropped',
        () =>
          harness.upstream`
          INSERT INTO foo(id, value, n) VALUES ('c', 'ack-dropped-again', 3)`,
      );

      await harness.restartReplicator();

      await mutateAndVerify(
        'replicator restart and multi-row transaction',
        () =>
          harness.upstream.begin(async tx => {
            await tx`DELETE FROM foo WHERE id = 'b'`;
            await tx`
            UPDATE foo SET value = 'worker-result-lost', n = n + 1
             WHERE id = 'z'`;
            await tx`
            INSERT INTO foo(id, value, n)
            VALUES ('d', 'same-pg-transaction', 4)`;
          }),
      );

      await mutateAndVerify(
        'post-fault transaction proves resumed stream',
        () =>
          harness.upstream`
          UPDATE foo SET value = 'resumed', n = n + 1 WHERE id = 'c'`,
      );

      expect(harness.faultyChangeSource.pendingFaults).toBe(0);
    } finally {
      await harness.cleanup();
    }
  },
);
