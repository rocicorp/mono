import {LogContext} from '@rocicorp/logger';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {expect} from 'vitest';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {getConnectionURI, test, type PgTest} from '../../test/db.ts';
import {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {forkChildWorker, type Worker} from '../../types/processes.ts';
import type {Source} from '../../types/streams.ts';
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
  type SerializedDownstream,
} from '../change-streamer/change-streamer.ts';
import {ReplicationStatusPublisher} from './replication-status.ts';
import {getSubscriptionState} from './schema/replication-state.ts';

const lc = new LogContext('error');

const APP_ID = 'replication_resumption';
const SHARD_NUM = 0;
const TASK_ID = 'replication-resumption-test';
const PUBLICATION = 'zero_replication_resumption';
const TIMEOUT_MS = 120_000;
const CHILD_URL = new URL('./replication-resumption-child.ts', import.meta.url);

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
type ChildFailpoint = 'none' | 'after-sqlite-commit-before-ack';

type FooRow = {
  id: string;
  value: string;
  n: number;
};

function isCommitAck(msg: ChangeSourceUpstream): boolean {
  return msg[0] === 'status' && 'tag' in msg[1] && msg[1].tag === 'commit';
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

type ParentMessage =
  | ['replication-resumption:ready', {pid: number}]
  | [
      'replication-resumption:subscribe',
      Parameters<ChangeStreamer['subscribe']>[0],
    ]
  | ['replication-resumption:consumed', {seq: number}]
  | ['replication-resumption:cancel', Record<string, never>]
  | [
      'replication-resumption:failpoint',
      {name: Exclude<ChildFailpoint, 'none'>; watermark: string},
    ];

type ChildMessage =
  | [
      'replication-resumption:downstream',
      {seq: number; msg: SerializedDownstream},
    ]
  | ['replication-resumption:source-error', {message: string}]
  | ['replication-resumption:source-end', Record<string, never>];

function parentMessage(data: unknown): ParentMessage | undefined {
  if (!Array.isArray(data) || data.length !== 2) {
    return undefined;
  }
  switch (data[0]) {
    case 'replication-resumption:ready':
    case 'replication-resumption:subscribe':
    case 'replication-resumption:consumed':
    case 'replication-resumption:cancel':
    case 'replication-resumption:failpoint':
      return data as ParentMessage;
  }
  return undefined;
}

function sendChild(child: Worker, msg: ChildMessage): void {
  child.send(msg as never);
}

type ActiveBridge = {
  source: Source<string>;
  waiters: Map<number, Resolver<void, Error>>;
};

class ForkedReplicator {
  readonly #changeStreamer: ChangeStreamerService;
  readonly #child: Worker;
  readonly #ready = resolver<void, Error>();
  readonly #closed = resolver<void>();
  readonly #failpoints = new Queue<{
    name: Exclude<ChildFailpoint, 'none'>;
    watermark: string;
  }>();
  readonly #bridges = new Set<ActiveBridge>();

  #closedFlag = false;
  #seq = 0;

  constructor(
    changeStreamer: ChangeStreamerService,
    replicaPath: string,
    failpoint: ChildFailpoint = 'none',
  ) {
    this.#changeStreamer = changeStreamer;
    this.#child = forkChildWorker(
      CHILD_URL,
      process.env,
      replicaPath,
      failpoint,
    );
    this.#child.on('message', this.#onMessage);
    this.#child.on('error', err => {
      this.#ready.reject(err);
      this.#failpoints.enqueueRejection(err);
    });
    this.#child.on('close', () => this.#onClose());
  }

  get pid() {
    return this.#child.pid;
  }

  ready() {
    return this.#ready.promise;
  }

  waitForFailpoint() {
    return this.#failpoints.dequeue();
  }

  async stop(signal: NodeJS.Signals = 'SIGTERM') {
    if (!this.#closedFlag) {
      this.#child.kill(signal);
    }
    await this.#closed.promise;
  }

  readonly #onMessage = (data: unknown) => {
    const msg = parentMessage(data);
    if (!msg) {
      return;
    }
    switch (msg[0]) {
      case 'replication-resumption:ready':
        this.#ready.resolve();
        break;
      case 'replication-resumption:subscribe':
        void this.#bridge(msg[1]);
        break;
      case 'replication-resumption:consumed':
        this.#resolveConsumed(msg[1].seq);
        break;
      case 'replication-resumption:cancel':
        this.#cancelBridges();
        break;
      case 'replication-resumption:failpoint':
        this.#failpoints.enqueue(msg[1]);
        break;
    }
  };

  #onClose() {
    if (this.#closedFlag) {
      return;
    }
    this.#closedFlag = true;
    const err = new Error(
      `forked replicator ${this.#child.pid ?? '<unknown>'} closed`,
    );
    this.#ready.reject(err);
    this.#failpoints.enqueueRejection(err);
    this.#cancelBridges(err);
    this.#child.off('message', this.#onMessage);
    this.#closed.resolve();
  }

  async #bridge(ctx: Parameters<ChangeStreamer['subscribe']>[0]) {
    const source = await this.#changeStreamer.subscribe(ctx);
    const bridge: ActiveBridge = {source, waiters: new Map()};
    this.#bridges.add(bridge);

    try {
      const pipeline = source.pipeline;
      if (!pipeline) {
        throw new Error('change-streamer source does not support pipelining');
      }

      for await (const {value, consumed} of pipeline) {
        const seq = ++this.#seq;
        sendChild(this.#child, [
          'replication-resumption:downstream',
          {
            seq,
            msg: {data: BigIntJSON.parse(value) as Downstream, json: value},
          },
        ]);
        await this.#waitForConsumed(bridge, seq);
        consumed();
      }

      if (!this.#closedFlag) {
        sendChild(this.#child, ['replication-resumption:source-end', {}]);
      }
    } catch (e) {
      if (!this.#closedFlag) {
        const message = e instanceof Error ? e.message : String(e);
        sendChild(this.#child, [
          'replication-resumption:source-error',
          {message},
        ]);
      }
    } finally {
      this.#bridges.delete(bridge);
      this.#rejectWaiters(bridge, new Error('change-streamer bridge stopped'));
      source.cancel();
    }
  }

  #waitForConsumed(bridge: ActiveBridge, seq: number): Promise<void> {
    if (this.#closedFlag) {
      throw new Error('forked replicator closed');
    }
    const r = resolver<void, Error>();
    bridge.waiters.set(seq, r);
    return r.promise;
  }

  #resolveConsumed(seq: number) {
    for (const bridge of this.#bridges) {
      const waiter = bridge.waiters.get(seq);
      if (waiter) {
        bridge.waiters.delete(seq);
        waiter.resolve();
        return;
      }
    }
  }

  #cancelBridges(err = new Error('forked replicator closed')) {
    for (const bridge of this.#bridges) {
      this.#rejectWaiters(bridge, err);
      bridge.source.cancel(err);
    }
    this.#bridges.clear();
  }

  #rejectWaiters(bridge: ActiveBridge, err: Error) {
    for (const waiter of bridge.waiters.values()) {
      waiter.reject(err);
    }
    bridge.waiters.clear();
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

    let replicator: ForkedReplicator | undefined;

    async function stopReplicator(signal: NodeJS.Signals = 'SIGTERM') {
      if (!replicator) {
        return;
      }
      const current = replicator;
      replicator = undefined;
      await current.stop(signal);
    }

    async function startReplicator(failpoint: ChildFailpoint = 'none') {
      replicator = new ForkedReplicator(
        changeStreamer,
        replicaDbFile.path,
        failpoint,
      );
      await replicator.ready();
    }

    await startReplicator();
    cleanup.push(stopReplicator);

    await eventuallyExpectReplicaMatches(upstream, replicaDbFile, 'startup');

    return {
      upstream,
      replicaDbFile,
      faultyChangeSource,
      async restartReplicator(failpoint: ChildFailpoint = 'none') {
        await stopReplicator();
        await startReplicator(failpoint);
        await eventuallyExpectReplicaMatches(
          upstream,
          replicaDbFile,
          'replicator restart',
        );
      },
      async killAtFailpoint(signal: NodeJS.Signals = 'SIGKILL') {
        if (!replicator) {
          throw new Error('replicator is not running');
        }
        const current = replicator;
        const failpoint = await current.waitForFailpoint();
        await stopReplicator(signal);
        return failpoint;
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
      await verifyReplica(description);
    }

    async function verifyReplica(description: string) {
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

      await harness.restartReplicator('after-sqlite-commit-before-ack');
      await harness.upstream.begin(async tx => {
        await tx`DELETE FROM foo WHERE id = 'b'`;
        await tx`
            UPDATE foo SET value = 'killed-after-sqlite-commit', n = n + 1
             WHERE id = 'z'`;
        await tx`
            INSERT INTO foo(id, value, n)
            VALUES ('d', 'same-pg-transaction', 4)`;
      });
      const failpoint = await harness.killAtFailpoint();
      expect(failpoint.name).toBe('after-sqlite-commit-before-ack');
      await harness.restartReplicator();
      await verifyReplica(
        'process killed after SQLite commit before downstream ACK',
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
