import {LogContext} from '@rocicorp/logger';
import fc from 'fast-check';
import {beforeEach, describe, expect, vi} from 'vitest';
import {assert} from '../../../../shared/src/asserts.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {test, type PgTest} from '../../test/db.ts';
import {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {cdcSchema, type ShardID} from '../../types/shards.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  changeLogFileName,
  CHANGE_LOG_META_TABLE,
  CHANGE_LOG_STREAM_TABLE,
  deleteChangeLogDB,
  type ChangeLogIdentity,
} from '../replicator/change-log-db.ts';
import {
  getSubscriptionState,
  initReplicationState,
} from '../replicator/schema/replication-state.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {serializeChangeStreamData} from './change-log-codec.ts';
import {initChangeStreamerSchema} from './schema/init.ts';
import {ensureReplicationConfig} from './schema/tables.ts';
import {
  isSampledForCompare,
  normalizeChangeJSON,
  SQLiteChangeLogComparator,
  type PGChangeLogRangeReader,
  type SQLiteChangeLogCompareOptions,
} from './sqlite-change-log-comparator.ts';
import {SQLiteChangeLogWriter} from './sqlite-change-log-writer.ts';
import {Storer} from './storer.ts';

describe('change-streamer/sqlite-change-log-comparator', () => {
  const REPLICA_VERSION = '01';
  const RETENTION_MS = 60_000;
  const shard: ShardID = {appID: 'cmp', shardNum: 7};
  const identity: ChangeLogIdentity = {
    epoch: null,
    generation: REPLICA_VERSION,
    replicaID: 'replica-id',
  };

  let lc: LogContext;
  let logSink: TestLogSink;
  let sql: PostgresDB;
  let storer: Storer;
  let storerDone: Promise<void>;
  let writer: SQLiteChangeLogWriter;
  let logFile: DbFile;
  let seededAtMs: number;

  beforeEach<PgTest>(async ({testDBs}) => {
    logSink = new TestLogSink();
    lc = new LogContext('debug', {}, logSink);

    sql = await testDBs.create('sqlite_change_log_comparator_test', {
      typeOpts: {sendStringAsJson: true},
    });

    const replica = new Database(lc, ':memory:');
    initReplicationState(replica, ['zero_data'], REPLICA_VERSION);
    const subscriptionState = getSubscriptionState(
      new StatementRunner(replica),
    );

    await initChangeStreamerSchema(lc, sql, shard);
    await ensureReplicationConfig(lc, sql, subscriptionState, shard, true);

    storer = new Storer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      REPLICA_VERSION,
      () => {},
      vi.fn(),
      {
        backPressureLimitHeapProportion: 0.04,
        statementTimeoutMs: 20_000,
        changeLogBatchSize: 2000,
      },
    );
    await storer.assumeOwnership();
    storerDone = storer.run();

    seededAtMs = 1_000_000;
    logFile = new DbFile('sqlite-change-log-comparator');
    writer = new SQLiteChangeLogWriter(lc, {
      replicaFile: logFile.path,
      identity,
      now: () => seededAtMs,
    });
    // The stream would resume from PG's lastWatermark, which
    // ensureReplicationConfig initialized to the replica version.
    writer.reconcile(REPLICA_VERSION);

    return async () => {
      await storer.stop();
      await storerDone;
      writer.close();
      logFile.delete();
      await testDBs.drop(sql);
    };
  });

  const messages = new ReplicationMessages({foo: 'id'});

  type Tx = {watermark: string; changes: ChangeStreamData[]};

  function tx(watermark: string, ...data: object[]): Tx {
    return {
      watermark,
      changes: [
        [
          'begin',
          messages.begin(),
          {commitWatermark: watermark},
        ] as unknown as ChangeStreamData,
        ...data.map(d => ['data', d] as unknown as ChangeStreamData),
        [
          'commit',
          messages.commit(),
          {watermark},
        ] as unknown as ChangeStreamData,
      ],
    };
  }

  function feedTx(t: Tx, target: {pg: boolean; sqlite: boolean}) {
    for (const change of t.changes) {
      const json = target.pg
        ? storer.store(t.watermark, change)
        : serializeChangeStreamData(change);
      if (target.sqlite) {
        writer.write(change, json);
      }
    }
  }

  async function feedBoth(...txs: Tx[]) {
    txs.forEach(t => feedTx(t, {pg: true, sqlite: true}));
    await storer.allProcessed();
  }

  async function feedPGOnly(...txs: Tx[]) {
    txs.forEach(t => feedTx(t, {pg: true, sqlite: false}));
    await storer.allProcessed();
  }

  function feedSQLiteOnly(...txs: Tx[]) {
    txs.forEach(t => feedTx(t, {pg: false, sqlite: true}));
  }

  function newComparator(
    overrides: Partial<SQLiteChangeLogCompareOptions> & {
      pg?: PGChangeLogRangeReader;
      logIdentity?: ChangeLogIdentity;
      file?: string;
    } = {},
  ): SQLiteChangeLogComparator {
    const {pg, logIdentity, file, ...opts} = overrides;
    return new SQLiteChangeLogComparator(
      lc,
      shard,
      file ?? changeLogFileName(logFile.path),
      logIdentity ?? identity,
      pg ?? storer,
      {
        comparePercent: 100,
        retentionMs: RETENTION_MS,
        // Warm by default: exactly one retention window past the seed.
        now: () => seededAtMs + RETENTION_MS,
        setTimeoutFn: vi.fn() as unknown as typeof setTimeout,
        yieldFn: () => Promise.resolve(),
        ...opts,
      },
    );
  }

  /** Direct writes to the SQLite log, as the purger or an injected fault. */
  function withSQLiteLog<T>(fn: (db: Database) => T): T {
    const db = new Database(lc, changeLogFileName(logFile.path));
    try {
      return fn(db);
    } finally {
      db.close();
    }
  }

  function cdc(table: string) {
    return sql(`${cdcSchema(shard)}.${table}`);
  }

  async function mutatePGChange(
    watermark: string,
    pos: number,
    mutate: (change: Record<string, unknown>) => void,
  ) {
    const [{change}] = await sql<{change: string}[]>`
      SELECT change::text FROM ${cdc('changeLog')}
       WHERE watermark = ${watermark} AND pos = ${pos}`;
    const parsed = JSON.parse(change) as Record<string, unknown>;
    mutate(parsed);
    const mutated = JSON.stringify(parsed);
    await sql`
      UPDATE ${cdc('changeLog')} SET change = ${mutated}::json
       WHERE watermark = ${watermark} AND pos = ${pos}`;
  }

  // The normalization pin, first (§6.5): a transaction round-tripped through
  // both stores — SQLite's exact stored substring versus PG's json column read
  // back as text — must digest identically before any injected-mutation case
  // can mean anything.
  //
  // Both stores carry a synthetic seed transaction at the replica version
  // '01' (PG's from ensureReplicationConfig, SQLite's from reconcile), so the
  // comparable range starts there and every fed transaction is compared.
  //
  // The payload deliberately contains an *escaped* backslash-u sequence, not
  // an actual NUL character: `change->'tag'` in the production PG catchup
  // statement de-escapes every string value while scanning for the field, and
  // Postgres cannot convert an escaped NUL to text, so a row containing one
  // fails PG catchup itself — the comparator faithfully reports it as
  // `reader-error` rather than as parity.
  test('round-trips one transaction through both stores with equal digests', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'first'})),
      tx(
        '04',
        messages.insert('foo', {
          id: 'nul-\\u0000-esc',
          big: 9007199254740993n,
          float: 1.5,
          text: 'quotes " backslash \\ newline \n emoji 🙂 control \u0001',
          nil: null,
          bool: true,
        }),
      ),
    );
    const result = await newComparator().compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result).toMatchObject({
      fromWatermark: '01',
      throughWatermark: '04',
      transactions: 2,
      sampled: 2,
      matched: 2,
      divergent: [],
    });
  });

  test('normalizeChangeJSON collapses formatting, preserves content', () => {
    expect(normalizeChangeJSON('{ "tag" : "insert",\n  "v": 1 }')).toBe(
      normalizeChangeJSON('{"tag":"insert","v":1}'),
    );
    // Precision above Number.MAX_SAFE_INTEGER survives the round trip.
    expect(normalizeChangeJSON('{"v":9007199254740993}')).toBe(
      '{"v":9007199254740993}',
    );
    // A value that does not parse is hashed as-is.
    expect(normalizeChangeJSON('not-json')).toBe('not-json');
  });

  test('parity across message families and multi-batch ranges', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx(
        '04',
        messages.insert('foo', {id: 'b', v: 1}),
        messages.update('foo', {id: 'b', v: 2}),
        messages.update('foo', {id: 'b2', v: 3}, {id: 'b'}),
        messages.delete('foo', {id: 'b2'}),
        messages.truncate('foo'),
      ),
      tx(
        '05',
        messages.createTable({
          schema: 'public',
          name: 'baz',
          columns: {id: {pos: 0, dataType: 'varchar'}},
          primaryKey: ['id'],
        }),
      ),
      tx('06', messages.addColumn('foo', 'extra', {dataType: 'text', pos: 9})),
      tx('07', messages.dropColumn('foo', 'extra'), messages.dropTable('baz')),
    );
    // readBatchRows 2 forces multiple SQLite read batches per transaction;
    // maxTransactionsPerCycle 2 forces the comparison across cycles.
    const comparator = newComparator({
      readBatchRows: 2,
      maxTransactionsPerCycle: 2,
    });

    const first = await comparator.compareOnce();
    assert(first.kind === 'compared', 'expected a compared cycle');
    expect(first).toMatchObject({
      fromWatermark: '01',
      throughWatermark: '04',
      transactions: 2,
      sampled: 2,
      matched: 2,
      divergent: [],
    });

    const second = await comparator.compareOnce();
    assert(second.kind === 'compared', 'expected a compared cycle');
    expect(second).toMatchObject({
      fromWatermark: '04',
      throughWatermark: '06',
      transactions: 2,
      sampled: 2,
      matched: 2,
      divergent: [],
    });

    const third = await comparator.compareOnce();
    assert(third.kind === 'compared', 'expected a compared cycle');
    expect(third).toMatchObject({
      fromWatermark: '06',
      throughWatermark: '07',
      transactions: 1,
      sampled: 1,
      matched: 1,
      divergent: [],
    });

    expect(await comparator.compareOnce()).toEqual({
      kind: 'skipped',
      reason: 'nothing-to-compare',
    });
  });

  test('a capped cycle schedules its continuation without the poll delay', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'a'})),
      tx('04', messages.insert('foo', {id: 'b'})),
      tx('05', messages.insert('foo', {id: 'c'})),
    );

    type Scheduled = {callback: () => void; delayMs: number};
    const scheduled = new Map<ReturnType<typeof setTimeout>, Scheduled>();
    let nextTimer = 0;
    const setTimeoutFn = vi.fn((callback: () => void, delayMs?: number) => {
      const handle = ++nextTimer as unknown as ReturnType<typeof setTimeout>;
      scheduled.set(handle, {callback, delayMs: delayMs ?? 0});
      return handle;
    }) as unknown as typeof setTimeout;
    const clearTimeoutFn = vi.fn((handle: ReturnType<typeof setTimeout>) => {
      scheduled.delete(handle);
    }) as unknown as typeof clearTimeout;
    const comparator = newComparator({
      intervalMs: 30_000,
      maxTransactionsPerCycle: 2,
      setTimeoutFn,
      clearTimeoutFn,
    });

    expect(Array.from(scheduled.values(), ({delayMs}) => delayMs)).toEqual([
      30_000,
    ]);
    const first = await comparator.compareOnce();
    assert(first.kind === 'compared', 'expected a compared cycle');
    expect(first).toMatchObject({
      throughWatermark: '04',
      transactions: 2,
    });
    expect(Array.from(scheduled.values(), ({delayMs}) => delayMs)).toEqual([0]);

    comparator.stop();
  });

  test('head lag in either direction is not divergence', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'both'})),
    );
    const t5 = tx('05', messages.insert('foo', {id: 'trailing-pg'}));
    const t6 = tx('06', messages.insert('foo', {id: 'trailing-sqlite'}));

    // The SQLite log leads, as it does in production: compare only through
    // the PG head.
    feedSQLiteOnly(t5);
    const comparator = newComparator();
    let result = await comparator.compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result).toMatchObject({
      throughWatermark: '04',
      transactions: 2,
      matched: 2,
      divergent: [],
    });

    // PG catches up and then leads: compare only through the SQLite head.
    await feedPGOnly(t5, t6);
    result = await comparator.compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result).toMatchObject({
      fromWatermark: '04',
      throughWatermark: '05',
      transactions: 1,
      matched: 1,
      divergent: [],
    });

    // SQLite catches up: the previously skewed transaction compares clean.
    feedSQLiteOnly(t6);
    result = await comparator.compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result).toMatchObject({
      fromWatermark: '05',
      throughWatermark: '06',
      transactions: 1,
      matched: 1,
      divergent: [],
    });
  });

  test('a mutated PG payload classifies as digest-mismatch', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'victim', v: 1})),
      tx('05', messages.insert('foo', {id: 'witness'})),
    );
    await mutatePGChange('04', 1, change => {
      (change.new as Record<string, unknown>).v = 999;
    });

    const result = await newComparator().compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result).toMatchObject({
      transactions: 3,
      sampled: 3,
      matched: 2,
    });
    expect(result.divergent).toMatchObject([
      {watermark: '04', outcome: 'digest-mismatch'},
    ]);
    const [divergent] = result.divergent;
    expect(divergent.sqliteDigest).toHaveLength(16);
    expect(divergent.pgDigest).toHaveLength(16);
    expect(divergent.sqliteDigest).not.toBe(divergent.pgDigest);
    expect(divergent.sqliteRows).toBe(3);
    expect(divergent.pgRows).toBe(3);
  });

  test('a transaction PG does not hold classifies as missing-pg', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'victim'})),
      tx('05', messages.insert('foo', {id: 'witness'})),
    );
    await sql`DELETE FROM ${cdc('changeLog')} WHERE watermark = '04'`;

    const result = await newComparator().compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result.divergent).toMatchObject([
      {watermark: '04', outcome: 'missing-pg'},
    ]);
    expect(result.matched).toBe(2); // '03' and '05' still match
  });

  test('a transaction SQLite does not serve classifies as missing-sqlite', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'victim'})),
      tx('05', messages.insert('foo', {id: 'witness'})),
    );
    withSQLiteLog(db =>
      db
        .prepare(
          /*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" = '04'`,
        )
        .run(),
    );

    const result = await newComparator().compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result.divergent).toMatchObject([
      {watermark: '04', outcome: 'missing-sqlite'},
    ]);
    expect(result.matched).toBe(2);
  });

  test('rows outside any committed transaction classify as orphan-output', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'a'})),
      tx('04', messages.insert('foo', {id: 'b'})),
      tx('05', messages.insert('foo', {id: 'c'})),
    );
    // A torn/corrupt remnant only SQLite serves: begin + data with no commit
    // row, invisible to the commit enumeration but served to any subscriber
    // catching up across it. '03b' is the same remnant in *both* stores,
    // identically — parity in what catchup serves, so not a finding.
    withSQLiteLog(db =>
      db.exec(/*sql*/ `
        INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
          ("watermark", "pos", "change", "precommit", "writeTimeMs")
        VALUES
          ('03a', 0, '{"tag":"begin"}', NULL, NULL),
          ('03a', 1, '{"tag":"insert"}', NULL, NULL),
          ('03b', 0, '{"tag":"begin"}', NULL, NULL),
          ('03b', 1, '{"tag":"insert"}', NULL, NULL)
      `),
    );
    await sql`
      INSERT INTO ${cdc('changeLog')} ("watermark", "pos", "change", "precommit")
      VALUES ('03b', 0, '{"tag":"begin"}'::json, NULL),
             ('03b', 1, '{"tag":"insert"}'::json, NULL),
             ('04a', 0, '{"tag":"begin"}'::json, NULL),
             ('04a', 1, '{"tag":"insert"}'::json, NULL)`;

    const result = await newComparator().compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    // The asymmetric orphans are reported at their own watermarks; the
    // transactions themselves all still match.
    expect(result.divergent).toMatchObject([
      {watermark: '03a', outcome: 'orphan-output', sqliteRows: 2},
      {watermark: '04a', outcome: 'orphan-output', pgRows: 2},
    ]);
    expect(result.matched).toBe(3);
    const [sqliteOrphan, pgOrphan] = result.divergent;
    expect(sqliteOrphan.sqliteDigest).toHaveLength(16);
    expect(sqliteOrphan.pgDigest).toBeUndefined();
    expect(pgOrphan.pgDigest).toHaveLength(16);
    expect(pgOrphan.sqliteDigest).toBeUndefined();
  });

  test('a missing catchup boundary classifies as bounds-mismatch', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'cursor-boundary'})),
    );
    const comparator = newComparator();
    const first = await comparator.compareOnce();
    assert(first.kind === 'compared', 'expected a compared cycle');
    expect(first).toMatchObject({throughWatermark: '04', matched: 2});

    // The cursor now rests on '04', whose commit row is the boundary the next
    // cycle's plan() must find. Removing it makes the log unable to serve a
    // range it claims to hold.
    withSQLiteLog(db =>
      db
        .prepare(/*sql*/ `
            DELETE FROM "${CHANGE_LOG_STREAM_TABLE}"
             WHERE "watermark" = '04' AND "precommit" IS NOT NULL`)
        .run(),
    );
    await feedBoth(tx('05', messages.insert('foo', {id: 'next'})));

    const second = await comparator.compareOnce();
    assert(second.kind === 'compared', 'expected a compared cycle');
    expect(second.divergent).toMatchObject([
      {watermark: '04', outcome: 'bounds-mismatch'},
    ]);
    // The finding is about the boundary, not the range above it: the same
    // cycle still compares ('04', '05'] and moves the cursor past the damage.
    expect(second).toMatchObject({throughWatermark: '05', matched: 1});

    // The next cycle starts above the bad boundary rather than re-reporting
    // the identical bounds-mismatch forever.
    const third = await comparator.compareOnce();
    expect(third).toEqual({kind: 'skipped', reason: 'nothing-to-compare'});
  });

  test('a missing PG catchup boundary prevents accepting the later range', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'older'})),
      tx('04', messages.insert('foo', {id: 'boundary'})),
      tx('05', messages.insert('foo', {id: 'later'})),
    );
    // SQLite has purged to '04', while PG still has older and later rows but
    // has a hole at that exact catchup boundary.
    withSQLiteLog(db =>
      db
        .prepare(
          /*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" < '04'`,
        )
        .run(),
    );
    await sql`DELETE FROM ${cdc('changeLog')} WHERE watermark = '04'`;

    const comparator = newComparator();
    const first = await comparator.compareOnce();
    assert(first.kind === 'compared', 'expected a compared cycle');
    expect(first).toMatchObject({
      fromWatermark: '04',
      throughWatermark: '04',
      transactions: 0,
      sampled: 0,
      matched: 0,
      divergent: [{watermark: '04', outcome: 'bounds-mismatch'}],
    });

    // The later transaction is not accepted and the invalid boundary remains
    // the start of the next attempt.
    const second = await comparator.compareOnce();
    assert(second.kind === 'compared', 'expected a compared cycle');
    expect(second).toMatchObject({
      fromWatermark: '04',
      throughWatermark: '04',
      matched: 0,
    });
  });

  test('a divergent cursor boundary does not wedge the comparator', async () => {
    // SQLite loses a whole transaction PG holds, while the log runs ahead of
    // PG (its normal state), so the cycle's common upper bound — and with it
    // the cursor — lands exactly on the commit the log cannot serve as a
    // catchup boundary.
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'sqlite-lost'})),
    );
    feedSQLiteOnly(tx('05', messages.insert('foo', {id: 'log-leads'})));
    withSQLiteLog(db =>
      db
        .prepare(
          /*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" = '04'`,
        )
        .run(),
    );

    const comparator = newComparator();
    const first = await comparator.compareOnce();
    assert(first.kind === 'compared', 'expected a compared cycle');
    expect(first).toMatchObject({throughWatermark: '04', matched: 1});
    expect(first.divergent).toMatchObject([
      {watermark: '04', outcome: 'missing-sqlite'},
    ]);

    // The stream continues, so the next cycle has a non-empty range whose
    // `from` is the divergent boundary: plan() is `too-old` and the bounds
    // have not moved.
    await feedBoth(tx('06', messages.insert('foo', {id: 'next'})));

    const second = await comparator.compareOnce();
    assert(second.kind === 'compared', 'expected a compared cycle');
    expect(second.divergent).toMatchObject([
      {watermark: '04', outcome: 'bounds-mismatch'},
      {watermark: '05', outcome: 'missing-pg'},
    ]);
    expect(second).toMatchObject({throughWatermark: '06', matched: 1});

    // Progress, not a wedge.
    const third = await comparator.compareOnce();
    expect(third).toEqual({kind: 'skipped', reason: 'nothing-to-compare'});
  });

  test('a limited cycle over divergent stores compares only the common covered range', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'a'})),
      tx('04', messages.insert('foo', {id: 'b'})),
      tx('05', messages.insert('foo', {id: 'c'})),
      tx('06', messages.insert('foo', {id: 'd'})),
    );
    withSQLiteLog(db =>
      db
        .prepare(
          /*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" = '04'`,
        )
        .run(),
    );

    const comparator = newComparator({maxTransactionsPerCycle: 2});
    const first = await comparator.compareOnce();
    assert(first.kind === 'compared', 'expected a compared cycle');
    // The stores hit the limit at different watermarks — SQLite's two commits
    // reach '05', PG's reach '04' — so the cycle covers only the range both
    // enumerations completely cover. '05', cut from PG's list by the limit
    // alone, must not be reported missing.
    expect(first).toMatchObject({
      throughWatermark: '04',
      transactions: 2,
      matched: 1,
    });
    expect(first.divergent).toMatchObject([
      {watermark: '04', outcome: 'missing-sqlite'},
    ]);

    // The next cycle resumes above the covered range and completes it.
    const second = await comparator.compareOnce();
    assert(second.kind === 'compared', 'expected a compared cycle');
    expect(second).toMatchObject({throughWatermark: '06', matched: 2});
    expect(second.divergent).toMatchObject([
      {watermark: '04', outcome: 'bounds-mismatch'},
    ]);
  });

  test('a reseed racing the comparison yields inconclusive', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'reseeded-away'})),
      tx('05', messages.insert('foo', {id: 'witness'})),
    );
    // The writer reseeds the log after the cycle pinned its bounds and
    // enumerated the range: '04' vanishes and the meta row's seed point
    // moves, as reconcileChangeLog's reseed would. The re-read bounds alone
    // cannot see this — only the meta comparison can.
    const racing: PGChangeLogRangeReader = {
      getCatchupBounds: () => storer.getCatchupBounds(),
      hasCatchupWatermark: w => storer.hasCatchupWatermark(w),
      listCommitWatermarks: async (after, through, limit) => {
        const list = await storer.listCommitWatermarks(after, through, limit);
        withSQLiteLog(db => {
          db.prepare(
            /*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" = '04'`,
          ).run();
          db.prepare(/*sql*/ `UPDATE "${CHANGE_LOG_META_TABLE}"
                SET "seedWatermark" = '03', "seededAtMs" = "seededAtMs" + 1`).run();
        });
        return list;
      },
      readCatchupRange: (after, through) =>
        storer.readCatchupRange(after, through),
    };
    const result = await newComparator({pg: racing}).compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result.divergent).toMatchObject([
      {watermark: '04', outcome: 'inconclusive'},
    ]);
    expect(result.matched).toBe(2);
    // A reseed is a race, not a finding: nothing reports as diverged.
    expect(
      logSink.messages.filter(
        ([level, , parts]) =>
          level === 'error' && JSON.stringify(parts).includes('diverged'),
      ),
    ).toEqual([]);
  });

  test('a suspect that cannot be reconfirmed is inconclusive', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'victim'})),
      tx('05', messages.insert('foo', {id: 'witness'})),
    );
    withSQLiteLog(db =>
      db
        .prepare(
          /*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" = '04'`,
        )
        .run(),
    );
    // The first bounds re-read that would confirm the finding fails; the
    // observation cannot be distinguished from a race and must not report.
    let boundsReads = 0;
    const failing: PGChangeLogRangeReader = {
      getCatchupBounds: () => {
        if (++boundsReads === 2) {
          throw new Error('injected bounds re-read failure');
        }
        return storer.getCatchupBounds();
      },
      hasCatchupWatermark: w => storer.hasCatchupWatermark(w),
      listCommitWatermarks: (after, through, limit) =>
        storer.listCommitWatermarks(after, through, limit),
      readCatchupRange: (after, through) =>
        storer.readCatchupRange(after, through),
    };
    const comparator = newComparator({pg: failing});
    const result = await comparator.compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result.divergent).toMatchObject([
      {watermark: '04', outcome: 'inconclusive'},
    ]);
    expect(result.matched).toBe(2);

    // The cursor did not move, so a later cycle retries and confirms the
    // persistent divergence once the transient bounds failure clears.
    const retry = await comparator.compareOnce();
    assert(retry.kind === 'compared', 'expected a compared cycle');
    expect(retry.divergent).toMatchObject([
      {watermark: '04', outcome: 'missing-sqlite'},
    ]);
  });

  test('a failing reader classifies as reader-error', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'unreadable'})),
    );
    const failing: PGChangeLogRangeReader = {
      getCatchupBounds: () => storer.getCatchupBounds(),
      hasCatchupWatermark: w => storer.hasCatchupWatermark(w),
      listCommitWatermarks: (after, through, limit) =>
        storer.listCommitWatermarks(after, through, limit),
      readCatchupRange: (after, through) => {
        if (through === '04') {
          throw new Error('injected read failure');
        }
        return storer.readCatchupRange(after, through);
      },
    };
    const result = await newComparator({pg: failing}).compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result.divergent).toMatchObject([
      {watermark: '04', outcome: 'reader-error'},
    ]);
    expect(result.matched).toBe(1); // '03' read cleanly and matched
  });

  test('a PG purge racing the comparison yields inconclusive', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'purged-mid-compare'})),
      tx('05', messages.insert('foo', {id: 'witness'})),
    );
    // The purge lands after the cycle pinned its bounds and enumerated the
    // range, exactly as a concurrently scheduled cleanup would.
    let purged = false;
    const racing: PGChangeLogRangeReader = {
      getCatchupBounds: () => storer.getCatchupBounds(),
      hasCatchupWatermark: w => storer.hasCatchupWatermark(w),
      listCommitWatermarks: (after, through, limit) =>
        storer.listCommitWatermarks(after, through, limit),
      readCatchupRange: (after, through) =>
        (async function* (this: void) {
          if (!purged) {
            purged = true;
            await storer.purgeRecordsBefore('05');
          }
          yield* storer.readCatchupRange(after, through);
        })(),
    };
    const result = await newComparator({pg: racing}).compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result.divergent).toMatchObject([
      {watermark: '03', outcome: 'inconclusive'},
      {watermark: '04', outcome: 'inconclusive'},
    ]);
    expect(result.matched).toBe(1); // '05' survives the purge and matches
    // Inconclusive is a race, not a finding: nothing is reported as diverged.
    expect(
      logSink.messages.filter(
        ([level, , parts]) =>
          level === 'error' && JSON.stringify(parts).includes('diverged'),
      ),
    ).toEqual([]);
  });

  test('a SQLite purge racing the comparison yields inconclusive', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'purged-mid-compare'})),
      tx('05', messages.insert('foo', {id: 'witness'})),
    );
    // Delete the low transactions after the cycle enumerates them, as the
    // purger (which deletes whole transactions below the floor) would.
    const racing: PGChangeLogRangeReader = {
      getCatchupBounds: () => storer.getCatchupBounds(),
      hasCatchupWatermark: w => storer.hasCatchupWatermark(w),
      listCommitWatermarks: async (after, through, limit) => {
        const list = await storer.listCommitWatermarks(after, through, limit);
        withSQLiteLog(db =>
          db
            .prepare(
              /*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" < '05'`,
            )
            .run(),
        );
        return list;
      },
      readCatchupRange: (after, through) =>
        storer.readCatchupRange(after, through),
    };
    const result = await newComparator({pg: racing}).compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result.divergent).toMatchObject([
      {watermark: '03', outcome: 'inconclusive'},
      {watermark: '04', outcome: 'inconclusive'},
    ]);
    expect(result.matched).toBe(1);
  });

  test('sampling is stable by shard and watermark', () => {
    const watermarks = Array.from({length: 64}, (_, i) =>
      (i + 3).toString(36).padStart(2, '0'),
    );
    for (const percent of [0, 25, 60, 100]) {
      for (const w of watermarks) {
        const sampled = isSampledForCompare(shard, w, percent);
        // Deterministic.
        expect(isSampledForCompare(shard, w, percent)).toBe(sampled);
        // Monotone in percent: raising the sample never drops a member.
        if (sampled) {
          expect(
            isSampledForCompare(shard, w, Math.min(100, percent + 20)),
          ).toBe(true);
        }
      }
      const count = watermarks.filter(w =>
        isSampledForCompare(shard, w, percent),
      ).length;
      if (percent === 0) {
        expect(count).toBe(0);
      } else if (percent === 100) {
        expect(count).toBe(watermarks.length);
      } else {
        expect(count).toBeGreaterThan(0);
        expect(count).toBeLessThan(watermarks.length);
      }
    }
  });

  test('a retried comparison samples the same transactions', async () => {
    const txs = Array.from({length: 12}, (_, i) =>
      tx((i + 3).toString(36).padStart(2, '0'), {
        ...messages.insert('foo', {id: `row-${i}`}),
      }),
    );
    await feedBoth(...txs);

    const percent = 40;
    const expected = txs.filter(({watermark}) =>
      isSampledForCompare(shard, watermark, percent),
    ).length;

    // Two independent comparators — as across a restart — pin the same range
    // and select exactly the same sample.
    const first = await newComparator({comparePercent: percent}).compareOnce();
    const second = await newComparator({comparePercent: percent}).compareOnce();
    assert(
      first.kind === 'compared' && second.kind === 'compared',
      'expected compared cycles',
    );
    expect(first.sampled).toBe(expected);
    expect(second.sampled).toBe(expected);
    expect(second.fromWatermark).toBe(first.fromWatermark);
    expect(second.throughWatermark).toBe(first.throughWatermark);
    expect(first.matched).toBe(first.sampled);
    expect(second.matched).toBe(second.sampled);
  });

  test('divergence reports carry no payload data', async () => {
    const sentinel = 'SENSITIVE-PAYLOAD-8675309';
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: sentinel, secret: sentinel})),
    );
    await mutatePGChange('04', 1, change => {
      (change.new as Record<string, unknown>).secret = `${sentinel}-mutated`;
    });

    const before = logSink.messages.length;
    const result = await newComparator().compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result.divergent).toMatchObject([
      {watermark: '04', outcome: 'digest-mismatch'},
    ]);

    // Neither the cycle result nor anything logged for it contains the
    // payload; digests are truncated prefixes.
    expect(JSON.stringify(result)).not.toContain(sentinel);
    expect(JSON.stringify(logSink.messages.slice(before))).not.toContain(
      sentinel,
    );
    expect(result.divergent[0].sqliteDigest).toHaveLength(16);
  });

  test('declines a cold log, a wrong identity, and an absent file', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'content'})),
    );

    expect(
      await newComparator({
        now: () => seededAtMs + RETENTION_MS - 1,
      }).compareOnce(),
    ).toEqual({kind: 'skipped', reason: 'cold-log'});

    expect(
      await newComparator({
        logIdentity: {...identity, replicaID: 'someone-else'},
      }).compareOnce(),
    ).toEqual({kind: 'skipped', reason: 'ineligible-identity'});

    expect(
      await newComparator({file: `${logFile.path}-absent`}).compareOnce(),
    ).toEqual({kind: 'skipped', reason: 'log-unavailable'});

    const result = await newComparator().compareOnce();
    assert(result.kind === 'compared', 'expected a compared cycle');
    expect(result).toMatchObject({matched: 2, divergent: []});
  });

  test('a freshly seeded log with no traffic has nothing to compare', async () => {
    // Both stores hold only their synthetic seed transactions at '01'. Seeds
    // are catchup boundaries, never compared output — and SQLite's seed is
    // never reported as missing from PG.
    expect(await newComparator().compareOnce()).toEqual({
      kind: 'skipped',
      reason: 'nothing-to-compare',
    });
  });

  test('property: injected corruptions are reported exactly, then quiescence', async () => {
    type CorruptionKind = 'clean' | 'drop-sqlite' | 'drop-pg' | 'mutate-pg';
    type OrphanSide = 'sqlite' | 'pg' | 'both';

    const faultScenario = fc.record({
      txs: fc.array(
        fc.record({
          width: fc.integer({min: 0, max: 3}),
          kind: fc.constantFrom<CorruptionKind>(
            'clean',
            'drop-sqlite',
            'drop-pg',
            'mutate-pg',
          ),
          orphanAfter: fc.option(
            fc.constantFrom<OrphanSide>('sqlite', 'pg', 'both'),
            {nil: undefined},
          ),
        }),
        {minLength: 1, maxLength: 5},
      ),
    });

    // Runs share the fixture's PG database; each run scopes itself with a
    // fresh SQLite log seeded above every prior run's watermarks, so prior
    // corruptions sit below its `from` and are invisible to it.
    let nextNum = 1;
    const wm = (n: number) => `w${String(n).padStart(8, '0')}`;

    const withRunLog = <T>(file: DbFile, fn: (db: Database) => T): T => {
      const db = new Database(lc, changeLogFileName(file.path));
      try {
        return fn(db);
      } finally {
        db.close();
      }
    };
    const insertSQLiteOrphan = (file: DbFile, watermark: string) =>
      withRunLog(file, db =>
        db
          .prepare(/*sql*/ `
              INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
                ("watermark", "pos", "change", "precommit", "writeTimeMs")
              VALUES (?, 0, '{"tag":"begin"}', NULL, NULL),
                     (?, 1, '{"tag":"insert"}', NULL, NULL)`)
          .run(watermark, watermark),
      );
    const insertPGOrphan = (watermark: string) => sql`
      INSERT INTO ${cdc('changeLog')} ("watermark", "pos", "change", "precommit")
      VALUES (${watermark}, 0, '{"tag":"begin"}'::json, NULL),
             (${watermark}, 1, '{"tag":"insert"}'::json, NULL)`;

    await fc.assert(
      fc.asyncProperty(faultScenario, async ({txs}) => {
        const base = nextNum;
        nextNum += (txs.length + 2) * 10;

        const runFile = new DbFile('sqlite-change-log-comparator-fuzz');
        const runWriter = new SQLiteChangeLogWriter(lc, {
          replicaFile: runFile.path,
          identity,
          now: () => seededAtMs,
        });
        try {
          runWriter.reconcile(wm(base));

          const specs = txs.map((spec, i) => ({
            ...spec,
            watermark: wm(base + (i + 1) * 10),
            orphanAt: wm(base + (i + 1) * 10 + 5),
          }));
          // A clean sentinel pins the common head, so every generated
          // transaction lies strictly inside the compared range — head skew
          // is deliberately never reported, and is not what this pins.
          const sentinel = wm(base + (txs.length + 1) * 10);

          const feedRun = (t: Tx) => {
            for (const change of t.changes) {
              runWriter.write(change, storer.store(t.watermark, change));
            }
          };
          // Production seeds at PG's durable head, which must be a valid
          // catchup boundary. Give each generated run the same invariant;
          // SQLite's synthetic seed remains excluded from the compared range.
          const boundary = tx(
            wm(base),
            messages.insert('foo', {id: `boundary-${base}`}),
          );
          for (const change of boundary.changes) {
            storer.store(boundary.watermark, change);
          }
          for (const spec of specs) {
            feedRun(
              tx(
                spec.watermark,
                ...Array.from({length: spec.width}, (_, k) =>
                  messages.insert('foo', {id: `${spec.watermark}-${k}`}),
                ),
              ),
            );
          }
          feedRun(tx(sentinel, messages.insert('foo', {id: sentinel})));
          await storer.allProcessed();

          // Corrupt, and build the expected report while doing so.
          const expected: {watermark: string; outcome: string}[] = [];
          let expectedMatched = 1; // the sentinel
          for (const [i, spec] of specs.entries()) {
            // An orphan is served inside the range of the next transaction
            // up; when one store does not hold that transaction, the range
            // is never read and the orphan is unobservable.
            const upper = specs[i + 1];
            const upperIntact =
              upper === undefined ||
              (upper.kind !== 'drop-sqlite' && upper.kind !== 'drop-pg');
            switch (spec.kind) {
              case 'clean':
                expectedMatched++;
                break;
              case 'drop-sqlite':
                withRunLog(runFile, db =>
                  db
                    .prepare(
                      /*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" = ?`,
                    )
                    .run(spec.watermark),
                );
                expected.push({
                  watermark: spec.watermark,
                  outcome: 'missing-sqlite',
                });
                break;
              case 'drop-pg':
                await sql`
                  DELETE FROM ${cdc('changeLog')} WHERE watermark = ${spec.watermark}`;
                expected.push({
                  watermark: spec.watermark,
                  outcome: 'missing-pg',
                });
                break;
              case 'mutate-pg':
                await mutatePGChange(spec.watermark, 0, change => {
                  change.mutated = true;
                });
                expected.push({
                  watermark: spec.watermark,
                  outcome: 'digest-mismatch',
                });
                break;
            }
            if (spec.orphanAfter !== undefined) {
              if (spec.orphanAfter !== 'pg') {
                insertSQLiteOrphan(runFile, spec.orphanAt);
              }
              if (spec.orphanAfter !== 'sqlite') {
                await insertPGOrphan(spec.orphanAt);
              }
              // Identical orphan output in both stores is parity in what
              // catchup serves, never a finding.
              if (spec.orphanAfter !== 'both' && upperIntact) {
                expected.push({
                  watermark: spec.orphanAt,
                  outcome: 'orphan-output',
                });
              }
            }
          }

          // Drive to quiescence in one complete range, whose clean sentinel is
          // a valid boundary in both stores. Capped-range behavior is covered
          // separately: allowing a cap to land on a PG hole would correctly
          // keep rejecting that boundary, just as authoritative catchup does.
          const comparator = newComparator({
            file: changeLogFileName(runFile.path),
          });
          const divergent: {watermark: string; outcome: string}[] = [];
          let matched = 0;
          for (let guard = 0; ; guard++) {
            assert(guard < 8, 'comparator failed to reach quiescence');
            const result = await comparator.compareOnce();
            if (result.kind === 'skipped') {
              expect(result.reason).toBe('nothing-to-compare');
              break;
            }
            matched += result.matched;
            divergent.push(
              ...result.divergent.map(({watermark, outcome}) => ({
                watermark,
                outcome,
              })),
            );
          }

          const order = (
            a: {watermark: string; outcome: string},
            b: {watermark: string; outcome: string},
          ) =>
            a.watermark < b.watermark
              ? -1
              : a.watermark > b.watermark
                ? 1
                : a.outcome.localeCompare(b.outcome);
          expect(divergent.toSorted(order)).toEqual(expected.toSorted(order));
          expect(matched).toBe(expectedMatched);
        } finally {
          runWriter.close();
          deleteChangeLogDB(runFile.path);
          runFile.delete();
        }
      }),
      {numRuns: 10},
    );
  });

  test('stop() ends the comparator', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'content'})),
    );
    const comparator = newComparator();
    comparator.stop();
    expect(await comparator.compareOnce()).toEqual({
      kind: 'skipped',
      reason: 'stopped',
    });
  });
});
