import {LogContext} from '@rocicorp/logger';
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
  CHANGE_LOG_STREAM_TABLE,
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

  test('a failing reader classifies as reader-error', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'unreadable'})),
    );
    const failing: PGChangeLogRangeReader = {
      getCatchupBounds: () => storer.getCatchupBounds(),
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
