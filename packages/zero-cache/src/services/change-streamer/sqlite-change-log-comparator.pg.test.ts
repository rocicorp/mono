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

  /** The production reader, with one call site swapped for a fault or a race. */
  function pgReader(
    overrides: Partial<PGChangeLogRangeReader> = {},
  ): PGChangeLogRangeReader {
    return {
      getCatchupBounds: () => storer.getCatchupBounds(),
      listCommitWatermarks: (after, through, limit) =>
        storer.listCommitWatermarks(after, through, limit),
      readCatchupRange: (after, through, batchRows) =>
        storer.readCatchupRange(after, through, batchRows),
      ...overrides,
    };
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

  /**
   * The watermarks reported diverged, read back off the log line — the
   * comparison's actual production surface. The cycle result carries counts
   * only, deliberately: one collapsed `mismatch` outcome replaced the old
   * per-watermark taxonomy.
   */
  function divergedWatermarks(since = 0): string[] {
    return logSink.messages
      .slice(since)
      .filter(([level, , parts]) => level === 'error' && parts.length === 2)
      .flatMap(([, , parts]) => {
        const detail = (
          parts[1] as {
            sqliteChangeLogCompare?: {watermark: string} | undefined;
          }
        ).sqliteChangeLogCompare;
        return detail === undefined ? [] : [detail.watermark];
      });
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

  function deleteFromSQLiteLog(where: string) {
    withSQLiteLog(db =>
      db
        .prepare(
          /*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE ${where}`,
        )
        .run(),
    );
  }

  /** Rows belonging to no committed transaction, as a torn write would leave. */
  function insertSQLiteRows(watermark: string) {
    withSQLiteLog(db =>
      db
        .prepare(/*sql*/ `
            INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
              ("watermark", "pos", "change", "precommit", "writeTimeMs")
            VALUES (?, 0, '{"tag":"begin"}', NULL, NULL),
                   (?, 1, '{"tag":"insert"}', NULL, NULL)`)
        .run(watermark, watermark),
    );
  }

  function insertPGRows(watermark: string) {
    return sql`
      INSERT INTO ${cdc('changeLog')} ("watermark", "pos", "change", "precommit")
      VALUES (${watermark}, 0, '{"tag":"begin"}'::json, NULL),
             (${watermark}, 1, '{"tag":"insert"}'::json, NULL)`;
  }

  // 1. Equivalent output matches.
  //
  // The normalization pin comes first (§6.5): a transaction round-tripped
  // through both stores — SQLite's exact stored substring versus PG's json
  // column read back as text — must digest identically before any injected
  // mutation can mean anything.
  //
  // Both stores carry a synthetic seed transaction at the replica version
  // '01' (PG's from ensureReplicationConfig, SQLite's from reconcile), so the
  // comparable range starts there and every fed transaction is compared.
  //
  // The payload deliberately contains an *escaped* backslash-u sequence, not
  // an actual NUL character: `change->'tag'` in the production PG catchup
  // statement de-escapes every string value while scanning for the field, and
  // Postgres cannot convert an escaped NUL to text, so a row containing one
  // fails PG catchup itself — the comparator faithfully reports it as a
  // mismatch rather than as parity.
  test('equivalent catchup output matches, across message families and batches', async () => {
    await feedBoth(
      tx(
        '03',
        messages.insert('foo', {
          id: 'nul-\\u0000-esc',
          big: 9007199254740993n,
          float: 1.5,
          text: 'quotes " backslash \\ newline \n emoji 🙂 control ',
          nil: null,
          bool: true,
        }),
      ),
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
    expect(first).toMatchObject({
      kind: 'compared',
      fromWatermark: '01',
      throughWatermark: '04',
      transactions: 2,
      sampled: 2,
      matched: 2,
      mismatched: 0,
    });

    const second = await comparator.compareOnce();
    expect(second).toMatchObject({
      fromWatermark: '04',
      throughWatermark: '06',
      matched: 2,
      mismatched: 0,
    });

    const third = await comparator.compareOnce();
    expect(third).toMatchObject({
      fromWatermark: '06',
      throughWatermark: '07',
      matched: 1,
      mismatched: 0,
    });

    expect(await comparator.compareOnce()).toEqual({
      kind: 'skipped',
      reason: 'nothing-to-compare',
    });
    expect(divergedWatermarks()).toEqual([]);
  });

  // 2. Mutated, missing, or extra output mismatches.
  //
  // One rolling digest per served range replaced the per-transaction digest
  // map, so extra rows at any watermark — which no commit enumeration can see
  // — are caught by the same equality check as a mutated payload, and are
  // reported at the watermark of the range that served them.
  describe('divergent catchup output mismatches', () => {
    const cases: {
      name: string;
      corrupt: () => Promise<void> | void;
      diverged: string[];
    }[] = [
      {
        name: 'a mutated PG payload',
        corrupt: () =>
          mutatePGChange('04', 1, change => {
            (change.new as Record<string, unknown>).v = 999;
          }),
        diverged: ['04'],
      },
      {
        name: 'a transaction SQLite does not hold',
        corrupt: () => deleteFromSQLiteLog(`"watermark" = '04'`),
        diverged: ['04'],
      },
      {
        name: 'a transaction PG does not hold',
        corrupt: async () => {
          await sql`DELETE FROM ${cdc('changeLog')} WHERE watermark = '04'`;
        },
        diverged: ['04'],
      },
      {
        name: 'a commit row SQLite lost from an otherwise intact transaction',
        corrupt: () =>
          deleteFromSQLiteLog(`"watermark" = '04' AND "precommit" IS NOT NULL`),
        diverged: ['04'],
      },
      {
        name: 'rows only SQLite serves',
        corrupt: () => insertSQLiteRows('04z'),
        diverged: ['05'],
      },
      {
        name: 'rows only PG serves',
        corrupt: async () => {
          await insertPGRows('04z');
        },
        diverged: ['05'],
      },
      {
        // Identical output is parity in what catchup *serves* — the thing this
        // comparison measures — even where no committed transaction claims it.
        name: 'identical extra rows in both stores',
        corrupt: async () => {
          insertSQLiteRows('04z');
          await insertPGRows('04z');
        },
        diverged: [],
      },
    ];

    for (const {name, corrupt, diverged} of cases) {
      test(name, async () => {
        await feedBoth(
          tx('03', messages.insert('foo', {id: 'boundary'})),
          tx('04', messages.insert('foo', {id: 'victim', v: 1})),
          tx('05', messages.insert('foo', {id: 'witness'})),
        );
        await corrupt();

        const result = await newComparator().compareOnce();
        assert(result.kind === 'compared', 'expected a compared cycle');
        expect(divergedWatermarks()).toEqual(diverged);
        expect(result.mismatched).toBe(diverged.length);
        expect(result.inconclusive).toBe(0);
      });
    }
  });

  // 3. Head skew compares only the common range.
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
    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '04',
      transactions: 2,
      matched: 2,
      mismatched: 0,
    });

    // PG catches up and then leads: compare only through the SQLite head.
    await feedPGOnly(t5, t6);
    expect(await comparator.compareOnce()).toMatchObject({
      fromWatermark: '04',
      throughWatermark: '05',
      matched: 1,
      mismatched: 0,
    });

    // SQLite catches up: the previously skewed transaction compares clean.
    feedSQLiteOnly(t6);
    expect(await comparator.compareOnce()).toMatchObject({
      fromWatermark: '05',
      throughWatermark: '06',
      matched: 1,
      mismatched: 0,
    });
    expect(divergedWatermarks()).toEqual([]);
  });

  test('a limited cycle compares only the range both enumerations cover', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'a'})),
      tx('04', messages.insert('foo', {id: 'b'})),
      tx('05', messages.insert('foo', {id: 'c'})),
      tx('06', messages.insert('foo', {id: 'd'})),
    );
    deleteFromSQLiteLog(`"watermark" = '04'`);

    const comparator = newComparator({maxTransactionsPerCycle: 2});
    // The stores hit the limit at different watermarks — SQLite's two commits
    // reach '05', PG's reach '04' — so the cycle covers only the range both
    // enumerations completely cover. '05', cut from PG's list by the limit
    // alone, must not be reported missing.
    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '04',
      transactions: 2,
      matched: 1,
      mismatched: 1,
    });
    expect(divergedWatermarks()).toEqual(['04']);

    // The next cycle resumes above the covered range and completes it.
    const before = logSink.messages.length;
    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '06',
      matched: 2,
    });
    // '04' is re-reported once, as the boundary the log cannot serve.
    expect(divergedWatermarks(before)).toEqual(['04']);
  });

  // 4. Purge/reseed races are inconclusive.
  describe('a race with the pinned bounds is inconclusive', () => {
    const cases: {
      name: string;
      pg: () => PGChangeLogRangeReader;
      matched: number;
      inconclusive: number;
    }[] = [
      {
        // The purge lands after the cycle pinned its bounds and enumerated the
        // range, exactly as a concurrently scheduled cleanup would.
        name: 'a PG purge',
        pg: () => {
          let purged = false;
          return pgReader({
            readCatchupRange: (after, through, batchRows) =>
              (async function* (this: void) {
                if (!purged) {
                  purged = true;
                  await storer.purgeRecordsBefore('05');
                }
                yield* storer.readCatchupRange(after, through, batchRows);
              })(),
          });
        },
        matched: 1, // '05' survives the purge and matches
        inconclusive: 2,
      },
      {
        // The purger deletes whole transactions below the floor, after the
        // cycle enumerated them.
        name: 'a SQLite purge',
        pg: () =>
          pgReader({
            listCommitWatermarks: async (after, through, limit) => {
              const list = await storer.listCommitWatermarks(
                after,
                through,
                limit,
              );
              deleteFromSQLiteLog(`"watermark" < '05'`);
              return list;
            },
          }),
        matched: 1,
        inconclusive: 2,
      },
      {
        // The writer reseeds the log after the cycle pinned its bounds: '04'
        // vanishes and the meta row's seed point moves, as reconcileChangeLog's
        // reseed would. The re-read bounds alone cannot see this — only the
        // meta comparison can.
        name: 'a reseed',
        pg: () =>
          pgReader({
            listCommitWatermarks: async (after, through, limit) => {
              const list = await storer.listCommitWatermarks(
                after,
                through,
                limit,
              );
              withSQLiteLog(db => {
                db.prepare(
                  /*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" = '04'`,
                ).run();
                db.prepare(/*sql*/ `UPDATE "${CHANGE_LOG_META_TABLE}"
                      SET "seedWatermark" = '03', "seededAtMs" = "seededAtMs" + 1`).run();
              });
              return list;
            },
          }),
        matched: 2,
        inconclusive: 1,
      },
    ];

    for (const {name, pg, matched, inconclusive} of cases) {
      test(name, async () => {
        await feedBoth(
          tx('03', messages.insert('foo', {id: 'boundary'})),
          tx('04', messages.insert('foo', {id: 'raced'})),
          tx('05', messages.insert('foo', {id: 'witness'})),
        );

        const result = await newComparator({pg: pg()}).compareOnce();
        expect(result).toMatchObject({matched, inconclusive, mismatched: 0});
        // A race is not a finding: nothing reports as diverged.
        expect(divergedWatermarks()).toEqual([]);
      });
    }
  });

  test('a suspect that cannot be reconfirmed is inconclusive, then retried', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'victim'})),
      tx('05', messages.insert('foo', {id: 'witness'})),
    );
    deleteFromSQLiteLog(`"watermark" = '04'`);

    // The bounds re-read that would confirm the finding fails once; the
    // observation cannot be distinguished from a race and must not report.
    let boundsReads = 0;
    const comparator = newComparator({
      pg: pgReader({
        getCatchupBounds: () => {
          if (++boundsReads === 2) {
            throw new Error('injected bounds re-read failure');
          }
          return storer.getCatchupBounds();
        },
      }),
    });
    expect(await comparator.compareOnce()).toMatchObject({
      matched: 2,
      mismatched: 0,
      inconclusive: 1,
    });
    expect(divergedWatermarks()).toEqual([]);

    // The cursor did not move, so a later cycle retries and confirms the
    // persistent divergence once the transient bounds failure clears.
    expect(await comparator.compareOnce()).toMatchObject({mismatched: 1});
    expect(divergedWatermarks()).toEqual(['04']);
  });

  test('a read that cannot complete is a mismatch, not a crash', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'unreadable'})),
    );
    const result = await newComparator({
      pg: pgReader({
        readCatchupRange: (after, through, batchRows) => {
          if (through === '04') {
            throw new Error('injected read failure');
          }
          return storer.readCatchupRange(after, through, batchRows);
        },
      }),
    }).compareOnce();
    expect(result).toMatchObject({matched: 1, mismatched: 1});
    expect(divergedWatermarks()).toEqual(['04']);
  });

  // 5. Sampling is deterministic.
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

  // 6. Remaining-budget exhaustion defers.
  test('caps total catchup rows per source and defers an unfinished transaction', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'first'})),
      tx(
        '04',
        messages.insert('foo', {id: 'second-a'}),
        messages.insert('foo', {id: 'second-b'}),
      ),
    );
    const comparator = newComparator({
      maxRowsPerSourcePerCycle: 5,
      readBatchRows: 2,
    });

    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '03',
      transactions: 1,
      sampled: 2,
      matched: 1,
      deferred: 1,
      oversized: 0,
      mismatched: 0,
    });

    // The next cycle's fresh budget fits it.
    expect(await comparator.compareOnce()).toMatchObject({
      fromWatermark: '03',
      throughWatermark: '04',
      matched: 1,
      deferred: 0,
      mismatched: 0,
    });
  });

  // 7. A fresh-budget overflow skips.
  test('skips a transaction that exceeds a fresh cycle row budget', async () => {
    await feedBoth(
      tx(
        '03',
        messages.insert('foo', {id: 'large-a'}),
        messages.insert('foo', {id: 'large-b'}),
        messages.insert('foo', {id: 'large-c'}),
      ),
      tx('04', messages.insert('foo', {id: 'after-large'})),
    );
    const comparator = newComparator({maxRowsPerSourcePerCycle: 4});

    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '03',
      sampled: 1,
      matched: 0,
      oversized: 1,
      mismatched: 0,
    });

    // Skipped, not wedged: the cursor moved past what can never fit.
    expect(await comparator.compareOnce()).toMatchObject({
      fromWatermark: '03',
      throughWatermark: '04',
      matched: 1,
      oversized: 0,
    });
  });

  // 8. A transaction that exactly fills the budget compares.
  test('compares a transaction that exactly fills the cycle row budget', async () => {
    await feedBoth(tx('03', messages.insert('foo', {id: 'exact'})));

    expect(
      await newComparator({maxRowsPerSourcePerCycle: 3}).compareOnce(),
    ).toMatchObject({
      throughWatermark: '03',
      matched: 1,
      deferred: 0,
      oversized: 0,
    });
  });

  // 9. A confirmed mismatch at the cursor boundary still advances.
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
    deleteFromSQLiteLog(`"watermark" = '04'`);

    const comparator = newComparator();
    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '04',
      matched: 1,
      mismatched: 1,
    });
    expect(divergedWatermarks()).toEqual(['04']);

    // The stream continues, so the next cycle has a non-empty range whose
    // `from` is the divergent boundary: plan() is `too-old` and the bounds
    // have not moved. The cycle must still compare the range above it.
    await feedBoth(tx('06', messages.insert('foo', {id: 'next'})));

    const before = logSink.messages.length;
    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '06',
      matched: 1,
      mismatched: 2,
    });
    expect(divergedWatermarks(before)).toEqual(['04', '05']);

    // Progress, not a wedge.
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
    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '04',
      transactions: 2,
    });
    expect(Array.from(scheduled.values(), ({delayMs}) => delayMs)).toEqual([0]);

    comparator.stop();
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
    expect(result).toMatchObject({mismatched: 1});

    // Neither the cycle result nor anything logged for it contains the
    // payload, or any digest of it.
    expect(JSON.stringify(result)).not.toContain(sentinel);
    expect(JSON.stringify(logSink.messages.slice(before))).not.toContain(
      sentinel,
    );
    expect(divergedWatermarks(before)).toEqual(['04']);
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

    expect(await newComparator().compareOnce()).toMatchObject({
      matched: 2,
      mismatched: 0,
    });
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

  test('property: every corrupted transaction is reported, every clean one is not', async () => {
    type CorruptionKind = 'clean' | 'drop-sqlite' | 'drop-pg' | 'mutate-pg';

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
        }),
        {minLength: 1, maxLength: 5},
      ),
    });

    // Runs share the fixture's PG database; each run scopes itself with a
    // fresh SQLite log seeded above every prior run's watermarks, so prior
    // corruptions sit below its `from` and are invisible to it.
    let nextNum = 1;
    const wm = (n: number) => `w${String(n).padStart(8, '0')}`;

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

          const withRunLog = <T>(fn: (db: Database) => T): T => {
            const db = new Database(lc, changeLogFileName(runFile.path));
            try {
              return fn(db);
            } finally {
              db.close();
            }
          };

          // Corrupt, and build the expected report while doing so. Every
          // corruption is reported at its own watermark: presence for a
          // dropped transaction, a digest inequality for a mutated one.
          const expected: string[] = [];
          let expectedMatched = 1; // the sentinel
          for (const spec of specs) {
            switch (spec.kind) {
              case 'clean':
                expectedMatched++;
                break;
              case 'drop-sqlite':
                withRunLog(db =>
                  db
                    .prepare(
                      /*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" = ?`,
                    )
                    .run(spec.watermark),
                );
                expected.push(spec.watermark);
                break;
              case 'drop-pg':
                await sql`
                  DELETE FROM ${cdc('changeLog')} WHERE watermark = ${spec.watermark}`;
                expected.push(spec.watermark);
                break;
              case 'mutate-pg':
                await mutatePGChange(spec.watermark, 0, change => {
                  change.mutated = true;
                });
                expected.push(spec.watermark);
                break;
            }
          }

          const before = logSink.messages.length;
          const comparator = newComparator({
            file: changeLogFileName(runFile.path),
          });
          let matched = 0;
          for (let guard = 0; ; guard++) {
            assert(guard < 8, 'comparator failed to reach quiescence');
            const result = await comparator.compareOnce();
            if (result.kind === 'skipped') {
              expect(result.reason).toBe('nothing-to-compare');
              break;
            }
            matched += result.matched;
          }

          expect(divergedWatermarks(before).toSorted()).toEqual(
            expected.toSorted(),
          );
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
});
