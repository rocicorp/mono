import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
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
import {EMPTY_COOKIE_SET} from '../replicator/change-log-cookies.ts';
import {
  changeLogFileName,
  CHANGE_LOG_META_TABLE,
  CHANGE_LOG_STREAM_TABLE,
  deleteChangeLogDB,
  estimateChangeLogStreamRowBytes,
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
import {isSampledForShard} from './shard-sampling.ts';
import {
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
  let onWriterRebuilt: () => void;

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
    onWriterRebuilt = () => {};
    logFile = new DbFile('sqlite-change-log-comparator');
    writer = new SQLiteChangeLogWriter(lc, {
      replicaFile: logFile.path,
      identity,
      now: () => seededAtMs,
      onRebuilt: () => onWriterRebuilt(),
    });
    // Resume from the Postgres watermark at the replica version.
    writer.reconcile({
      resumeWatermark: REPLICA_VERSION,
      cookies: EMPTY_COOKIE_SET,
    });

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
        // Make the log exactly one retention period old.
        now: () => seededAtMs + RETENTION_MS,
        setTimeoutFn: vi.fn() as unknown as typeof setTimeout,
        yieldFn: () => Promise.resolve(),
        ...opts,
      },
    );
  }

  /** Returns the production reader with optional overrides. */
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

  /** Opens the SQLite log for direct fault injection. */
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

  /** Reads diverged watermarks from production error logs. */
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

  /** Inserts rows that do not belong to a committed transaction. */
  function insertSQLiteRows(watermark: string) {
    withSQLiteLog(db => {
      const insert = db.prepare(/*sql*/ `
          INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
            ("watermark", "pos", "tag", "estimatedBytes", "change",
             "precommit", "writeTimeMs")
          VALUES (?, ?, ?, ?, ?, NULL, NULL)`);
      for (const [pos, tag, change] of [
        [0, 'begin', '{"tag":"begin"}'],
        [1, 'insert', '{"tag":"insert"}'],
      ] as const) {
        insert.run(
          watermark,
          pos,
          tag,
          estimateChangeLogStreamRowBytes(watermark, tag, change),
          change,
        );
      }
    });
  }

  function insertPGRows(watermark: string) {
    return sql`
      INSERT INTO ${cdc('changeLog')} ("watermark", "pos", "change", "precommit")
      VALUES (${watermark}, 0, '{"tag":"begin"}'::json, NULL),
             (${watermark}, 1, '{"tag":"insert"}'::json, NULL)`;
  }

  // Use a literal backslash-u sequence. An escaped NUL makes Postgres catchup fail.
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
    // Use small limits to require multiple batches and cycles.
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
        // Equal output matches even when no commit owns these rows.
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

  test('head lag in either direction is not divergence', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'both'})),
    );
    const t5 = tx('05', messages.insert('foo', {id: 'trailing-pg'}));
    const t6 = tx('06', messages.insert('foo', {id: 'trailing-sqlite'}));

    // SQLite leads, so compare only through the Postgres head.
    feedSQLiteOnly(t5);
    const comparator = newComparator();
    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '04',
      transactions: 2,
      matched: 2,
      mismatched: 0,
    });

    // Postgres then leads, so compare only through the SQLite head.
    await feedPGOnly(t5, t6);
    expect(await comparator.compareOnce()).toMatchObject({
      fromWatermark: '04',
      throughWatermark: '05',
      matched: 1,
      mismatched: 0,
    });

    // SQLite catches up, and the earlier skew matches.
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
    // The lower complete enumeration ends at 04, so 05 is not missing.
    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '04',
      transactions: 2,
      matched: 1,
      mismatched: 1,
    });
    expect(divergedWatermarks()).toEqual(['04']);

    // The next cycle completes the remaining range.
    const before = logSink.messages.length;
    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '06',
      matched: 2,
    });
    // Report 04 again because the log cannot serve that boundary.
    expect(divergedWatermarks(before)).toEqual(['04']);
  });

  describe('a race with the pinned bounds is inconclusive', () => {
    const cases: {
      name: string;
      pg: () => PGChangeLogRangeReader;
      transactions: number;
      matched: number;
      inconclusive: number;
    }[] = [
      {
        // Purge Postgres after the cycle reads its initial bounds, so the
        // enumeration already sees the smaller Postgres range.
        name: 'a PG purge before the enumeration',
        pg: () => {
          let purged = false;
          return pgReader({
            getCatchupBounds: async () => {
              const bounds = await storer.getCatchupBounds();
              if (!purged) {
                purged = true;
                await storer.purgeRecordsBefore('05');
              }
              return bounds;
            },
          });
        },
        // A presence check needs no payload read, so the cycle continues
        // past the purged transactions and still compares 05.
        transactions: 3,
        matched: 1,
        inconclusive: 2,
      },
      {
        // Purge Postgres once the cycle starts reading payloads, so the
        // enumeration still lists the transactions that the purge removes.
        name: 'a PG purge during a payload read',
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
        // A sampled read that cannot be reconfirmed ends the cycle, so the
        // first purged transaction is the only result.
        transactions: 0,
        matched: 0,
        inconclusive: 1,
      },
      {
        // Purge SQLite after the cycle enumerates its transactions.
        name: 'a SQLite purge during the enumeration',
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
        transactions: 0,
        matched: 0,
        inconclusive: 1,
      },
      {
        // Change the seed after the cycle enumerates its transactions.
        name: 'a reseed during the enumeration',
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
        // 03 still matches because the reseed only removes 04.
        transactions: 1,
        matched: 1,
        inconclusive: 1,
      },
    ];

    for (const {name, pg, transactions, matched, inconclusive} of cases) {
      test(name, async () => {
        await feedBoth(
          tx('03', messages.insert('foo', {id: 'boundary'})),
          tx('04', messages.insert('foo', {id: 'raced'})),
          tx('05', messages.insert('foo', {id: 'witness'})),
        );

        const result = await newComparator({pg: pg()}).compareOnce();
        expect(result).toMatchObject({
          transactions,
          matched,
          inconclusive,
          mismatched: 0,
        });
        // Bounds races do not report divergence.
        expect(divergedWatermarks()).toEqual([]);
      });
    }
  });

  test('a file rebuild invalidates a cycle before it opens another reader', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'removed-before-rebuild'})),
      tx('04', messages.insert('foo', {id: 'old-head'})),
    );

    const boundsPinned = resolver();
    const continueBounds = resolver();
    let firstBoundsRead = true;
    const comparator = newComparator({
      pg: pgReader({
        getCatchupBounds: async () => {
          if (firstBoundsRead) {
            firstBoundsRead = false;
            boundsPinned.resolve();
            await continueBounds.promise;
          }
          return storer.getCatchupBounds();
        },
      }),
    });
    onWriterRebuilt = () => comparator.invalidate();

    const comparing = comparator.compareOnce();
    await boundsPinned.promise;

    // Rebuild while the comparator waits for Postgres bounds.
    // The old database handle then points to the removed file.
    deleteFromSQLiteLog(`"watermark" = '03'`);
    writer.reconcile({
      resumeWatermark: '03',
      cookies: EMPTY_COOKIE_SET,
    });
    continueBounds.resolve();

    expect(await comparing).toEqual({
      kind: 'skipped',
      reason: 'log-rebuilt',
    });
    expect(divergedWatermarks()).toEqual([]);
  });

  test('a suspect that cannot be reconfirmed is inconclusive, then retried', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'boundary'})),
      tx('04', messages.insert('foo', {id: 'victim'})),
      tx('05', messages.insert('foo', {id: 'witness'})),
    );
    deleteFromSQLiteLog(`"watermark" = '04'`);

    // Fail the bounds recheck once, so the first result is inconclusive.
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

    // The next cycle retries and reports the persistent mismatch.
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

  test('a retried comparison samples the same transactions', async () => {
    const txs = Array.from({length: 12}, (_, i) =>
      tx((i + 3).toString(36).padStart(2, '0'), {
        ...messages.insert('foo', {id: `row-${i}`}),
      }),
    );
    await feedBoth(...txs);

    const percent = 40;
    const expected = txs.filter(({watermark}) =>
      isSampledForShard(shard, watermark, percent),
    ).length;

    // Independent comparators select the same sample.
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

    // The fresh budget in the next cycle fits the transaction.
    expect(await comparator.compareOnce()).toMatchObject({
      fromWatermark: '03',
      throughWatermark: '04',
      matched: 1,
      deferred: 0,
      mismatched: 0,
    });
  });

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

    // The cursor advances past the oversized transaction.
    expect(await comparator.compareOnce()).toMatchObject({
      fromWatermark: '03',
      throughWatermark: '04',
      matched: 1,
      oversized: 0,
    });
  });

  test('caps total catchup bytes per source and defers, then skips, a wide transaction', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'narrow'})),
      // One row wider than a whole cycle budget.
      tx('04', messages.insert('foo', {id: 'wide', pad: 'x'.repeat(5000)})),
    );
    const comparator = newComparator({maxBytesPerSourcePerCycle: 2000});

    // The narrow transaction fits and spends part of the budget. The wide one
    // cannot fit in what remains, so it waits for a fresh budget.
    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '03',
      transactions: 1,
      sampled: 2,
      matched: 1,
      deferred: 1,
      oversized: 0,
      mismatched: 0,
    });

    // A fresh budget does not fit it either, so it is skipped rather than
    // retried forever, and the cursor advances past it.
    expect(await comparator.compareOnce()).toMatchObject({
      fromWatermark: '03',
      throughWatermark: '04',
      sampled: 1,
      matched: 0,
      deferred: 0,
      oversized: 1,
      mismatched: 0,
    });
    // A byte limit is not divergence.
    expect(divergedWatermarks()).toEqual([]);
  });

  test('a generous byte budget leaves the row budget binding', async () => {
    await feedBoth(
      tx('03', messages.insert('foo', {id: 'first'})),
      tx(
        '04',
        messages.insert('foo', {id: 'second-a'}),
        messages.insert('foo', {id: 'second-b'}),
      ),
    );
    // The same expectations as the row-budget test above.
    const comparator = newComparator({
      maxRowsPerSourcePerCycle: 5,
      maxBytesPerSourcePerCycle: 1024 * 1024,
      readBatchRows: 2,
    });

    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '03',
      matched: 1,
      deferred: 1,
      oversized: 0,
    });
    expect(await comparator.compareOnce()).toMatchObject({
      fromWatermark: '03',
      throughWatermark: '04',
      matched: 1,
      deferred: 0,
      oversized: 0,
    });
  });

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

  test('a divergent cursor boundary does not wedge the comparator', async () => {
    // Remove the transaction at the common upper bound.
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

    // The next cycle starts at the missing boundary and compares later rows.
    await feedBoth(tx('06', messages.insert('foo', {id: 'next'})));

    const before = logSink.messages.length;
    expect(await comparator.compareOnce()).toMatchObject({
      throughWatermark: '06',
      matched: 1,
      mismatched: 2,
    });
    expect(divergedWatermarks(before)).toEqual(['04', '05']);

    // The cursor advanced.
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

    // Results and logs contain neither payloads nor digests.
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
    // Synthetic seed transactions are boundaries, not compared output.
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

    // Each run starts above the watermarks from earlier runs.
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
          runWriter.reconcile({
            resumeWatermark: wm(base),
            cookies: EMPTY_COOKIE_SET,
          });

          const specs = txs.map((spec, i) => ({
            ...spec,
            watermark: wm(base + (i + 1) * 10),
          }));
          // A clean sentinel keeps generated transactions below the common head.
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

          // Apply each fault and record its expected watermark.
          const expected: string[] = [];
          let expectedMatched = 1; // The sentinel matches.
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
