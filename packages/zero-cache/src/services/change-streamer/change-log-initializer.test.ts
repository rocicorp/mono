/**
 * The change log's initialization comparator, driven the way the stream loop
 * drives it.
 *
 * The replica-derived side is real: an actual {@link ChangeProcessor} folding an
 * actual change stream into an actual replica, stopped short of the log's head
 * so that the fold has something to do. The Postgres-derived side is the change
 * log's own cookie set, which `change-log-cookies.pg.test.ts` pins as identical
 * to Postgres's — so what is under test here is the third interpreter and the
 * fold that joins the two positions, without needing a shard.
 */

import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile} from '../../test/lite.ts';
import type {SchemaChange} from '../change-source/protocol/current/data.ts';
import {readCookies} from '../replicator/change-log-cookies.ts';
import {CHANGE_LOG_STREAM_TABLE} from '../replicator/change-log-db.ts';
import {BACKFILLING_TABLE} from '../replicator/schema/backfilling.ts';
import {
  initReplicationState,
  updateReplicationWatermark,
} from '../replicator/schema/replication-state.ts';
import {
  ChangeLogInitializer,
  readReplicaInitializationParameters,
  replicaInitializationSource,
  type InitializationParameters,
} from './change-log-initializer.ts';
import {
  ChangeStreamDriver,
  createChangeLog,
  createReplica,
  REPLICA_VERSION,
} from './change-log-test-utils.ts';

const lc = createSilentLogContext();

describe('change-streamer/change-log-initializer', () => {
  let changeLog: Database;
  let replica: Database;
  let driver: ChangeStreamDriver;

  beforeEach(() => {
    changeLog = createChangeLog(lc);
    const {replica: db, processor} = createReplica(lc);
    replica = db;
    // No shard: the change log stands in for Postgres, per the header.
    driver = new ChangeStreamDriver(lc, {changeLog, replica: processor});

    return () => {
      replica.close();
      changeLog.close();
    };
  });

  /**
   * Drives one transaction into the change log, and -- unless the replica is
   * being held back to simulate a replicator that trails -- into the replica.
   */
  const transaction = (changes: SchemaChange[], opts?: {toReplica?: boolean}) =>
    driver.transaction(changes, opts);

  /**
   * What Postgres would have derived, taken from the change log's own fold of
   * the same stream. `backfillRequests` is not part of the comparison -- the
   * cookie set it is projected from is strictly more -- so it is left empty.
   */
  function pgParameters(): InitializationParameters {
    const {head} = changeLog
      .prepare(/*sql*/ `SELECT max("watermark") AS "head"
                   FROM "${CHANGE_LOG_STREAM_TABLE}"`)
      .get<{head: string}>();
    return {
      lastWatermark: head,
      backfillRequests: [],
      cookies: readCookies(changeLog),
    };
  }

  function initializer(
    opts: {
      initFromPgChangeLog?: boolean;
      initFromReplica?: boolean;
      pg?: () => Promise<InitializationParameters>;
      replica?: () => InitializationParameters;
      changeLog?: () => Database | undefined;
    } = {},
  ) {
    return new ChangeLogInitializer(
      lc,
      {
        initFromPgChangeLog: opts.initFromPgChangeLog ?? true,
        initFromReplica: opts.initFromReplica ?? true,
      },
      {
        pgChangeLog: opts.pg ?? (() => Promise.resolve(pgParameters())),
        replica:
          opts.replica ?? (() => readReplicaInitializationParameters(replica)),
        changeLog: opts.changeLog ?? (() => changeLog),
      },
    );
  }

  /** The stream loop's order: initialize, reconcile, then compare. */
  async function compare(init = initializer()) {
    await init.initialize();
    return init.compare();
  }

  const CREATE_FOO: SchemaChange = {
    tag: 'create-table',
    spec: {
      schema: 'my',
      name: 'foo',
      columns: {
        id: {pos: 0, dataType: 'int8', notNull: true},
        a: {pos: 1, dataType: 'text'},
        b: {pos: 2, dataType: 'text'},
      },
      primaryKey: ['id'],
    },
    metadata: {rowKey: {type: 'default', columns: ['id']}},
    backfill: {a: {fooID: 987}, b: {fooID: 843}},
  };

  test('both stores at the same watermark, holding the same cookies', async () => {
    await transaction([CREATE_FOO]);

    expect(await compare()).toBe('equal');
  });

  test('every transition the replica trails by is reproduced by the fold', async () => {
    // Replicated: the replica's snapshot is taken here.
    await transaction([CREATE_FOO]);
    const replicated = readReplicaInitializationParameters(replica);

    // Everything below lands in the change log only, i.e. it is exactly what
    // the fold has to carry the replica's snapshot forward over.
    await transaction(
      [
        // A second table, with in-flight backfills and no metadata at all.
        {
          tag: 'create-table',
          spec: {
            schema: 'your',
            name: 'bar',
            columns: {id: {pos: 0, dataType: 'int8', notNull: true}},
            primaryKey: ['id'],
          },
          backfill: {c: {barID: 'zoo'}},
        },
        // add-column, carrying both halves.
        {
          tag: 'add-column',
          table: {schema: 'my', name: 'foo'},
          tableMetadata: {rowKey: {type: 'index', columns: ['id']}},
          column: {name: 'd', spec: {pos: 3, dataType: 'text'}},
          backfill: {fooID: 123},
        },
      ],
      {toReplica: false},
    );
    await transaction(
      [
        // update-table-metadata.
        {
          tag: 'update-table-metadata',
          table: {schema: 'your', name: 'bar'},
          old: {rowKey: {type: 'default', columns: ['id']}},
          new: {rowKey: {type: 'index', columns: ['id']}},
        },
        // A column rename moves the cookie; a spec-only update moves nothing.
        {
          tag: 'update-column',
          table: {schema: 'my', name: 'foo'},
          old: {name: 'a', spec: {pos: 1, dataType: 'text'}},
          new: {name: 'z', spec: {pos: 1, dataType: 'text'}},
        },
        {
          tag: 'update-column',
          table: {schema: 'my', name: 'foo'},
          old: {name: 'b', spec: {pos: 2, dataType: 'text'}},
          new: {name: 'b', spec: {pos: 2, dataType: 'text', notNull: true}},
        },
        {tag: 'drop-column', table: {schema: 'my', name: 'foo'}, column: 'd'},
        // Index changes carry no cookie state.
        {
          tag: 'create-index',
          spec: {
            schema: 'my',
            tableName: 'foo',
            name: 'foo_z',
            columns: {z: 'ASC'},
            unique: false,
          },
        },
      ],
      {toReplica: false},
    );
    await transaction(
      [
        {
          tag: 'rename-table',
          old: {schema: 'my', name: 'foo'},
          new: {schema: 'renamed', name: 'foo'},
        },
        {tag: 'drop-table', id: {schema: 'your', name: 'bar'}},
      ],
      {toReplica: false},
    );

    const pg = pgParameters();
    // The premise of the test: the two positions really are different, and the
    // sets really do differ before the fold.
    expect(replicated.lastWatermark).not.toBe(pg.lastWatermark);
    expect(replicated.cookies).not.toEqual(pg.cookies);

    expect(await compare()).toBe('equal-after-fold');
  });

  test('a backfill completed inside the interval is reproduced by the fold', async () => {
    await transaction([CREATE_FOO]);
    // `backfill-completed` clears the listed columns *and* the rowKey columns,
    // which is the transition most easily got wrong in a second interpreter.
    await transaction(
      [
        {
          tag: 'backfill-completed',
          relation: {
            schema: 'my',
            name: 'foo',
            rowKey: {type: 'default', columns: ['b']},
          },
          columns: ['a'],
          watermark: '03',
        },
      ],
      {toReplica: false},
    );

    expect(pgParameters().cookies.backfilling).toEqual([]);
    expect(await compare()).toBe('equal-after-fold');
  });

  test('a cookie the replica does not have is divergent', async () => {
    await transaction([CREATE_FOO]);
    await transaction(
      [
        {
          tag: 'add-column',
          table: {schema: 'my', name: 'foo'},
          column: {name: 'd', spec: {pos: 3, dataType: 'text'}},
          backfill: {fooID: 123},
        },
      ],
      {toReplica: false},
    );
    // The replica drops a backfill the log's interval never mentions again, so
    // the fold cannot restore it.
    replica
      .prepare(
        /*sql*/ `DELETE FROM "${BACKFILLING_TABLE}" WHERE "column" = 'a'`,
      )
      .run();

    expect(await compare()).toBe('divergent');
  });

  test('a cookie whose value differs at the same watermark is divergent', async () => {
    await transaction([CREATE_FOO]);
    replica
      .prepare(/*sql*/ `UPDATE "${BACKFILLING_TABLE}"
                   SET "backfill" = '{"fooID":1}' WHERE "column" = 'a'`)
      .run();

    expect(await compare()).toBe('divergent');
  });

  test('key order and row order are not divergences', async () => {
    await transaction([
      {
        tag: 'create-table',
        spec: {
          schema: 'my',
          name: 'foo',
          columns: {id: {pos: 0, dataType: 'int8', notNull: true}},
          primaryKey: ['id'],
        },
        backfill: {a: {fooID: 1, barID: 'z'}, b: {fooID: 2, barID: 'y'}},
      },
    ]);
    const pg = pgParameters();

    expect(
      await compare(
        initializer({
          pg: () =>
            Promise.resolve({
              ...pg,
              cookies: {
                tableMetadata: pg.cookies.tableMetadata,
                // Reversed rows, and reversed keys within each document.
                backfilling: pg.cookies.backfilling.toReversed().map(c => ({
                  ...c,
                  backfill: {barID: c.backfill.barID, fooID: c.backfill.fooID},
                })),
              },
            }),
        }),
      ),
    ).toBe('equal');
  });

  test('a log that does not reach back to the replica is inconclusive', async () => {
    await transaction([CREATE_FOO]);
    const replicated = readReplicaInitializationParameters(replica);
    await transaction([], {toReplica: false});

    // What a restore leaves behind: the log was reseeded above the replica's
    // state version, so the interval between them is not in it.
    changeLog
      .prepare(
        /*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" <= ?`,
      )
      .run(replicated.lastWatermark);

    expect(await compare()).toBe('inconclusive');
  });

  test('a log with no boundary at the replica is inconclusive', async () => {
    await transaction([CREATE_FOO]);
    const replicated = readReplicaInitializationParameters(replica);
    await transaction([], {toReplica: false});

    // The log reaches back far enough, but not to a transaction boundary: it
    // holds part of the replica's last transaction, so the next row is not the
    // first change of the interval.
    changeLog
      .prepare(/*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}"
                   WHERE "watermark" = ? AND "pos" > 0`)
      .run(replicated.lastWatermark);

    expect(await compare()).toBe('inconclusive');
  });

  test('no change log at all is inconclusive', async () => {
    await transaction([CREATE_FOO]);
    await transaction([], {toReplica: false});

    expect(await compare(initializer({changeLog: () => undefined}))).toBe(
      'inconclusive',
    );
  });

  test('a change log that has not been created yet is inconclusive', async () => {
    await transaction([CREATE_FOO]);
    await transaction([], {toReplica: false});
    const empty = new Database(lc, ':memory:');

    try {
      expect(await compare(initializer({changeLog: () => empty}))).toBe(
        'inconclusive',
      );
    } finally {
      empty.close();
    }
  });

  test('a replica ahead of Postgres is inconclusive', async () => {
    const wm = await transaction([CREATE_FOO]);
    // Transactions reach subscribers before the storer commits, so a replica
    // read taken mid-transaction can be one ahead. The fold only runs forward.
    const pg = pgParameters();

    expect(
      await compare(
        initializer({
          pg: () => Promise.resolve({...pg, lastWatermark: '0' + wm[1]}),
          replica: () => ({
            ...readReplicaInitializationParameters(replica),
            lastWatermark: '99',
          }),
        }),
      ),
    ).toBe('inconclusive');
  });

  // `lastPos` is inclusive, and the transaction's own begin/commit already
  // occupy positions 0 and 1, so the interval ends up `lastPos + 1` rows deep.
  function padTransaction(watermark: string, lastPos: number) {
    changeLog
      .prepare(/*sql*/ `
        INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
          ("watermark", "pos", "tag", "estimatedBytes", "change")
          WITH RECURSIVE n("i") AS (
            SELECT 2 UNION ALL SELECT "i" + 1 FROM n WHERE "i" < ?
          )
          SELECT ?, "i", 'insert', 0, '{"tag":"insert"}' FROM n
      `)
      .run(lastPos, watermark);
  }

  test('an interval too large to scan is inconclusive', async () => {
    await transaction([CREATE_FOO]);
    const head = await transaction([], {toReplica: false});
    // Above MAX_FOLD_SCAN_ROWS. A replicator that has been stalled can leave
    // the interval arbitrarily large, and this runs between reconciliation and
    // `startStream`, so the comparison declines rather than pays.
    padTransaction(head, 100_002);

    expect(await compare()).toBe('inconclusive');
  });

  test('an interval exactly at the cap is still folded', async () => {
    await transaction([CREATE_FOO]);
    const head = await transaction([], {toReplica: false});
    // Exactly MAX_FOLD_SCAN_ROWS, i.e. the last interval that is scanned
    // rather than declined. Pinned because the guard counts to one row *past*
    // the cap in SQL so that an unbounded interval is not walked in full, and
    // an off-by-one there would silently stop folding a legal interval.
    padTransaction(head, 99_999);

    expect(await compare()).toBe('equal-after-fold');
  });

  test('ordinary changes are filtered without parsing their payloads', async () => {
    await transaction([CREATE_FOO]);
    const head = await transaction([], {toReplica: false});

    // A malformed ordinary payload stands in for an arbitrarily large data
    // change. Filtering with json_extract() would parse it and fail; the stored
    // tag lets the schema fold skip it without touching the payload.
    changeLog.transaction(() => {
      changeLog
        .prepare(/*sql*/ `
          UPDATE "${CHANGE_LOG_STREAM_TABLE}" SET "pos" = 2
            WHERE "watermark" = ? AND "tag" = 'commit'
        `)
        .run(head);
      changeLog
        .prepare(/*sql*/ `
          INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
            ("watermark", "pos", "tag", "estimatedBytes", "change")
            VALUES (?, 1, 'insert', 1, 'not-json')
        `)
        .run(head);
    });

    expect(await compare()).toBe('equal-after-fold');
  });

  test('a replica that cannot be read does not stop the stream', async () => {
    const wm = await transaction([CREATE_FOO]);
    const init = initializer({
      replica: () => {
        throw new Error('replica is gone');
      },
    });

    // Postgres is authoritative, so the parameters still come back...
    expect((await init.initialize()).lastWatermark).toBe(wm);
    // ...and there is nothing to compare them against.
    expect(init.compare()).toBeUndefined();
  });

  test('a replica that cannot be read *is* fatal once it is the only source', async () => {
    await transaction([CREATE_FOO]);
    const init = initializer({
      initFromPgChangeLog: false,
      replica: () => {
        throw new Error('replica is gone');
      },
    });

    await expect(init.initialize()).rejects.toThrow('replica is gone');
  });

  test('with Postgres alone there is nothing to compare', async () => {
    const wm = await transaction([CREATE_FOO]);
    const init = initializer({initFromReplica: false});

    expect((await init.initialize()).lastWatermark).toBe(wm);
    expect(init.compare()).toBeUndefined();
  });

  test('with the replica alone its parameters are the ones returned', async () => {
    await transaction([CREATE_FOO]);
    // What the flip produces. The replica's own state version, and the requests
    // assembled from its own tables.
    const init = initializer({
      initFromPgChangeLog: false,
      pg: () => Promise.reject(new Error('must not be read')),
    });

    const params = await init.initialize();
    expect(params.lastWatermark).toBe(
      readReplicaInitializationParameters(replica).lastWatermark,
    );
    expect(params.backfillRequests).toEqual([
      {
        table: {
          schema: 'my',
          name: 'foo',
          metadata: {rowKey: {type: 'default', columns: ['id']}},
        },
        columns: {a: {fooID: 987}, b: {fooID: 843}},
      },
    ]);
    expect(init.compare()).toBeUndefined();
  });

  test('a comparison is consumed, so a missed initialize cannot compare stale state', async () => {
    await transaction([CREATE_FOO]);
    const init = initializer();

    await init.initialize();
    expect(init.compare()).toBe('equal');
    expect(init.compare()).toBeUndefined();
  });

  test('a fold that throws is reported rather than raised', async () => {
    await transaction([CREATE_FOO]);
    await transaction([], {toReplica: false});
    const init = initializer({
      changeLog: () => {
        throw new Error('the change log is not readable');
      },
    });

    await init.initialize();
    // Nothing acts on the comparison, so nothing about it may reach the stream
    // loop -- but it must not be silent either.
    expect(init.compare()).toBe('error');
  });

  test('at least one source is required', () => {
    expect(() =>
      initializer({initFromPgChangeLog: false, initFromReplica: false}),
    ).toThrow('At least one of initFromPgChangeLog or initFromReplica');
  });

  /**
   * The source the change-streamer actually uses, against a real file in WAL
   * mode -- which is the arrangement that makes a read-only handle non-obvious,
   * since one still has to attach the WAL's `-shm`. Both cases are covered: the
   * replicator holding the replica open, which is the steady state, and nothing
   * holding it, which is what a change-streamer that starts first sees.
   */
  describe('the replica source, on a file', () => {
    let file: DbFile;

    beforeEach(() => {
      file = new DbFile('change-log-initializer');
      const db = file.connect(lc);
      db.pragma('journal_mode = WAL');
      initReplicationState(db, ['zero_data'], REPLICA_VERSION);
      db.prepare(/*sql*/ `INSERT INTO "${BACKFILLING_TABLE}"
                   ("schema", "table", "column", "backfill")
                   VALUES ('my', 'foo', 'a', '{"fooID":987}')`).run();
      db.close();

      return () => file.delete();
    });

    test('reads it with no other connection open', () => {
      const params = replicaInitializationSource(lc, file.path)();

      expect(params.lastWatermark).toBe(REPLICA_VERSION);
      expect(params.cookies.backfilling).toEqual([
        {schema: 'my', table: 'foo', column: 'a', backfill: {fooID: 987}},
      ]);
    });

    test('reads it while the replicator holds it', () => {
      const writer = file.connect(lc);
      try {
        const source = replicaInitializationSource(lc, file.path);
        expect(source().lastWatermark).toBe(REPLICA_VERSION);

        // A snapshot, not a cached handle: the source re-opens per stream
        // connection, so it sees what the replicator has committed since.
        updateReplicationWatermark(new StatementRunner(writer), '09');
        expect(source().lastWatermark).toBe('09');
      } finally {
        writer.close();
      }
    });
  });

  test('a minRowVersion-only table is not a metadata cookie', async () => {
    await transaction([CREATE_FOO]);
    // `_zero.tableMetadata` also carries `minRowVersion`, so a row exists for
    // every table whose rows were ever force-re-downloaded. Neither cookie
    // store can represent one: both `"metadata"` columns are NOT NULL.
    replica
      .prepare(/*sql*/ `INSERT INTO "_zero.tableMetadata" ("schema", "table", "minRowVersion")
                   VALUES ('my', 'unrelated', '05')`)
      .run();

    expect(
      readReplicaInitializationParameters(replica).cookies.tableMetadata,
    ).toHaveLength(1);
    expect(await compare()).toBe('equal');
  });
});
