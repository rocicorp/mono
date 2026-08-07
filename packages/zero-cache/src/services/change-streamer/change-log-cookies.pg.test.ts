/**
 * The Postgres and SQLite change logs interpret one fold
 * (`replicator/change-log-cookies.ts`), and they have to agree on every
 * transition forever: the failure mode when they drift is a backfill that is
 * silently never re-requested, on a column that is then silently never
 * populated. This drives the same change sequence through both writers and
 * requires the resulting cookie sets to be identical.
 *
 */

import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {SchemaChange} from '../change-source/protocol/current/data.ts';
import {readCookies, type CookieSet} from '../replicator/change-log-cookies.ts';
import {
  reconcileChangeLog,
  type ChangeLogAnchor,
} from '../replicator/change-log-db.ts';
import {ChangeLogStreamWriter} from '../replicator/change-log-stream-writer.ts';
import {serializeChangeStreamData} from './change-log-codec.ts';
import {
  ChangeStreamDriver,
  createChangeLog,
  createShardStorer,
  readPgCookies,
  REPLICA_VERSION,
  transactionMessages,
  type ShardStorer,
} from './change-log-test-utils.ts';

const lc = createSilentLogContext();

describe('change-streamer/change-log cookie parity', () => {
  let shard: ShardStorer;
  let changeLog: Database;
  let driver: ChangeStreamDriver;

  beforeEach<PgTest>(async ({testDBs}) => {
    shard = await createShardStorer(
      lc,
      testDBs,
      'change_log_cookie_parity',
      'cookies',
    );
    changeLog = createChangeLog(lc);
    driver = new ChangeStreamDriver(lc, {
      upstream: shard.storer,
      changeLog,
    });

    return async () => {
      changeLog.close();
      await shard.close();
    };
  });

  /**
   * Drives one transaction carrying `changes` through both writers, the way
   * their respective stream loops do.
   */
  const transaction = (...changes: SchemaChange[]) =>
    driver.transaction(changes);

  /** The cookie set the Postgres change log holds, in the SQLite log's shape. */
  const pgCookies = (): Promise<CookieSet> =>
    readPgCookies(shard.db, shard.cdcSchema);

  /**
   * Both stores agree, and the agreed-on set is what was expected. The second
   * half matters: two interpreters of the same fold agree vacuously if the fold
   * itself does nothing.
   */
  async function expectCookies(expected: CookieSet) {
    const pg = await pgCookies();
    expect(pg).toEqual(readCookies(changeLog));
    expect(pg).toEqual(expected);
  }

  test('all eight transitions', async () => {
    // 1. create-table, carrying metadata and two in-flight backfills.
    await transaction({
      tag: 'create-table',
      spec: {schema: 'my', name: 'foo', columns: {}},
      metadata: {rowKey: {type: 'index', columns: ['a', 'b']}},
      backfill: {
        a: {fooID: 987, barID: 'zoo'},
        b: {fooID: 843, barID: 'ozz'},
      },
    });
    await expectCookies({
      tableMetadata: [
        {
          schema: 'my',
          table: 'foo',
          metadata: {rowKey: {type: 'index', columns: ['a', 'b']}},
        },
      ],
      backfilling: [
        {
          schema: 'my',
          table: 'foo',
          column: 'a',
          backfill: {fooID: 987, barID: 'zoo'},
        },
        {
          schema: 'my',
          table: 'foo',
          column: 'b',
          backfill: {fooID: 843, barID: 'ozz'},
        },
      ],
    });

    // A table on a non-`public` schema with backfills and *no* metadata row:
    // legal, since `metadata` is optional, and the case the replica-derived
    // path (C3) cannot reconstruct without an identity of its own.
    await transaction({
      tag: 'create-table',
      spec: {schema: 'your', name: 'bar', columns: {}},
      backfill: {c: {fooID: 5}},
    });

    // 2. update-table-metadata.
    // 3. add-column, carrying both a metadata update and a backfill.
    await transaction(
      {
        tag: 'update-table-metadata',
        table: {schema: 'my', name: 'foo'},
        old: {rowKey: {type: 'index', columns: ['a', 'b']}},
        new: {rowKey: {type: 'default', columns: ['id']}},
      },
      {
        tag: 'add-column',
        table: {schema: 'my', name: 'foo'},
        tableMetadata: {rowKey: {type: 'default', columns: ['id']}},
        column: {name: 'd', spec: {pos: 4, dataType: 'text'}},
        backfill: {fooID: 123, barID: 'baz'},
      },
    );
    await expectCookies({
      tableMetadata: [
        {
          schema: 'my',
          table: 'foo',
          metadata: {rowKey: {type: 'default', columns: ['id']}},
        },
      ],
      backfilling: [
        {
          schema: 'my',
          table: 'foo',
          column: 'a',
          backfill: {fooID: 987, barID: 'zoo'},
        },
        {
          schema: 'my',
          table: 'foo',
          column: 'b',
          backfill: {fooID: 843, barID: 'ozz'},
        },
        {
          schema: 'my',
          table: 'foo',
          column: 'd',
          backfill: {fooID: 123, barID: 'baz'},
        },
        {schema: 'your', table: 'bar', column: 'c', backfill: {fooID: 5}},
      ],
    });

    // 4. update-column (a rename), and a spec-only update that must move
    //    nothing. 5. drop-column.
    await transaction(
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
        new: {name: 'b', spec: {pos: 2, dataType: 'int4'}},
      },
      {
        tag: 'drop-column',
        table: {schema: 'my', name: 'foo'},
        column: 'd',
      },
    );
    await expectCookies({
      tableMetadata: [
        {
          schema: 'my',
          table: 'foo',
          metadata: {rowKey: {type: 'default', columns: ['id']}},
        },
      ],
      backfilling: [
        {
          schema: 'my',
          table: 'foo',
          column: 'b',
          backfill: {fooID: 843, barID: 'ozz'},
        },
        {
          schema: 'my',
          table: 'foo',
          column: 'z',
          backfill: {fooID: 987, barID: 'zoo'},
        },
        {schema: 'your', table: 'bar', column: 'c', backfill: {fooID: 5}},
      ],
    });

    // 6. backfill-completed. `rowKey` columns are excluded from `columns` but
    //    are backfilled with them, so both are cleared; the table's metadata is
    //    not, since it is what the table's *next* BackfillRequest must carry.
    await transaction({
      tag: 'backfill-completed',
      relation: {
        schema: 'my',
        name: 'foo',
        rowKey: {columns: ['b'], type: 'index'},
      },
      columns: ['z'],
      watermark: '07',
    });
    await expectCookies({
      tableMetadata: [
        {
          schema: 'my',
          table: 'foo',
          metadata: {rowKey: {type: 'default', columns: ['id']}},
        },
      ],
      backfilling: [
        {schema: 'your', table: 'bar', column: 'c', backfill: {fooID: 5}},
      ],
    });

    // 7. rename-table, which moves both cookies and only the renamed table's.
    await transaction({
      tag: 'rename-table',
      old: {schema: 'my', name: 'foo'},
      new: {schema: 'renamed', name: 'foo'},
    });
    await expectCookies({
      tableMetadata: [
        {
          schema: 'renamed',
          table: 'foo',
          metadata: {rowKey: {type: 'default', columns: ['id']}},
        },
      ],
      backfilling: [
        {schema: 'your', table: 'bar', column: 'c', backfill: {fooID: 5}},
      ],
    });

    // 8. drop-table, which clears both.
    await transaction(
      {tag: 'drop-table', id: {schema: 'renamed', name: 'foo'}},
      {tag: 'drop-table', id: {schema: 'your', name: 'bar'}},
    );
    await expectCookies({tableMetadata: [], backfilling: []});
  });

  test('index changes and metadata-free schema changes move nothing', async () => {
    await transaction({
      tag: 'create-table',
      spec: {schema: 'my', name: 'foo', columns: {}},
      metadata: {rowKey: {type: 'default', columns: ['id']}},
    });
    await transaction(
      {
        tag: 'create-index',
        spec: {
          schema: 'my',
          name: 'foo_idx',
          tableName: 'foo',
          unique: false,
          columns: {id: 'ASC'},
        },
      },
      {tag: 'drop-index', id: {schema: 'my', name: 'foo_idx'}},
      // A change source that does not support backfill sends neither field.
      {
        tag: 'add-column',
        table: {schema: 'my', name: 'foo'},
        column: {name: 'e', spec: {pos: 5, dataType: 'text'}},
      },
    );

    await expectCookies({
      tableMetadata: [
        {
          schema: 'my',
          table: 'foo',
          metadata: {rowKey: {type: 'default', columns: ['id']}},
        },
      ],
      backfilling: [],
    });
  });

  test('an aborted transaction leaves neither store holding its cookies', async () => {
    await transaction({
      tag: 'create-table',
      spec: {schema: 'my', name: 'foo', columns: {}},
      backfill: {a: {fooID: 1}},
    });

    // The stream is interrupted after the schema change and before the commit:
    // Postgres aborts its transaction pool, SQLite rolls its transaction back.
    // Driven by hand rather than by the driver, which only commits.
    const change: SchemaChange = {
      tag: 'add-column',
      table: {schema: 'my', name: 'foo'},
      column: {name: 'b', spec: {pos: 2, dataType: 'text'}},
      backfill: {fooID: 2},
    };
    const wm = driver.claimWatermark();
    const [begin, data] = transactionMessages(wm, [change]);

    shard.storer.store(wm, begin);
    shard.storer.store(wm, data);

    const writer = new ChangeLogStreamWriter(new StatementRunner(changeLog));
    writer.begin(wm, serializeChangeStreamData(begin));
    writer.append(serializeChangeStreamData(data), change);
    writer.rollback();

    shard.storer.abort();
    await shard.storer.allProcessed();

    await expectCookies({
      tableMetadata: [],
      backfilling: [
        {schema: 'my', table: 'foo', column: 'a', backfill: {fooID: 1}},
      ],
    });
  });

  /**
   * Slice C2. Everything above proves the two stores fold alike; this proves
   * that the log can be *handed* the Postgres set at a resume point and carry
   * on folding from it — which is what invariant 17 requires of every
   * reconciliation that moves the head, and what the C5 flip inverts.
   */
  describe('reseeding the log at the resume point', () => {
    function anchorFrom({
      lastWatermark,
      cookies,
    }: {
      lastWatermark: string;
      cookies: CookieSet;
    }): ChangeLogAnchor {
      return {
        identity: {epoch: null, generation: REPLICA_VERSION, replicaID: null},
        resumeWatermark: lastWatermark,
        nowMs: 1_700_000_000_000,
        cookies,
      };
    }

    /** The anchor a stream connection would reconcile the log against. */
    async function anchor(): Promise<ChangeLogAnchor> {
      return anchorFrom(
        await shard.storer.getStartStreamInitializationParameters(),
      );
    }

    test('a reseed lands the cookie set Postgres holds', async () => {
      await transaction({
        tag: 'create-table',
        spec: {schema: 'my', name: 'foo', columns: {}},
        metadata: {rowKey: {type: 'index', columns: ['a', 'b']}},
        backfill: {a: {fooID: 987}, b: {fooID: 843}},
      });
      // A table whose metadata `backfillRequests` drops, since it has no
      // in-flight backfill (§3.7). The raw cookie set is what carries it, and
      // is why the reseed reads the tables rather than the request list.
      await transaction({
        tag: 'create-table',
        spec: {schema: 'your', name: 'bar', columns: {}},
        metadata: {rowKey: {type: 'default', columns: ['id']}},
      });

      // A log with no meta row of its own: what a task that has just restored a
      // replica, or upgraded past a schema version, brings to its first stream
      // connection. It is wiped and reseeded at the resume watermark.
      const params =
        await shard.storer.getStartStreamInitializationParameters();
      expect(
        reconcileChangeLog(lc, changeLog, anchorFrom(params)),
      ).toMatchObject({
        action: 'reseeded',
        reason: 'created',
        cookiesStale: true,
      });

      expect(readCookies(changeLog)).toEqual(await pgCookies());
      // Not the same thing as the request list, and strictly more than it.
      expect(params.backfillRequests).toHaveLength(1);
      expect(readCookies(changeLog).tableMetadata).toHaveLength(2);
    });

    test('a truncation replaces the set rather than keeping its own', async () => {
      await transaction({
        tag: 'create-table',
        spec: {schema: 'my', name: 'foo', columns: {}},
        backfill: {a: {fooID: 1}},
      });
      // The resume point of the connection that is about to reconnect, captured
      // before the log runs ahead of it.
      const resumePoint = await anchor();
      reconcileChangeLog(lc, changeLog, resumePoint);

      // The log commits before anything that advances the resume watermark, so
      // it can hold a transaction Postgres' watermark has not reached -- and
      // that transaction's cookies with it.
      await transaction({
        tag: 'add-column',
        table: {schema: 'my', name: 'foo'},
        column: {name: 'b', spec: {pos: 2, dataType: 'text'}},
        backfill: {fooID: 2},
      });
      expect(readCookies(changeLog).backfilling).toHaveLength(2);

      // The reconnect truncates it away. Rolling the cookies back is not
      // possible -- an `add-column`'s backfill is not undone by deleting the
      // row that carried it -- so the set that belongs to the resume watermark
      // replaces it wholesale.
      expect(reconcileChangeLog(lc, changeLog, resumePoint)).toMatchObject({
        action: 'truncated',
        cookiesStale: true,
      });
      expect(readCookies(changeLog)).toEqual(resumePoint.cookies);
      expect(readCookies(changeLog).backfilling).toEqual([
        {schema: 'my', table: 'foo', column: 'a', backfill: {fooID: 1}},
      ]);
    });

    test('the log folds onward from the set it was reseeded with', async () => {
      await transaction({
        tag: 'create-table',
        spec: {schema: 'my', name: 'foo', columns: {}},
        backfill: {a: {fooID: 1}, b: {fooID: 2}},
      });
      reconcileChangeLog(lc, changeLog, await anchor());

      // The completion arrives on the resumed stream, against cookies this log
      // never folded for itself. Both stores land in the same place, which is
      // the property the whole reseed exists to preserve.
      await transaction({
        tag: 'backfill-completed',
        relation: {schema: 'my', name: 'foo', rowKey: {columns: ['a']}},
        columns: ['b'],
        watermark: '09',
      });

      await expectCookies({tableMetadata: [], backfilling: []});
    });
  });

  test('several cookie moves in one transaction stay in stream order', async () => {
    // The storer flushes its buffered changeLog batch before a schema change so
    // that the cookie DML stays in stream order relative to the rows; SQLite
    // gets that from statement order. Several cookie-moving changes in one
    // transaction is what would catch either one losing it.
    await transaction(
      {
        tag: 'create-table',
        spec: {schema: 'my', name: 'foo', columns: {}},
        backfill: {a: {fooID: 1}, b: {fooID: 2}},
      },
      {
        tag: 'backfill-completed',
        relation: {schema: 'my', name: 'foo', rowKey: {columns: ['a']}},
        columns: [],
        watermark: '07',
      },
      {
        tag: 'add-column',
        table: {schema: 'my', name: 'foo'},
        column: {name: 'c', spec: {pos: 3, dataType: 'text'}},
        backfill: {fooID: 3},
      },
    );

    // `a` completed, `b` never did, `c` started after the completion.
    await expectCookies({
      tableMetadata: [],
      backfilling: [
        {schema: 'my', table: 'foo', column: 'b', backfill: {fooID: 2}},
        {schema: 'my', table: 'foo', column: 'c', backfill: {fooID: 3}},
      ],
    });
  });
});
