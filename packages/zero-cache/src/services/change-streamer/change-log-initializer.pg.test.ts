/**
 * The change log's initialization comparator against the two stores it actually
 * compares.
 *
 * `change-log-initializer.test.ts` covers the classification and the fold with
 * the change log standing in for Postgres. This covers what only a shard can:
 * `Storer.getStartStreamInitializationParameters()` reading `cdc.*`, against a
 * real replica, through the real {@link ChangeLogInitializer}.
 *
 * The specific thing it pins is that the two stores are allowed to differ in
 * ways that are not differences. Postgres holds the cookie documents as `jsonb`,
 * which does not preserve key order, and orders its rows under a collation the
 * replica does not share -- so a comparator that compared the two renderings
 * would report a divergence at the first cookie with more than one key. Every
 * `backfill` below has two.
 *
 * The transitions themselves are not re-walked here: `change-log-cookies.pg.test.ts`
 * pins all eight against Postgres, and `change-log-initializer.test.ts` pins
 * them across an interval. What is left is the composition.
 */

import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {BackfillRequest} from '../change-source/protocol/current/upstream.ts';
import {readCookies} from '../replicator/change-log-cookies.ts';
import {readChangeLogHead} from '../replicator/change-log-db.ts';
import type {ChangeProcessor} from '../replicator/change-processor.ts';
import {
  ChangeLogInitializer,
  readReplicaInitializationParameters,
} from './change-log-initializer.ts';
import {
  ChangeStreamDriver,
  createChangeLog,
  createReplica,
  createShardStorer,
  type ShardStorer,
} from './change-log-test-utils.ts';

const lc = createSilentLogContext();

describe('change-streamer/change-log-initializer against a shard', () => {
  let shard: ShardStorer;
  let changeLog: Database;
  let replica: Database;
  let processor: ChangeProcessor;
  let initializer: ChangeLogInitializer;
  let driver: ChangeStreamDriver;

  beforeEach<PgTest>(async ({testDBs}) => {
    shard = await createShardStorer(
      lc,
      testDBs,
      'change_log_initializer',
      'initializer',
    );
    changeLog = createChangeLog(lc);
    ({replica, processor} = createReplica(lc));

    initializer = makeInitializer(true);
    // The stream loop's three stores, driven together.
    driver = new ChangeStreamDriver(lc, {
      upstream: shard.storer,
      changeLog,
      replica: processor,
    });

    return async () => {
      replica.close();
      changeLog.close();
      await shard.close();
    };
  });

  /**
   * Creates an initializer with or without Postgres as an initialization
   * source.
   */
  function makeInitializer(initFromPgChangeLog: boolean) {
    return new ChangeLogInitializer(
      lc,
      {initFromPgChangeLog, initFromReplica: true},
      {
        pgChangeLog: () =>
          shard.storer.getStartStreamInitializationParameters(),
        replica: () => readReplicaInitializationParameters(replica),
        // This test writes directly to the log, so writer reconciliation is not
        // necessary. Use the Postgres point when present. Otherwise, use the
        // current log head.
        reconcileChangeLog: (resumeFrom, seed) => {
          if (resumeFrom) {
            return resumeFrom;
          }
          const head = readChangeLogHead(changeLog);
          return head === null
            ? seed()
            : {resumeWatermark: head, cookies: readCookies(changeLog)};
        },
        changeLog: () => changeLog,
      },
    );
  }

  async function compare() {
    await initializer.initialize();
    return initializer.lastComparison();
  }

  /**
   * Makes sure that both configurations produce the same initialization
   * parameters from the same stream.
   *
   * Sorts the requests because Postgres and SQLite use different collations.
   */
  async function expectFlipIsANoOp() {
    const withPg = await makeInitializer(true).initialize();
    const withoutPg = await makeInitializer(false).initialize();

    expect(withoutPg.lastWatermark).toBe(withPg.lastWatermark);
    expect(sorted(withoutPg.backfillRequests)).toEqual(
      sorted(withPg.backfillRequests),
    );
    expect(withoutPg.cookies).toEqual(withPg.cookies);
  }

  function sorted(requests: BackfillRequest[]): BackfillRequest[] {
    return requests.toSorted((a, b) =>
      `${a.table.schema}.${a.table.name}`.localeCompare(
        `${b.table.schema}.${b.table.name}`,
      ),
    );
  }

  test('the two stores agree when the replica is caught up', async () => {
    await driver.transaction([
      {
        tag: 'create-table',
        spec: {
          schema: 'my',
          name: 'foo',
          columns: {
            id: {pos: 0, dataType: 'int8', notNull: true},
            a: {pos: 1, dataType: 'text'},
          },
          primaryKey: ['id'],
        },
        metadata: {rowKey: {type: 'default', columns: ['id']}},
        // Two keys, in an order `jsonb` does not preserve.
        backfill: {a: {fooID: 987, barID: 'zoo'}},
      },
    ]);

    expect(await compare()).toBe('equal');
    await expectFlipIsANoOp();
  });

  test('and when it trails, over an interval that moves both cookies', async () => {
    await driver.transaction([
      {
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
        backfill: {a: {fooID: 987, barID: 'zoo'}, b: {fooID: 843, barID: 'oz'}},
      },
      // A table with in-flight backfills and no metadata row: legal, and the
      // case the replica could not name before `_zero.backfilling` existed.
      {
        tag: 'create-table',
        spec: {
          schema: 'your',
          name: 'bar',
          columns: {id: {pos: 0, dataType: 'int8', notNull: true}},
          primaryKey: ['id'],
        },
        backfill: {c: {barID: 'zoo', fooID: 1}},
      },
    ]);

    // Everything below reaches the change log and Postgres but not the replica,
    // i.e. it is exactly what the fold has to carry the replica forward over.
    await driver.transaction(
      [
        {
          tag: 'add-column',
          table: {schema: 'my', name: 'foo'},
          tableMetadata: {rowKey: {type: 'index', columns: ['id']}},
          column: {name: 'd', spec: {pos: 3, dataType: 'text'}},
          backfill: {fooID: 123, barID: 'dd'},
        },
        {
          tag: 'update-column',
          table: {schema: 'my', name: 'foo'},
          old: {name: 'a', spec: {pos: 1, dataType: 'text'}},
          new: {name: 'z', spec: {pos: 1, dataType: 'text'}},
        },
      ],
      {toReplica: false},
    );
    await driver.transaction(
      [
        // Clears the listed columns *and* the rowKey's, leaving `your.bar`.
        {
          tag: 'backfill-completed',
          relation: {
            schema: 'my',
            name: 'foo',
            rowKey: {type: 'default', columns: ['z']},
          },
          columns: ['b', 'd'],
          watermark: '03',
        },
      ],
      {toReplica: false},
    );

    const {cookies} =
      await shard.storer.getStartStreamInitializationParameters();
    // Non-vacuous: the interval really did move the cookie set, and what is
    // left is not empty.
    expect(cookies.tableMetadata).toHaveLength(1);
    expect(cookies.backfilling).toEqual([
      {
        schema: 'your',
        table: 'bar',
        column: 'c',
        backfill: {barID: 'zoo', fooID: 1},
      },
    ]);

    expect(await compare()).toBe('equal-after-fold');
    await expectFlipIsANoOp();
  });
});
