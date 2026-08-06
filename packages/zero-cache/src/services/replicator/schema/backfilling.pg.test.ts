/**
 * The point of `_zero.backfilling` is that a change log can be initialized from
 * a restored replica instead of from the Postgres change DB. That is only true
 * if the two derive the *same* {@link BackfillRequest}s from the same change
 * stream, so this drives one sequence through both the storer and the
 * change-processor and requires their request lists to be identical.
 *
 * It covers the two cases `_zero.column_metadata` alone cannot express, and
 * which are the whole reason this table exists: a table on a non-`public`
 * schema, whose lite name is ambiguous, and a table with in-flight backfills
 * and no metadata row at all.
 *
 * The change log's initializer runs this comparison in production, against a
 * replica that trails the log's head. Here they are driven in lockstep, so
 * equality is exact.
 */

import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {type PgTest, test} from '../../../test/db.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {SchemaChange} from '../../change-source/protocol/current/data.ts';
import type {
  ChangeStreamData,
  Commit,
} from '../../change-source/protocol/current/downstream.ts';
import type {UpstreamStatusMessage} from '../../change-source/protocol/current/status.ts';
import type {BackfillRequest} from '../../change-source/protocol/current/upstream.ts';
import {
  ensureReplicationConfig,
  setupCDCTables,
} from '../../change-streamer/schema/tables.ts';
import {Storer, type TuningOptions} from '../../change-streamer/storer.ts';
import {ChangeProcessor} from '../change-processor.ts';
import {readBackfillRequests} from './backfilling.ts';
import {initReplicationState} from './replication-state.ts';

const lc = createSilentLogContext();

const APP_ID = 'backfilling';
const SHARD_NUM = 1;
const REPLICA_VERSION = '00';

const opts: TuningOptions = {
  backPressureLimitHeapProportion: 0.04,
  statementTimeoutMs: 20_000,
  changeLogBatchSize: 2000,
};

describe('replicator/schema/backfill request parity', () => {
  let db: PostgresDB;
  let storer: Storer;
  let done: Promise<void>;
  let replica: Database;
  let processor: ChangeProcessor;
  let watermark: number;

  beforeEach<PgTest>(async ({testDBs}) => {
    db = await testDBs.create('backfill_request_parity', {
      typeOpts: {sendStringAsJson: true},
    });
    const shard = {appID: APP_ID, shardNum: SHARD_NUM};
    await db.begin(tx => setupCDCTables(lc, tx, shard));
    await ensureReplicationConfig(
      lc,
      db,
      {
        replicaVersion: REPLICA_VERSION,
        publications: [],
        watermark: REPLICA_VERSION,
      },
      shard,
      true,
    );

    storer = new Storer(
      lc,
      shard,
      'task-id',
      'change-streamer:12345',
      'ws',
      db,
      REPLICA_VERSION,
      (_: Commit | UpstreamStatusMessage) => {},
      () => {},
      opts,
    );
    await storer.assumeOwnership();
    done = storer.run();

    replica = new Database(lc, ':memory:');
    initReplicationState(replica, ['zero_data'], REPLICA_VERSION);
    // The replication-manager's replica, which is the one a change log would
    // eventually be initialized from.
    processor = new ChangeProcessor(
      new StatementRunner(replica),
      'backup',
      (_, err: unknown) => {
        throw err;
      },
    );
    watermark = 1;

    return async () => {
      replica.close();
      await testDBs.drop(db);
      void storer?.stop();
      await done;
    };
  });

  /** Drives one transaction carrying `changes` through both stores. */
  async function transaction(...changes: SchemaChange[]): Promise<void> {
    const wm = String(++watermark).padStart(2, '0');
    const messages: ChangeStreamData[] = [
      ['begin', {tag: 'begin'}, {commitWatermark: wm}],
      ...changes.map((change): ChangeStreamData => ['data', change]),
      ['commit', {tag: 'commit'}, {watermark: wm}],
    ];

    messages.forEach(message => storer.store(wm, message));
    messages.forEach(message => processor.processMessage(lc, message));

    await storer.allProcessed();
  }

  /** Requests are grouped per table, so the table's identity orders them. */
  function sorted(requests: BackfillRequest[]): BackfillRequest[] {
    return requests.toSorted((a, b) =>
      `${a.table.schema}.${a.table.name}`.localeCompare(
        `${b.table.schema}.${b.table.name}`,
      ),
    );
  }

  /**
   * The `your.bar` request, which no transition below touches until the final
   * drop. Named rather than repeated so that each step's expectation is its own
   * delta, and so that "bar did not move" is something the reader can see
   * instead of having to diff two literals.
   */
  const BAR: BackfillRequest = {
    table: {schema: 'your', name: 'bar', metadata: null},
    columns: {c: {barID: 'zoo'}},
  };

  /** The `my.foo` request: the two halves every step below moves. */
  function foo(
    rowKey: 'default' | 'index',
    columns: BackfillRequest['columns'],
    schema = 'my',
  ): BackfillRequest {
    return {
      table: {
        schema,
        name: 'foo',
        metadata: {rowKey: {type: rowKey, columns: ['id']}},
      },
      columns,
    };
  }

  /**
   * Both stores agree, and the agreed-on list is what was expected. The second
   * half matters: two derivations agree vacuously if neither derives anything.
   */
  async function expectRequests(expected: BackfillRequest[]) {
    const {backfillRequests} =
      await storer.getStartStreamInitializationParameters();
    const pg = sorted(backfillRequests);
    expect(pg).toEqual(sorted(readBackfillRequests(replica)));
    expect(pg).toEqual(sorted(expected));
  }

  test('every transition that moves a backfill request', async () => {
    // A table on a non-`public` schema. Its lite name, "my.foo", is ambiguous
    // against a `public` table named "my.foo", which is what the identity in
    // "_zero.backfilling" resolves.
    await transaction({
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
    });

    // A table with in-flight backfills and *no* metadata row: legal, since a
    // change source need not send one, and the case the replica could not
    // reconstruct before this table existed.
    await transaction({
      tag: 'create-table',
      spec: {
        schema: 'your',
        name: 'bar',
        columns: {
          id: {pos: 0, dataType: 'int8', notNull: true},
          c: {pos: 1, dataType: 'text'},
        },
        primaryKey: ['id'],
      },
      backfill: {c: {barID: 'zoo'}},
    });

    await expectRequests([
      foo('default', {a: {fooID: 987}, b: {fooID: 843}}),
      BAR,
    ]);

    // add-column, carrying both a metadata update and a backfill.
    await transaction({
      tag: 'add-column',
      table: {schema: 'my', name: 'foo'},
      tableMetadata: {rowKey: {type: 'index', columns: ['id']}},
      column: {name: 'd', spec: {pos: 3, dataType: 'text'}},
      backfill: {fooID: 123},
    });
    await expectRequests([
      foo('index', {a: {fooID: 987}, b: {fooID: 843}, d: {fooID: 123}}),
      BAR,
    ]);

    // update-column: a rename moves the request's column, a spec-only update
    // moves nothing. Then drop-column removes one.
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
        new: {name: 'b', spec: {pos: 2, dataType: 'text', notNull: true}},
      },
      {
        tag: 'drop-column',
        table: {schema: 'my', name: 'foo'},
        column: 'd',
      },
    );
    await expectRequests([
      foo('index', {b: {fooID: 843}, z: {fooID: 987}}),
      BAR,
    ]);

    // backfill-completed clears the rowKey columns along with the listed ones,
    // but not the table's metadata: that is what the table's *next* request
    // would have to carry.
    await transaction({
      tag: 'backfill-completed',
      relation: {
        schema: 'my',
        name: 'foo',
        rowKey: {type: 'default', columns: ['id']},
      },
      columns: ['z'],
      watermark: '05',
    });
    await expectRequests([foo('index', {b: {fooID: 843}}), BAR]);

    // rename-table moves the request, and only the renamed table's.
    await transaction({
      tag: 'rename-table',
      old: {schema: 'my', name: 'foo'},
      new: {schema: 'renamed', name: 'foo'},
    });
    await expectRequests([foo('index', {b: {fooID: 843}}, 'renamed'), BAR]);

    // drop-table clears both halves.
    await transaction(
      {tag: 'drop-table', id: {schema: 'renamed', name: 'foo'}},
      {tag: 'drop-table', id: {schema: 'your', name: 'bar'}},
    );
    await expectRequests([]);
  });

  test('a table with no in-flight backfill produces no request', async () => {
    await transaction({
      tag: 'create-table',
      spec: {
        schema: 'public',
        name: 'foo',
        columns: {id: {pos: 0, dataType: 'int8', notNull: true}},
        primaryKey: ['id'],
      },
      metadata: {rowKey: {type: 'default', columns: ['id']}},
    });
    // A change source that does not support backfill sends neither field.
    await transaction({
      tag: 'add-column',
      table: {schema: 'public', name: 'foo'},
      column: {name: 'a', spec: {pos: 1, dataType: 'text'}},
    });

    await expectRequests([]);
  });
});
