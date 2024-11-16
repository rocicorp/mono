import {PG_OBJECT_IN_USE} from '@drdgvhbh/postgres-error-codes';
import {LogContext} from '@rocicorp/logger';
import {DatabaseError} from 'pg-protocol';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {AbortError} from '../../../../../shared/src/abort-error.js';
import {TestLogSink} from '../../../../../shared/src/logging-test-utils.js';
import {Queue} from '../../../../../shared/src/queue.js';
import {promiseVoid} from '../../../../../shared/src/resolved-promises.js';
import {sleep} from '../../../../../shared/src/sleep.js';
import {StatementRunner} from '../../../db/statements.js';
import {
  dropReplicationSlots,
  getConnectionURI,
  testDBs,
} from '../../../test/db.js';
import {DbFile} from '../../../test/lite.js';
import {
  oneAfter,
  versionFromLexi,
  versionToLexi,
} from '../../../types/lexi-version.js';
import type {PostgresDB} from '../../../types/pg.js';
import type {Source} from '../../../types/streams.js';
import {getSubscriptionState} from '../../replicator/schema/replication-state.js';
import type {
  ChangeSource,
  ChangeStream,
  ChangeStreamMessage,
} from '../change-streamer-service.js';
import type {Commit} from '../change-streamer.js';
import {initializeChangeSource} from './change-source.js';
import {replicationSlot} from './initial-sync.js';
import {fromLexiVersion} from './lsn.js';
import {dropEventTriggerStatements} from './schema/ddl-test-utils.js';

const SHARD_ID = 'change_source_test_id';

describe('change-source/pg', () => {
  let logSink: TestLogSink;
  let lc: LogContext;
  let upstream: PostgresDB;
  let upstreamURI: string;
  let replicaDbFile: DbFile;
  let source: ChangeSource;
  let streams: ChangeStream[];

  beforeEach(async () => {
    logSink = new TestLogSink();
    lc = new LogContext('error', {}, logSink);
    upstream = await testDBs.create('change_source_pg_test_upstream');
    replicaDbFile = new DbFile('change_source_pg_test_replica');

    upstreamURI = getConnectionURI(upstream);
    await upstream.unsafe(`
    CREATE TABLE foo(
      id TEXT CONSTRAINT foo_pk PRIMARY KEY,
      int INT4,
      big BIGINT,
      flt FLOAT8,
      bool BOOLEAN,
      timea TIMESTAMPTZ,
      timeb TIMESTAMPTZ,
      date DATE,
      time TIME,
      dates DATE[],
      times TIMESTAMP[]
    );
    CREATE PUBLICATION zero_foo FOR TABLE foo WHERE (id != 'exclude-me');

    CREATE SCHEMA IF NOT EXISTS zero;
    CREATE TABLE zero.boo(
      a TEXT PRIMARY KEY, b TEXT, c TEXT, d TEXT
    );
    CREATE PUBLICATION zero_zero FOR TABLES IN SCHEMA zero;
    `);

    source = (
      await initializeChangeSource(
        lc,
        upstreamURI,
        {id: SHARD_ID, publications: ['zero_foo', 'zero_zero']},
        replicaDbFile.path,
      )
    ).changeSource;
    streams = [];
  });

  afterEach(async () => {
    streams.forEach(s => s.changes.cancel());
    await testDBs.drop(upstream);
    replicaDbFile.delete();
  });

  function drainToQueue(
    sub: Source<ChangeStreamMessage>,
  ): Queue<ChangeStreamMessage> {
    const queue = new Queue<ChangeStreamMessage>();
    void (async () => {
      for await (const msg of sub) {
        void queue.enqueue(msg);
      }
    })();
    return queue;
  }

  const WATERMARK_REGEX = /[0-9a-z]{2,}/;

  function withTriggers() {
    return promiseVoid;
  }

  async function withoutTriggers() {
    await upstream.unsafe(
      `UPDATE zero_${SHARD_ID}."shardConfig" SET "ddlDetection" = false;` +
        dropEventTriggerStatements(SHARD_ID),
    );
  }

  const MAX_ATTEMPTS_IF_REPLICATION_SLOT_ACTIVE = 5;

  async function startStream(watermark: string) {
    let err;
    for (let i = 0; i < MAX_ATTEMPTS_IF_REPLICATION_SLOT_ACTIVE; i++) {
      try {
        const stream = await source.startStream(watermark);
        // cleanup in afterEach() ensures that replication slots are released
        streams.push(stream);
        return stream;
      } catch (e) {
        if (e instanceof DatabaseError && e.code === PG_OBJECT_IN_USE) {
          // Sometimes Postgres still considers the replication slot active
          // from the previous test, e.g.
          // error: replication slot "zero_change_source_test_id" is active for PID 388
          console.warn(e);
          err = e;
          await sleep(100);
          continue; // retry
        }
        throw e;
      }
    }
    throw err;
  }

  test.each([[withTriggers], [withoutTriggers]])(
    'filtered changes and acks %o',
    async init => {
      await init();
      const {replicaVersion} = getSubscriptionState(
        new StatementRunner(replicaDbFile.connect(lc)),
      );

      const {initialWatermark, changes, acks} = await startStream('00');
      const downstream = drainToQueue(changes);

      await upstream.begin(async tx => {
        await tx`INSERT INTO foo(id) VALUES('hello')`;
        await tx`INSERT INTO foo(id) VALUES('world')`;
        await tx`
      INSERT INTO foo(id, int, big, flt, bool, timea, timeb, date, time, dates, times) 
        VALUES('datatypes',
               123456789, 
               987654321987654321, 
               123.456, 
               true, 
               '2003-04-12 04:05:06 America/New_York',
               '2019-01-12T00:30:35.381101032Z',
               'April 12, 2003',
               '04:05:06.123456789',
               ARRAY['2001-02-03'::date, '2002-03-04'::date],
               ARRAY['2019-01-12T00:30:35.654321'::timestamp, '2019-01-12T00:30:35.123456'::timestamp]
               )`;
        // zero.schemaVersions
        await tx`
      UPDATE zero."schemaVersions" SET "maxSupportedVersion" = 2;
      `;
      });

      expect(initialWatermark).toEqual(oneAfter(replicaVersion));
      expect(await downstream.dequeue()).toMatchObject([
        'begin',
        {tag: 'begin'},
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {
          tag: 'insert',
          new: {id: 'hello'},
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {
          tag: 'insert',
          new: {id: 'world'},
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {
          tag: 'insert',
          new: {
            id: 'datatypes',
            int: 123456789,
            big: 987654321987654321n,
            flt: 123.456,
            bool: true,
            timea: 1050134706000,
            timeb: 1547253035381.101,
            date: Date.UTC(2003, 3, 12),
            time: '04:05:06.123457', // PG rounds to microseconds
            dates: [Date.UTC(2001, 1, 3), Date.UTC(2002, 2, 4)],
            times: [1547253035654.321, 1547253035123.456],
          },
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {
          tag: 'update',
          new: {minSupportedVersion: 1, maxSupportedVersion: 2},
        },
      ]);
      const firstCommit = (await downstream.dequeue()) as Commit;
      expect(firstCommit).toMatchObject([
        'commit',
        {tag: 'commit'},
        {watermark: expect.stringMatching(WATERMARK_REGEX)},
      ]);
      acks.push(firstCommit);

      // Write more upstream changes.
      await upstream.begin(async tx => {
        await tx`DELETE FROM foo WHERE id = 'world'`;
        await tx`UPDATE foo SET int = 123 WHERE id = 'hello';`;
        await tx`TRUNCATE foo`;
        // Should be excluded by zero_all.
        await tx`INSERT INTO foo(id) VALUES ('exclude-me')`;
        await tx`INSERT INTO foo(id) VALUES ('include-me')`;
        // zero.clients change that should be included in _zero_{SHARD_ID}_client.
        await tx.unsafe(
          `INSERT INTO zero_${SHARD_ID}.clients("clientGroupID", "clientID", "lastMutationID")
            VALUES ('foo', 'bar', 23)`,
        );
      });

      expect(await downstream.dequeue()).toMatchObject([
        'begin',
        {tag: 'begin'},
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {
          tag: 'delete',
          key: {id: 'world'},
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {
          tag: 'update',
          new: {id: 'hello', int: 123},
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {
          tag: 'truncate',
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {
          tag: 'insert',
          new: {id: 'include-me'},
        },
      ]);
      // Only zero.client updates for this SHARD_ID are replicated.
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {
          tag: 'insert',
          new: {
            clientGroupID: 'foo',
            clientID: 'bar',
            lastMutationID: 23n,
          },
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'commit',
        {tag: 'commit'},
        {watermark: expect.stringMatching(WATERMARK_REGEX)},
      ]);

      // Close the stream.
      changes.cancel();

      // Verify that the ACK was stored with the replication slot.
      // Postgres stores 1 + the LSN of the confirmed ACK.
      const results = await upstream<{confirmed: string}[]>`
    SELECT confirmed_flush_lsn as confirmed FROM pg_replication_slots
        WHERE slot_name = ${replicationSlot(SHARD_ID)}`;
      const expected = versionFromLexi(firstCommit[2].watermark) + 1n;
      expect(results).toEqual([
        {confirmed: fromLexiVersion(versionToLexi(expected))},
      ]);
    },
  );

  test.each([[withTriggers], [withoutTriggers]])(
    'start after confirmed flush %o',
    async init => {
      await init();
      const {replicaVersion} = getSubscriptionState(
        new StatementRunner(replicaDbFile.connect(lc)),
      );

      // Write three transactions, to experiment with different starting points.
      await upstream`INSERT INTO foo(id) VALUES('hello')`;
      await upstream`INSERT INTO foo(id) VALUES('world')`;
      await upstream`INSERT INTO foo(id) VALUES('foobar')`;

      const stream1 = await startStream('00');
      const changes1 = drainToQueue(stream1.changes);

      expect(stream1.initialWatermark).toEqual(oneAfter(replicaVersion));
      expect(await changes1.dequeue()).toMatchObject(['begin', {tag: 'begin'}]);
      expect(await changes1.dequeue()).toMatchObject(['data', {tag: 'insert'}]);
      const firstCommit = (await changes1.dequeue()) as Commit;
      expect(firstCommit).toMatchObject([
        'commit',
        {tag: 'commit'},
        {watermark: expect.stringMatching(WATERMARK_REGEX)},
      ]);

      expect(await changes1.dequeue()).toMatchObject(['begin', {tag: 'begin'}]);
      expect(await changes1.dequeue()).toMatchObject(['data', {tag: 'insert'}]);
      const secondCommit = (await changes1.dequeue()) as Commit;
      expect(secondCommit).toMatchObject([
        'commit',
        {tag: 'commit'},
        {watermark: expect.stringMatching(WATERMARK_REGEX)},
      ]);

      expect(await changes1.dequeue()).toMatchObject(['begin', {tag: 'begin'}]);
      expect(await changes1.dequeue()).toMatchObject(['data', {tag: 'insert'}]);
      const thirdCommit = (await changes1.dequeue()) as Commit;
      expect(thirdCommit).toMatchObject([
        'commit',
        {tag: 'commit'},
        {watermark: expect.stringMatching(WATERMARK_REGEX)},
      ]);

      stream1.changes.cancel();

      // Starting a new stream should replay at the original position since we did not ACK.
      const stream2 = await startStream('00');
      const changes2 = drainToQueue(stream2.changes);

      expect(stream2.initialWatermark).toEqual(oneAfter(replicaVersion));
      expect(await changes2.dequeue()).toMatchObject(['begin', {tag: 'begin'}]);
      expect(await changes2.dequeue()).toMatchObject(['data', {tag: 'insert'}]);
      expect(await changes2.dequeue()).toEqual(firstCommit);

      stream2.changes.cancel();

      // Still with no ACK, start a stream from after the secondCommit.
      const stream3 = await startStream(secondCommit[2].watermark);
      const changes3 = drainToQueue(stream3.changes);

      expect(stream3.initialWatermark).toEqual(
        oneAfter(secondCommit[2].watermark),
      );
      expect(await changes3.dequeue()).toMatchObject(['begin', {tag: 'begin'}]);
      expect(await changes3.dequeue()).toMatchObject(['data', {tag: 'insert'}]);
      expect(await changes3.dequeue()).toEqual(thirdCommit);

      stream3.changes.cancel();
    },
  );

  test('bad schema change error', async () => {
    const {changes} = await startStream('00');
    try {
      const downstream = drainToQueue(changes);

      // This statement should be successfully converted to Changes.
      await upstream`INSERT INTO foo(id) VALUES('hello')`;
      expect(await downstream.dequeue()).toMatchObject([
        'begin',
        {tag: 'begin'},
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {tag: 'insert'},
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'commit',
        {tag: 'commit'},
        {watermark: expect.stringMatching(WATERMARK_REGEX)},
      ]);

      // This statement should result in a replication error and
      // effectively freeze replication.
      await upstream.begin(async tx => {
        await tx`INSERT INTO foo(id) VALUES('wide')`;
        await tx`ALTER TABLE foo DROP CONSTRAINT foo_pk`;
        await tx`INSERT INTO foo(id) VALUES('world')`;
      });

      // The transaction should be rolled back.
      expect(await downstream.dequeue()).toMatchObject([
        'begin',
        {tag: 'begin'},
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {tag: 'insert'},
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'rollback',
        {tag: 'rollback'},
      ]);

      expect(logSink.messages[0]).toMatchObject([
        'error',
        {component: 'change-source'},
        [
          expect.stringMatching(
            'UnsupportedTableSchemaError: Table "foo" does not have a PRIMARY KEY',
          ),
          {tag: 'message'},
        ],
      ]);
    } finally {
      changes.cancel();
    }
  });

  test.each([
    ['ALTER TABLE foo ADD COLUMN bar int4', null],
    ['ALTER TABLE foo RENAME times TO timez', null],
    ['ALTER TABLE foo DROP COLUMN date', null],
    ['ALTER TABLE foo ALTER COLUMN times TYPE TIMESTAMPTZ[]', null],
    [
      // Rename column and rename back
      'ALTER TABLE foo RENAME times TO timez',
      'ALTER TABLE foo RENAME timez TO times',
    ],
    [
      // New table.
      `CREATE TABLE zero.oof(a TEXT PRIMARY KEY);` +
        `INSERT INTO zero.oof(a) VALUES ('1');`,
      null,
    ],
    [
      // New table that's dropped.
      `CREATE TABLE zero.oof(a TEXT PRIMARY KEY);` +
        `INSERT INTO zero.oof(a) VALUES ('1');`,
      `DROP TABLE zero.oof;`,
    ],
    [
      // Rename table and rename back.
      `ALTER TABLE zero.boo RENAME TO oof;` +
        `INSERT INTO zero.oof(a) VALUES ('1');`,
      `ALTER TABLE zero.oof RENAME TO boo;`,
    ],
    [
      // Drop a column and add it back.
      `ALTER TABLE zero.boo DROP d;` +
        `ALTER TABLE zero.boo ADD d TEXT;` +
        `INSERT INTO zero.boo(a) VALUES ('1');`,
      null,
    ],
    [
      // Shift columns so that they look similar.
      `ALTER TABLE zero.boo DROP b;` +
        `ALTER TABLE zero.boo RENAME c TO b;` +
        `ALTER TABLE zero.boo RENAME d TO c;` +
        `ALTER TABLE zero.boo ADD d TEXT;` +
        `INSERT INTO zero.boo(a) VALUES ('1');`,
      null,
    ],
  ])(
    'halt on schema change when ddlDetection = false: %s',
    async (before, after) => {
      await withoutTriggers();

      const {changes} = await startStream('00');
      try {
        const downstream = drainToQueue(changes);

        // This statement should be successfully converted to Changes.
        await upstream`INSERT INTO foo(id) VALUES('hello')`;
        expect(await downstream.dequeue()).toMatchObject([
          'begin',
          {tag: 'begin'},
        ]);
        expect(await downstream.dequeue()).toMatchObject([
          'data',
          {tag: 'insert'},
        ]);
        expect(await downstream.dequeue()).toMatchObject([
          'commit',
          {tag: 'commit'},
          {watermark: expect.stringMatching(WATERMARK_REGEX)},
        ]);

        // This statement should result in a replication error and
        // effectively freeze replication.
        await upstream.begin(async tx => {
          await tx.unsafe(before);
          await tx`INSERT INTO foo(id) VALUES('wide')`;
          await tx`INSERT INTO foo(id) VALUES('world')`;
          if (after) {
            await tx.unsafe(after);
          }
        });

        // The transaction should be rolled back.
        expect(await downstream.dequeue()).toMatchObject([
          'begin',
          {tag: 'begin'},
        ]);
        expect(await downstream.dequeue()).toMatchObject([
          'rollback',
          {tag: 'rollback'},
        ]);
        expect(await downstream.dequeue()).toMatchObject([
          'control',
          {tag: 'reset-required'},
        ]);

        expect(logSink.messages[0]).toMatchObject([
          'error',
          {component: 'change-source'},
          [
            expect.stringMatching(
              'UnsupportedSchemaChangeError: Replication halted. ' +
                'Schema changes cannot be reliably replicated without event trigger support. ' +
                'Resync the replica to recover.',
            ),
            {tag: 'relation'},
          ],
        ]);
      } finally {
        changes.cancel();
      }
    },
  );

  test('missing replication slot', async () => {
    // Purposely drop the replication slot to test the error case.
    await dropReplicationSlots(upstream);

    let err;
    try {
      await startStream('00');
    } catch (e) {
      err = e;
    }
    expect(err).not.toBeUndefined();
  });

  test('abort', async () => {
    const {changes} = await startStream('00');

    const results = await upstream<{pid: number}[]>`
      SELECT active_pid as pid from pg_replication_slots WHERE
        slot_name = ${replicationSlot(SHARD_ID)}`;
    const {pid} = results[0];

    await upstream`SELECT pg_terminate_backend(${pid})`;

    let err;
    try {
      for await (const _ of changes) {
        throw new Error('DatabaseError was not thrown');
      }
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(AbortError);
  });

  test('handoff', {retry: 3}, async () => {
    const {changes} = await startStream('00');

    // Starting another stream should stop the first.
    const {changes: changes2} = await startStream('00');

    let err;
    try {
      for await (const _ of changes) {
        throw new Error('DatabaseError was not thrown');
      }
    } catch (e) {
      err = e;
    }

    expect(err).toBeInstanceOf(AbortError);
    changes2.cancel();
  });

  test('error on wrong publications', async () => {
    let err;
    try {
      await initializeChangeSource(
        lc,
        upstreamURI,
        {id: SHARD_ID, publications: ['zero_different_publication']},
        replicaDbFile.path,
      );
    } catch (e) {
      err = e;
    }
    expect(err).toMatchInlineSnapshot(
      `[Error: Invalid ShardConfig. Requested publications [zero_different_publication] do not match synced publications: [zero_foo,zero_zero]]`,
    );
  });
});
