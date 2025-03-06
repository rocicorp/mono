import {PG_OBJECT_IN_USE} from '@drdgvhbh/postgres-error-codes';
import {LogContext} from '@rocicorp/logger';
import {DatabaseError} from 'pg-protocol';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {AbortError} from '../../../../../shared/src/abort-error.ts';
import {TestLogSink} from '../../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../../shared/src/queue.ts';
import {promiseVoid} from '../../../../../shared/src/resolved-promises.ts';
import {sleep} from '../../../../../shared/src/sleep.ts';
import {Default, Index} from '../../../db/postgres-replica-identity-enum.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {
  dropReplicationSlots,
  getConnectionURI,
  testDBs,
} from '../../../test/db.ts';
import {DbFile} from '../../../test/lite.ts';
import {versionFromLexi, versionToLexi} from '../../../types/lexi-version.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {Source} from '../../../types/streams.ts';
import type {
  ChangeSource,
  ChangeStream,
} from '../../change-streamer/change-streamer-service.ts';
import {getSubscriptionState} from '../../replicator/schema/replication-state.ts';
import type {
  Begin,
  ChangeStreamMessage,
  Commit,
} from '../protocol/current/downstream.ts';
import {initializePostgresChangeSource} from './change-source.ts';
import {replicationSlot} from './initial-sync.ts';
import {fromLexiVersion} from './lsn.ts';
import {dropEventTriggerStatements} from './schema/ddl.ts';

const APP_ID = '23';
const SHARD_NUM = 1;

describe('change-source/pg', {timeout: 30000}, () => {
  let logSink: TestLogSink;
  let lc: LogContext;
  let upstream: PostgresDB;
  let upstreamURI: string;
  let replicaDbFile: DbFile;
  let source: ChangeSource;
  let streams: ChangeStream[];

  beforeEach(async () => {
    streams = [];
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
      times TIMESTAMP[],
      num NUMERIC
    );
    CREATE TABLE compound_key_same_order(
      a TEXT NOT NULL,
      b TEXT NOT NULL,
      PRIMARY KEY (a, b)
    );
    CREATE TABLE compound_key_reverse_order(
      a TEXT NOT NULL,
      b TEXT NOT NULL,
      PRIMARY KEY (b, a)
    );
    CREATE PUBLICATION zero_foo FOR TABLE foo WHERE (id != 'exclude-me'), 
      TABLE compound_key_same_order, compound_key_reverse_order;

    CREATE SCHEMA IF NOT EXISTS my;
    CREATE TABLE my.boo(
      a TEXT PRIMARY KEY, b TEXT, c TEXT, d TEXT
    );
    CREATE PUBLICATION zero_zero FOR TABLES IN SCHEMA my;
    `);

    source = (
      await initializePostgresChangeSource(
        lc,
        upstreamURI,
        {
          appID: APP_ID,
          publications: ['zero_foo', 'zero_zero'],
          shardNum: SHARD_NUM,
        },
        replicaDbFile.path,
        {tableCopyWorkers: 5, rowBatchSize: 10000},
      )
    ).changeSource;
  }, 30000);

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
        queue.enqueue(msg);
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
      `UPDATE "${APP_ID}_${SHARD_NUM}"."shardConfig" SET "ddlDetection" = false;` +
        dropEventTriggerStatements(APP_ID, SHARD_NUM),
    );
  }

  const MAX_ATTEMPTS_IF_REPLICATION_SLOT_ACTIVE = 10;

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
          // eslint-disable-next-line no-console
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

      const {changes, acks} = await startStream('00');
      const downstream = drainToQueue(changes);

      await upstream.begin(async tx => {
        await tx`INSERT INTO foo(id) VALUES('hello')`;
        await tx`INSERT INTO foo(id) VALUES('world')`;
        await tx`
      INSERT INTO foo(id, int, big, flt, bool, timea, timeb, date, time, dates, times, num) 
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
               ARRAY['2019-01-12T00:30:35.654321'::timestamp, '2019-01-12T00:30:35.123456'::timestamp],
               123456789012
               )`;
        // schemaVersions
        await tx`
      UPDATE ${tx(APP_ID)}."schemaVersions" SET "maxSupportedVersion" = 2;
      `;
      });

      const begin1 = (await downstream.dequeue()) as Begin;
      expect(begin1).toMatchObject([
        'begin',
        {tag: 'begin'},
        {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
      ]);
      expect(begin1[2].commitWatermark > replicaVersion).toBe(true);
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
            num: 123456789012,
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
      const commit1 = (await downstream.dequeue()) as Commit;
      expect(commit1).toMatchObject([
        'commit',
        {tag: 'commit'},
        {watermark: begin1[2]?.commitWatermark},
      ]);
      acks.push(['status', {}, commit1[2]]);

      // Write more upstream changes.
      await upstream.begin(async tx => {
        await tx`DELETE FROM foo WHERE id = 'world'`;
        await tx`UPDATE foo SET int = 123 WHERE id = 'hello';`;
        await tx`TRUNCATE foo`;
        // Should be excluded by zero_all.
        await tx`INSERT INTO foo(id) VALUES ('exclude-me')`;
        await tx`INSERT INTO foo(id) VALUES ('include-me')`;
        // clients change that should be included.
        await tx.unsafe(
          `INSERT INTO "${APP_ID}_${SHARD_NUM}".clients("clientGroupID", "clientID", "lastMutationID")
            VALUES ('foo', 'bar', 23)`,
        );
      });

      const begin2 = (await downstream.dequeue()) as Begin;
      expect(begin2).toMatchObject([
        'begin',
        {tag: 'begin'},
        {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
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
      // Only client updates for this shard are replicated.
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
        {watermark: begin2[2]?.commitWatermark},
      ]);

      // Close the stream.
      changes.cancel();

      // Verify that the ACK was stored with the replication slot.
      // Postgres stores 1 + the LSN of the confirmed ACK.
      const results = await upstream<{confirmed: string}[]>`
    SELECT confirmed_flush_lsn as confirmed FROM pg_replication_slots
        WHERE slot_name = ${replicationSlot({
          appID: APP_ID,
          shardNum: SHARD_NUM,
        })}`;
      const expected = versionFromLexi(commit1[2].watermark) + 1n;
      expect(results).toEqual([
        {confirmed: fromLexiVersion(versionToLexi(expected))},
      ]);
    },
  );

  test.each([[withTriggers], [withoutTriggers]])(
    'relations with compound keys %o',
    async init => {
      await init();
      const {replicaVersion} = getSubscriptionState(
        new StatementRunner(replicaDbFile.connect(lc)),
      );

      const {changes, acks} = await startStream('00');
      const downstream = drainToQueue(changes);

      await upstream.begin(async tx => {
        await tx`INSERT INTO compound_key_same_order(a, b) VALUES('c', 'd')`;
        await tx`INSERT INTO compound_key_reverse_order(a, b) VALUES('e', 'f')`;
      });

      const begin1 = (await downstream.dequeue()) as Begin;
      expect(begin1).toMatchObject([
        'begin',
        {tag: 'begin'},
        {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
      ]);
      expect(begin1[2].commitWatermark > replicaVersion).toBe(true);
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {
          tag: 'insert',
          new: {a: 'c', b: 'd'},
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {
          tag: 'insert',
          new: {a: 'e', b: 'f'},
        },
      ]);
      const commit1 = (await downstream.dequeue()) as Commit;
      expect(commit1).toMatchObject([
        'commit',
        {tag: 'commit'},
        {watermark: begin1[2]?.commitWatermark},
      ]);
      acks.push(['status', {}, commit1[2]]);
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

      const begin1 = (await changes1.dequeue()) as Begin;
      expect(begin1).toMatchObject([
        'begin',
        {tag: 'begin'},
        {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
      ]);
      expect(begin1[2].commitWatermark > replicaVersion).toBe(true);
      expect(await changes1.dequeue()).toMatchObject(['data', {tag: 'insert'}]);
      const commit1 = (await changes1.dequeue()) as Commit;
      expect(commit1).toMatchObject([
        'commit',
        {tag: 'commit'},
        {watermark: begin1[2]?.commitWatermark},
      ]);

      const begin2 = (await changes1.dequeue()) as Begin;
      expect(begin2).toMatchObject([
        'begin',
        {tag: 'begin'},
        {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
      ]);
      expect(await changes1.dequeue()).toMatchObject(['data', {tag: 'insert'}]);
      const commit2 = (await changes1.dequeue()) as Commit;
      expect(commit2).toMatchObject([
        'commit',
        {tag: 'commit'},
        {watermark: begin2[2]?.commitWatermark},
      ]);

      const begin3 = (await changes1.dequeue()) as Begin;
      expect(begin3).toMatchObject([
        'begin',
        {tag: 'begin'},
        {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
      ]);
      expect(await changes1.dequeue()).toMatchObject(['data', {tag: 'insert'}]);
      const commit3 = (await changes1.dequeue()) as Commit;
      expect(commit3).toMatchObject([
        'commit',
        {tag: 'commit'},
        {watermark: begin3[2]?.commitWatermark},
      ]);

      stream1.changes.cancel();

      // Starting a new stream should replay at the original position since we did not ACK.
      const stream2 = await startStream('00');
      const changes2 = drainToQueue(stream2.changes);

      expect(await changes2.dequeue()).toMatchObject(begin1);
      expect(await changes2.dequeue()).toMatchObject(['data', {tag: 'insert'}]);
      expect(await changes2.dequeue()).toEqual(commit1);

      stream2.changes.cancel();

      // Still with no ACK, start a stream from after the secondCommit.
      const stream3 = await startStream(commit2[2].watermark);
      const changes3 = drainToQueue(stream3.changes);

      expect(await changes3.dequeue()).toMatchObject(begin3);
      expect(await changes3.dequeue()).toMatchObject(['data', {tag: 'insert'}]);
      expect(await changes3.dequeue()).toEqual(commit3);

      stream3.changes.cancel();
    },
  );

  test('replica identity full', async () => {
    await withTriggers();
    const {replicaVersion} = getSubscriptionState(
      new StatementRunner(replicaDbFile.connect(lc)),
    );

    const {changes} = await startStream('00');
    const downstream = drainToQueue(changes);

    await upstream.begin(async tx => {
      await tx`ALTER TABLE foo REPLICA IDENTITY FULL`;
      await tx`INSERT INTO foo(id, int) VALUES('hello', 1)`;
      await tx`INSERT INTO foo(id, int) VALUES('world', 2)`;
      await tx`UPDATE foo SET id = 'bello' WHERE id = 'hello'`;
      await tx`DELETE FROM foo WHERE id = 'world'`;
    });

    const begin1 = (await downstream.dequeue()) as Begin;
    expect(begin1).toMatchObject([
      'begin',
      {tag: 'begin'},
      {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
    ]);
    expect(begin1[2].commitWatermark > replicaVersion).toBe(true);
    expect(await downstream.dequeue()).toMatchObject([
      'data',
      {
        tag: 'insert',
        relation: {replicaIdentity: 'full'},
        new: {id: 'hello', int: 1},
      },
    ]);
    expect(await downstream.dequeue()).toMatchObject([
      'data',
      {
        tag: 'insert',
        relation: {replicaIdentity: 'full'},
        new: {id: 'world', int: 2},
      },
    ]);
    expect(await downstream.dequeue()).toMatchObject([
      'data',
      {
        tag: 'update',
        relation: {replicaIdentity: 'full'},
        new: {id: 'bello', int: 1},
        key: {id: 'hello', int: 1},
      },
    ]);
    expect(await downstream.dequeue()).toMatchObject([
      'data',
      {
        tag: 'delete',
        relation: {replicaIdentity: 'full'},
        key: {id: 'world', int: 2},
      },
    ]);

    const commit1 = (await downstream.dequeue()) as Commit;
    expect(commit1[2].watermark).toEqual(begin1[2].commitWatermark);
  });

  test('set replica identity using index', async () => {
    const stream = await startStream('00');
    const changes = drainToQueue(stream.changes);

    const getReplicaIdentityStatement = `
      SELECT relname as name, relreplident as "replicaIdentity" FROM pg_class 
        WHERE relname = 'join_table';
      SELECT relname as name, indisreplident as "isReplicaIdentity" FROM pg_index
        JOIN pg_class ON indexrelid = oid
        WHERE pg_class.relname = 'join_key';
    `;

    // Create a table without a primary key but suitable index.
    const beforeState = await upstream.unsafe(`
      CREATE TABLE my.join_table(id1 TEXT NOT NULL, id2 TEXT NOT NULL);
      CREATE UNIQUE INDEX join_key ON my.join_table(id1, id2);
      ${getReplicaIdentityStatement}
    `);
    expect(beforeState).toEqual([
      [],
      [{name: 'join_table', replicaIdentity: Default}],
      [{name: 'join_key', isReplicaIdentity: false}],
    ]);

    expect(await changes.dequeue()).toMatchObject([
      'begin',
      {tag: 'begin'},
      {commitWatermark: WATERMARK_REGEX},
    ]);
    expect(await changes.dequeue()).toMatchObject([
      'data',
      {tag: 'create-table'},
    ]);
    expect(await changes.dequeue()).toMatchObject([
      'data',
      {tag: 'create-index'},
    ]);
    expect(await changes.dequeue()).toMatchObject([
      'commit',
      {tag: 'commit'},
      {watermark: WATERMARK_REGEX},
    ]);

    // Let the 500ms timeout fire.
    await sleep(550);

    // Poll upstream up to 5 times to account for timing variability.
    let afterState;
    for (let i = 0; i < 5; i++) {
      afterState = await upstream.unsafe(getReplicaIdentityStatement);
      if (afterState[0].replicaIdentity === Index) {
        break;
      }
      await sleep(100);
    }
    expect(afterState).toEqual([
      [{name: 'join_table', replicaIdentity: Index}],
      [{name: 'join_key', isReplicaIdentity: true}],
    ]);

    stream.changes.cancel();
  });

  test.each([
    [
      'UnsupportedTableSchemaError: Table "invalid/character\\$" has invalid characters.',
      `
        ALTER TABLE foo RENAME TO "invalid/character$";
        INSERT INTO "invalid/character$"(id) VALUES('world');
      `,
    ],
    [
      'UnsupportedSchemaChangeError: Replication halted',
      `ALTER TABLE foo ADD bar TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP`,
    ],
    [
      'UnsupportedSchemaChangeError: Replication halted',
      `ALTER TABLE foo ADD pubid INT DEFAULT random()`,
    ],
  ])('bad schema change error: %s', async (errMsg, stmt) => {
    const {changes} = await startStream('00');
    try {
      const downstream = drainToQueue(changes);

      // This statement should be successfully converted to Changes.
      await upstream`INSERT INTO foo(id) VALUES('hello')`;
      expect(await downstream.dequeue()).toMatchObject([
        'begin',
        {tag: 'begin'},
        {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
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
        await tx.unsafe(stmt);
      });

      // The transaction should be rolled back.
      expect(await downstream.dequeue()).toMatchObject([
        'begin',
        {tag: 'begin'},
        {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'data',
        {tag: 'insert'},
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
        [expect.stringMatching(errMsg), {tag: 'message'}],
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
    ['ALTER TABLE foo ALTER COLUMN int SET NOT NULL', null],
    [
      // Rename column and rename back
      'ALTER TABLE foo RENAME times TO timez',
      'ALTER TABLE foo RENAME timez TO times',
    ],
    [
      // New table.
      `CREATE TABLE my.oof(a TEXT PRIMARY KEY);` +
        `INSERT INTO my.oof(a) VALUES ('1');`,
      null,
    ],
    [
      // New table that's dropped.
      `CREATE TABLE my.oof(a TEXT PRIMARY KEY);` +
        `INSERT INTO my.oof(a) VALUES ('1');`,
      `DROP TABLE my.oof;`,
    ],
    [
      // Rename table and rename back.
      `ALTER TABLE my.boo RENAME TO oof;` +
        `INSERT INTO my.oof(a) VALUES ('1');`,
      `ALTER TABLE my.oof RENAME TO boo;`,
    ],
    [
      // Drop a column and add it back.
      `ALTER TABLE my.boo DROP d;` +
        `ALTER TABLE my.boo ADD d TEXT;` +
        `INSERT INTO my.boo(a) VALUES ('1');`,
      null,
    ],
    [
      // Shift columns so that they look similar.
      `ALTER TABLE my.boo DROP b;` +
        `ALTER TABLE my.boo RENAME c TO b;` +
        `ALTER TABLE my.boo RENAME d TO c;` +
        `ALTER TABLE my.boo ADD d TEXT;` +
        `INSERT INTO my.boo(a) VALUES ('1');`,
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
        await upstream`INSERT INTO foo(id, int) VALUES('hello', 0)`;
        expect(await downstream.dequeue()).toMatchObject([
          'begin',
          {tag: 'begin'},
          {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
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
          await tx`INSERT INTO foo(id, int) VALUES('wide', 1)`;
          await tx`INSERT INTO foo(id, int) VALUES('world', 2)`;
          if (after) {
            await tx.unsafe(after);
          }
        });

        // The transaction should be rolled back.
        expect(await downstream.dequeue()).toMatchObject([
          'begin',
          {tag: 'begin'},
          {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
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
        slot_name = ${replicationSlot({appID: APP_ID, shardNum: SHARD_NUM})}`;
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
      await initializePostgresChangeSource(
        lc,
        upstreamURI,
        {
          appID: APP_ID,
          shardNum: SHARD_NUM,
          publications: ['zero_different_publication'],
        },
        replicaDbFile.path,
        {tableCopyWorkers: 5, rowBatchSize: 10000},
      );
    } catch (e) {
      err = e;
    }
    expect(err).toMatchInlineSnapshot(
      `[Error: Invalid ShardConfig. Requested publications [zero_different_publication] do not match synced publications: [zero_foo,zero_zero]]`,
    );
  });
});
