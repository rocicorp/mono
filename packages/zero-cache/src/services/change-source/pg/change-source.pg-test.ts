import {PG_OBJECT_IN_USE} from '@drdgvhbh/postgres-error-codes';
import {LogContext} from '@rocicorp/logger';
import {PostgresError} from 'postgres';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {AbortError} from '../../../../../shared/src/abort-error.ts';
import {TestLogSink} from '../../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../../shared/src/queue.ts';
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
import {type PostgresDB} from '../../../types/pg.ts';
import type {Source} from '../../../types/streams.ts';
import type {
  ChangeSource,
  ChangeStream,
} from '../../change-streamer/change-streamer-service.ts';
import {AutoResetSignal} from '../../change-streamer/schema/tables.ts';
import {getSubscriptionState} from '../../replicator/schema/replication-state.ts';
import type {
  Begin,
  ChangeStreamMessage,
  Commit,
} from '../protocol/current/downstream.ts';
import {initializePostgresChangeSource} from './change-source.ts';
import {fromLexiVersion} from './lsn.ts';
import {dropEventTriggerStatements} from './schema/ddl.ts';

const APP_ID = '23';
const SHARD_NUM = 1;

describe.skip('change-source/pg', {timeout: 30000, retry: 3}, () => {
  let logSink: TestLogSink;
  let lc: LogContext;
  let upstream: PostgresDB;
  let upstreamURI: string;
  let replicationSlot: string;
  let replicaDbFile: DbFile;
  let source: ChangeSource;
  let streams: ChangeStream[];

  beforeEach(async () => {
    streams = [];
    logSink = new TestLogSink();
    lc = new LogContext('error', {}, logSink);
    upstream = await testDBs.create(
      'change_source_pg_test_upstream_' + Math.random().toString(36).slice(2),
    );
    replicaDbFile = new DbFile(
      'change_source_pg_test_replica_' + Math.random().toString(36).slice(2),
    );

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

    return async () => {
      streams.forEach(s => s.changes.cancel());
      await testDBs.drop(upstream);
      replicaDbFile.delete();
    };
  }, 30000);

  function drainToQueue(
    sub: Source<ChangeStreamMessage>,
  ): Queue<ChangeStreamMessage> {
    const queue = new Queue<ChangeStreamMessage>();
    void (async () => {
      try {
        for await (const msg of sub) {
          queue.enqueue(msg);
        }
      } catch (e) {
        queue.enqueueRejection(e);
      }
    })();
    return queue;
  }

  const WATERMARK_REGEX = /[0-9a-z]{2,}/;

  async function setReplicaIdentityFull() {
    await upstream.unsafe(
      `
      ALTER TABLE foo REPLICA IDENTITY FULL;
      ALTER TABLE compound_key_same_order REPLICA IDENTITY FULL;
      ALTER TABLE compound_key_reverse_order REPLICA IDENTITY FULL;
      ALTER TABLE my.boo REPLICA IDENTITY FULL;
      `,
    );
  }

  async function startReplication(options?: {
    ignoredTables?: readonly string[];
  }) {
    const { ignoredTables = [] } = options ?? {};
    
    ({changeSource: source} = await initializePostgresChangeSource(
      lc,
      upstreamURI,
      {
        appID: APP_ID,
        publications: ['zero_foo', 'zero_zero'],
        shardNum: SHARD_NUM,
        ignoredTables,
      },
      replicaDbFile.path,
      {tableCopyWorkers: 5},
    ));

    [{slot: replicationSlot}] = await upstream<{slot: string}[]>`
    SELECT slot FROM ${upstream(`${APP_ID}_${SHARD_NUM}.replicas`)};
  `;
  }

  async function withTriggers() {
    await startReplication();
  }

  async function withoutTriggers() {
    await startReplication();
    await upstream.unsafe(
      `UPDATE "${APP_ID}_${SHARD_NUM}"."shardConfig" SET "ddlDetection" = false;` +
        dropEventTriggerStatements(APP_ID, SHARD_NUM),
    );
  }

  async function replicaIdentityFullWithTriggers() {
    await setReplicaIdentityFull();
    await withTriggers();
  }

  async function replicaIdentityFullWithoutTriggers() {
    await setReplicaIdentityFull();
    await withoutTriggers();
  }

  const MAX_ATTEMPTS_IF_REPLICATION_SLOT_ACTIVE = 10;

  async function startStream(watermark: string, src = source) {
    let err;
    for (let i = 0; i < MAX_ATTEMPTS_IF_REPLICATION_SLOT_ACTIVE; i++) {
      try {
        const stream = await src.startStream(watermark);
        // cleanup in afterEach() ensures that replication slots are released
        streams.push(stream);
        return stream;
      } catch (e) {
        if (e instanceof PostgresError && e.code === PG_OBJECT_IN_USE) {
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

  test.each([
    [withTriggers],
    [withoutTriggers],
    [replicaIdentityFullWithTriggers],
    [replicaIdentityFullWithoutTriggers],
  ])('filtered changes and acks %o', async init => {
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
          time: 14706123,
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
    const results = await upstream<{confirmed: string}[]>`
    SELECT confirmed_flush_lsn as confirmed FROM pg_replication_slots
        WHERE slot_name = ${replicationSlot}`;
    const expected = versionFromLexi(commit1[2].watermark);
    expect(results).toEqual([
      {confirmed: fromLexiVersion(versionToLexi(expected))},
    ]);
  });

  test.each([
    [withTriggers],
    [withoutTriggers],
    [replicaIdentityFullWithTriggers],
    [replicaIdentityFullWithoutTriggers],
  ])('relations with compound keys %o', async init => {
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
  });

  test.each([
    [withTriggers],
    [withoutTriggers],
    [replicaIdentityFullWithTriggers],
    [replicaIdentityFullWithoutTriggers],
  ])('start after confirmed flush %o', async init => {
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
  });

  test('set replica identity using index', async () => {
    await startReplication();
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
    await sleep(1000);

    // Poll upstream up to 10 times to account for timing variability.
    let afterState;
    for (let i = 0; i < 10; i++) {
      afterState = await upstream.unsafe(getReplicaIdentityStatement);
      if (afterState[0].replicaIdentity === Index) {
        break;
      }
      await sleep(200);
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
    await startReplication();
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
              'UnsupportedSchemaChangeError: Replication halted. Resync the replica to recover',
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
    await startReplication();
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
    await startReplication();
    const {changes} = await startStream('00');

    const results = await upstream<{pid: number}[]>`
      SELECT active_pid as pid from pg_replication_slots WHERE
        slot_name = ${replicationSlot}`;
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
    await startReplication();
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

  test('non-disruptive resync', async () => {
    await startReplication();
    const {changes: changes1} = await startStream('00');

    // Force another initial sync with an empty replica.
    const replicaFile2 = new DbFile('change_source_pg_test_replica2');
    const {changeSource: source2} = await initializePostgresChangeSource(
      lc,
      upstreamURI,
      {
        appID: APP_ID,
        publications: ['zero_foo', 'zero_zero'],
        shardNum: SHARD_NUM,
        ignoredTables: [] as string[],
      },
      replicaFile2.path,
      {tableCopyWorkers: 5},
    );

    // Initial sync should have created a second replication slot.
    const slots1 = await upstream<{slot: string}[]>`
      SELECT slot_name as slot FROM pg_replication_slots
        WHERE slot_name LIKE ${APP_ID + '\\_' + SHARD_NUM + '\\_%'}
    `.values();
    expect(slots1).toHaveLength(2);

    const replicas1 = await upstream.unsafe(`
      SELECT slot FROM "${APP_ID}_${SHARD_NUM}".replicas
    `);
    expect(replicas1).toHaveLength(2);

    // The original stream should still be active and receiving changes.
    const downstream1 = drainToQueue(changes1);
    await upstream`INSERT INTO foo(id, int) VALUES('hello', 0)`;
    expect(await downstream1.dequeue()).toMatchObject([
      'begin',
      {tag: 'begin'},
      {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
    ]);
    expect(await downstream1.dequeue()).toMatchObject([
      'data',
      {tag: 'insert'},
    ]);
    expect(await downstream1.dequeue()).toMatchObject([
      'commit',
      {tag: 'commit'},
      {watermark: expect.stringMatching(WATERMARK_REGEX)},
    ]);

    // Start a *third* initial sync with an empty replica.
    const replicaFile3 = new DbFile('change_source_pg_test_replica2');
    await initializePostgresChangeSource(
      lc,
      upstreamURI,
      {
        appID: APP_ID,
        publications: ['zero_foo', 'zero_zero'],
        shardNum: SHARD_NUM,
        ignoredTables: [] as string[],
      },
      replicaFile3.path,
      {tableCopyWorkers: 5},
    );

    // There should now be 3 replication slot2.
    const slots2 = await upstream<{slot: string}[]>`
        SELECT slot_name as slot FROM pg_replication_slots
          WHERE slot_name LIKE ${APP_ID + '\\_' + SHARD_NUM + '\\_%'}
      `.values();
    expect(slots2).toHaveLength(3);

    // Starting a subscription on the new slot should kill the old
    // subscription and drop the first replication slot.
    const {changes: changes2} = await startStream('00', source2);

    await expect(() => downstream1.dequeue()).rejects.toThrow(AbortError);

    // The new stream should get the same changes since it was synced
    // before they occurred.
    const downstream2 = drainToQueue(changes2);
    expect(await downstream2.dequeue()).toMatchObject([
      'begin',
      {tag: 'begin'},
      {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
    ]);
    expect(await downstream2.dequeue()).toMatchObject([
      'data',
      {tag: 'insert'},
    ]);
    expect(await downstream2.dequeue()).toMatchObject([
      'commit',
      {tag: 'commit'},
      {watermark: expect.stringMatching(WATERMARK_REGEX)},
    ]);

    changes2.cancel();

    // Verify that the replica rows have been cleaned up.
    const replicas2 = await upstream.unsafe(`
      SELECT slot FROM "${APP_ID}_${SHARD_NUM}".replicas
    `);
    expect(replicas2).toEqual(replicas1.slice(1));

    // Verify that the two latter slots remain. (Use waitFor to reduce
    // flakiness because the drop is non-transactional.)
    await vi.waitFor(
      async () => {
        const slots3 = await upstream<{slot: string}[]>`
      SELECT slot_name as slot FROM pg_replication_slots
        WHERE slot_name LIKE ${APP_ID + '\\_' + SHARD_NUM + '\\_%'}
    `.values();
        expect(slots3).toEqual(slots2.slice(1));
      },
      {
        interval: 100,
      },
    );

    replicaFile2.delete();
    replicaFile3.delete();
  });

  test('AutoReset on changed publications', async () => {
    await startReplication();
    let err;
    try {
      await initializePostgresChangeSource(
        lc,
        upstreamURI,
        {
          appID: APP_ID,
          shardNum: SHARD_NUM,
          publications: ['zero_different_publication'],
          ignoredTables: [] as string[],
        },
        replicaDbFile.path,
        {tableCopyWorkers: 5},
      );
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(AutoResetSignal);
    expect(err).toMatchInlineSnapshot(
      `[AutoResetSignal: Requested publications [zero_different_publication] do not match configured publications: [zero_foo,zero_zero]]`,
    );
  });

  test('AutoReset on missing publications', async () => {
    await startReplication();
    await upstream`DROP PUBLICATION zero_foo`;

    let err;
    try {
      await startReplication();
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(AutoResetSignal);
    expect(err).toMatchInlineSnapshot(
      `[AutoResetSignal: Upstream publications [zero_zero,_23_metadata_1] do not contain all subscribed publications [_23_metadata_1,zero_foo,zero_zero]]`,
    );
  });

  test('ignored tables excluded from initial sync', async () => {
    // Insert data into both ignored and non-ignored tables BEFORE initial sync
    await upstream.begin(async tx => {
      await tx`INSERT INTO foo(id) VALUES('test1')`;
      await tx`INSERT INTO my.boo(a, b) VALUES('ignored1', 'data1')`;
    });
    
    // Start replication with my.boo ignored - this triggers initial sync
    await startReplication({ ignoredTables: ['my.boo'] });

    // Verify foo table has data in replica (synced during initial sync)
    const replica = replicaDbFile.connect(lc);
    const fooRows = replica.prepare('SELECT * FROM foo').all();
    expect(fooRows).toHaveLength(1);
    expect(fooRows[0]).toMatchObject({id: 'test1'});

    // Verify my.boo table exists but is empty (ignored during initial sync)
    // SQLite stores schema.table as "schema.table" in a flat namespace
    const booRows = replica.prepare('SELECT * FROM "my.boo"').all();
    expect(booRows).toHaveLength(0);
    replica.close();
  });

  test('changes to ignored tables are dropped', async () => {
    await startReplication({ ignoredTables: ['my.boo'] });
    
    const {changes, acks} = await startStream('00');
    const downstream = drainToQueue(changes);

    // Insert into both ignored and non-ignored tables
    await upstream.begin(async tx => {
      await tx`INSERT INTO foo(id) VALUES('test2')`;
      await tx`INSERT INTO my.boo(a, b) VALUES('ignored2', 'data2')`;
    });

    const begin = (await downstream.dequeue()) as Begin;
    expect(begin).toMatchObject([
      'begin',
      {tag: 'begin'},
      {commitWatermark: expect.stringMatching(WATERMARK_REGEX)},
    ]);
    
    // Should only see the foo insert, not the my.boo insert
    const data = await downstream.dequeue();
    expect(data).toMatchObject([
      'data',
      {
        tag: 'insert',
        new: {id: 'test2'},
      },
    ]);
    
    const commit = (await downstream.dequeue()) as Commit;
    expect(commit).toMatchObject([
      'commit',
      {tag: 'commit'},
      {watermark: begin[2]?.commitWatermark},
    ]);
    
    acks.push(['status', {}, commit[2]]);
  });

  test('AutoReset on changed ignored tables', async () => {
    await startReplication({ ignoredTables: ['my.boo'] });
    
    let err;
    try {
      // Try to reinitialize with different ignored tables
      await initializePostgresChangeSource(
        lc,
        upstreamURI,
        {
          appID: APP_ID,
          shardNum: SHARD_NUM,
          publications: ['zero_foo', 'zero_zero'],
          ignoredTables: ['public.foo'],  // Different ignored tables
        },
        replicaDbFile.path,
        {tableCopyWorkers: 5},
      );
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(AutoResetSignal);
    expect((err as Error).message).toContain('Requested ignored tables');
  });

  test('ignored table with row filter in publication', async () => {
    // Create a table with a row filter that will be part of existing publication
    await upstream`
      CREATE TABLE filtered_table(id INT4 PRIMARY KEY, status TEXT);
      ALTER PUBLICATION zero_foo ADD TABLE filtered_table WHERE (status = 'active');
    `.simple();
    
    // Insert data that matches and doesn't match the filter
    await upstream.begin(async tx => {
      await tx`INSERT INTO filtered_table(id, status) VALUES(1, 'active')`;
      await tx`INSERT INTO filtered_table(id, status) VALUES(2, 'inactive')`;
    });

    // Start replication with the table in both publication AND ignored list
    await startReplication({
      ignoredTables: ['public.filtered_table']
    });

    // Table should be created but no data should sync (ignored takes precedence)
    const replica = replicaDbFile.connect(lc);
    const tables = replica.prepare("SELECT name FROM sqlite_master WHERE type='table'").all() as Array<{name: string}>;
    expect(tables.map(t => t.name)).toContain('filtered_table');
    
    const rows = replica.prepare('SELECT * FROM filtered_table').all();
    expect(rows).toEqual([]);

    // Even changes that match the row filter should be dropped
    await upstream`INSERT INTO filtered_table(id, status) VALUES(3, 'active')`.simple();
    await sleep(200);
    
    const rowsAfter = replica.prepare('SELECT * FROM filtered_table').all();
    expect(rowsAfter).toEqual([]);
    replica.close();
  });

  test('ignored table in multiple publications', async () => {
    // Create a table that will be added to both existing publications
    await upstream`
      CREATE TABLE shared_table(id INT4 PRIMARY KEY, category TEXT);
      ALTER PUBLICATION zero_foo ADD TABLE shared_table WHERE (category = 'A');
      ALTER PUBLICATION zero_zero ADD TABLE shared_table WHERE (category = 'B');
    `.simple();
    
    await upstream.begin(async tx => {
      await tx`INSERT INTO shared_table(id, category) VALUES(1, 'A')`;
      await tx`INSERT INTO shared_table(id, category) VALUES(2, 'B')`;
      await tx`INSERT INTO shared_table(id, category) VALUES(3, 'C')`;
    });

    // Start replication with the table ignored
    await startReplication({
      ignoredTables: ['public.shared_table']
    });

    // Table should be created but empty despite being in multiple publications
    const replica = replicaDbFile.connect(lc);
    const tables = replica.prepare("SELECT name FROM sqlite_master WHERE type='table'").all() as Array<{name: string}>;
    expect(tables.map(t => t.name)).toContain('shared_table');
    
    const rows = replica.prepare('SELECT * FROM shared_table').all();
    expect(rows).toEqual([]);
    replica.close();
  });

  test('exact table name matching for ignored tables', async () => {
    // Create tables with similar names and add them to existing publication
    await upstream`
      CREATE TABLE test_table(id INT4 PRIMARY KEY);
      CREATE TABLE test_table_2(id INT4 PRIMARY KEY);
      CREATE TABLE other_test_table(id INT4 PRIMARY KEY);
      ALTER PUBLICATION zero_foo ADD TABLE test_table, test_table_2, other_test_table;
    `.simple();
    
    await upstream.begin(async tx => {
      await tx`INSERT INTO test_table(id) VALUES(1)`;
      await tx`INSERT INTO test_table_2(id) VALUES(2)`;
      await tx`INSERT INTO other_test_table(id) VALUES(3)`;
    });

    // Only ignore the exact table name
    await startReplication({
      ignoredTables: ['public.test_table']
    });

    // Only test_table should be empty
    const replica = replicaDbFile.connect(lc);
    const testTableRows = replica.prepare('SELECT * FROM test_table').all();
    expect(testTableRows).toEqual([]);
    
    // Similar named tables should have their data
    const testTable2Rows = replica.prepare('SELECT * FROM test_table_2').all();
    expect(testTable2Rows).toHaveLength(1);
    expect(testTable2Rows[0]).toMatchObject({id: 2});
    
    const otherTestTableRows = replica.prepare('SELECT * FROM other_test_table').all();
    expect(otherTestTableRows).toHaveLength(1);
    expect(otherTestTableRows[0]).toMatchObject({id: 3});
    replica.close();
  });

  test('ignored table with schema qualification', async () => {
    // Create tables in different schemas with same name and add to existing publications
    await upstream`
      CREATE SCHEMA other_schema;
      CREATE TABLE foo2(id INT4 PRIMARY KEY);
      CREATE TABLE other_schema.foo2(id INT4 PRIMARY KEY);
      ALTER PUBLICATION zero_foo ADD TABLE foo2;
      ALTER PUBLICATION zero_zero ADD TABLE other_schema.foo2;
    `.simple();
    
    await upstream.begin(async tx => {
      await tx`INSERT INTO foo2(id) VALUES(1)`;
      await tx`INSERT INTO other_schema.foo2(id) VALUES(2)`;
    });

    // Only ignore the one in other_schema
    await startReplication({
      ignoredTables: ['other_schema.foo2']
    });

    // public.foo2 should have data
    const replica = replicaDbFile.connect(lc);
    const publicRows = replica.prepare('SELECT * FROM foo2').all();
    expect(publicRows).toHaveLength(1);
    expect(publicRows[0]).toMatchObject({id: 1});
    
    // other_schema.foo2 should be empty (SQLite uses flat namespace)
    const otherRows = replica.prepare('SELECT * FROM "other_schema.foo2"').all();
    expect(otherRows).toEqual([]);
    replica.close();
  });
});
