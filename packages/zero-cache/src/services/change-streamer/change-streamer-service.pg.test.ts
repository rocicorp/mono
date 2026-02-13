import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {beforeEach, describe, expect, vi, type Mock} from 'vitest';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {stringify} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {expectTables, test, type PgTest} from '../../test/db.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription, type Result} from '../../types/subscription.ts';
import {type ChangeStreamMessage} from '../change-source/protocol/current/downstream.ts';
import type {StatusMessage} from '../change-source/protocol/current/status.ts';
import {
  getSubscriptionState,
  initReplicationState,
  type SubscriptionState,
} from '../replicator/schema/replication-state.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {initializeStreamer} from './change-streamer-service.ts';
import {
  PROTOCOL_VERSION,
  type ChangeStreamerService,
  type Downstream,
} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';
import {AutoResetSignal, ensureReplicationConfig} from './schema/tables.ts';

describe('change-streamer/service', () => {
  let lc: LogContext;
  let replicaConfig: SubscriptionState;
  let sql: PostgresDB;
  let streamer: ChangeStreamerService;
  let changes: Subscription<ChangeStreamMessage>;
  let acks: Queue<StatusMessage>;
  let streamerDone: Promise<void>;

  // vi.useFakeTimers() does not play well with the postgres client.
  // Inject a manual mock instead.
  let setTimeoutFn: Mock<typeof setTimeout>;

  const REPLICA_VERSION = '01';
  const shard = {appID: 'zoro', shardNum: 3};

  beforeEach<PgTest>(async ({testDBs}) => {
    lc = createSilentLogContext();

    sql = await testDBs.create('change_streamer_test_change_db', undefined, {
      sendStringAsJson: true,
    });

    const replica = new Database(lc, ':memory:');
    initReplicationState(replica, ['zero_data'], REPLICA_VERSION);
    replicaConfig = getSubscriptionState(new StatementRunner(replica));

    changes = Subscription.create();
    acks = new Queue();
    setTimeoutFn = vi.fn();

	    streamer = await initializeStreamer(
	      lc,
	      shard,
	      'task-id',
	      'change.streamer:12345',
      'ws',
      sql,
      {
        startStream: () =>
          Promise.resolve({
            initialWatermark: '02',
            changes,
            acks: {push: status => acks.enqueue(status)},
          }),
      },
	      replicaConfig,
	      true,
	      0.04,
	      setTimeoutFn as unknown as typeof setTimeout,
	    );
    streamerDone = streamer.run();

    return async () => {
      await streamer.stop();
      await testDBs.drop(sql);
    };
  });

  function drainToQueue(sub: Source<Downstream>): Queue<Downstream> {
    const queue = new Queue<Downstream>();
    void (async () => {
      for await (const msg of sub) {
        queue.enqueue(msg);
      }
    })();
    return queue;
  }

  async function nextChange(sub: Queue<Downstream>) {
    const down = await sub.dequeue();
    assert(down[0] !== 'error', `Unexpected error ${stringify(down)}`);
    return down[1];
  }

  async function verifyNoMoreChanges(sub: Queue<Downstream>) {
    const down = await sub.dequeue(
      ['error', {type: 0, message: 'timed-out'}],
      50,
    );
    expect(down).toEqual(['error', {type: 0, message: 'timed-out'}]);
  }

  async function expectAcks(...watermarks: string[]) {
    for (const watermark of watermarks) {
      expect((await acks.dequeue())[2].watermark).toBe(watermark);
    }
  }

  const messages = new ReplicationMessages({foo: 'id'});

  test('get empty changelog state', async () => {
    expect(await streamer.getChangeLogState()).toEqual({
      minWatermark: '01',
      replicaVersion: '01',
    });
  });

  test('immediate forwarding, transaction storage', async () => {
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });
    const downstream = drainToQueue(sub);

    changes.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push([
      'commit',
      messages.commit({extra: 'fields'}),
      {watermark: '09'},
    ]);

    changes.push(['status', {}, {watermark: '0b'}]);

    expect(await nextChange(downstream)).toMatchObject({tag: 'status'});
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'hello'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      extra: 'fields',
    });

    // Await the ACK for the single commit, then the status message.
    await expectAcks('09', '0b');

    expect(
      await sql`SELECT watermark, change->'tag' FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "09",
          "begin",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "commit",
        ],
      ]
    `);
    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'task-id',
          ownerAddress: 'change.streamer:12345',
          lastWatermark: '09',
        },
      ],
    });
  });

  test('subscriber catchup and continuation', async () => {
    // Process some changes upstream.
    changes.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push([
      'commit',
      messages.commit({extra: 'stuff'}),
      {watermark: '09'},
    ]);

    // Subscribe to the original watermark.
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    changes.push(['status', {}, {watermark: '0a'}]);

    // Process more upstream changes.
    changes.push(['begin', messages.begin(), {commitWatermark: '0b'}]);
    changes.push(['data', messages.delete('foo', {id: 'world'})]);
    changes.push([
      'commit',
      messages.commit({more: 'stuff'}),
      {watermark: '0b'},
    ]);

    changes.push(['status', {}, {watermark: '0d'}]);

    // Verify that all changes were sent to the subscriber ...
    const downstream = drainToQueue(sub);
    expect(await nextChange(downstream)).toMatchObject({tag: 'status'});
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'hello'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      extra: 'stuff',
    });
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'delete',
      key: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      more: 'stuff',
    });

    // Two commits with intervening status messages
    await expectAcks('09', '0a', '0b', '0d');

    expect(
      await sql`SELECT watermark, change->'tag' FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "09",
          "begin",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "commit",
        ],
        [
          "0b",
          "begin",
        ],
        [
          "0b",
          "delete",
        ],
        [
          "0b",
          "commit",
        ],
      ]
    `);
    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'task-id',
          ownerAddress: 'change.streamer:12345',
          lastWatermark: '0b',
        },
      ],
    });
  });

  test('subscriber catchup and continuation after rollback', async () => {
    // Process some changes upstream.
    changes.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push([
      'commit',
      messages.commit({extra: 'stuff'}),
      {watermark: '09'},
    ]);

    // Subscribe to the original watermark.
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    // Process more upstream changes.
    changes.push(['begin', messages.begin(), {commitWatermark: '0a'}]);
    changes.push(['data', messages.delete('foo', {id: 'world'})]);
    changes.push(['rollback', messages.rollback()]);

    changes.push(['status', {}, {watermark: '0d'}]);

    // Verify that all changes were sent to the subscriber ...
    const downstream = drainToQueue(sub);
    expect(await nextChange(downstream)).toMatchObject({tag: 'status'});
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'hello'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      extra: 'stuff',
    });
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'delete',
      key: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({tag: 'rollback'});

    // One commit to ACK, then the status message
    await expectAcks('09', '0d');

    // Only the changes for the committed (i.e. first) transaction are persisted.
    expect(
      await sql`SELECT watermark, change->'tag' FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "09",
          "begin",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "commit",
        ],
      ]
    `);
    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'task-id',
          ownerAddress: 'change.streamer:12345',
          lastWatermark: '09',
        },
      ],
    });
  });

  test('subscriber ahead of change log', async () => {
    // Process some changes upstream.
    changes.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push([
      'commit',
      messages.commit({extra: 'stuff'}),
      {watermark: '09'},
    ]);

    // Subscribe to a watermark from "the future".
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid',
      mode: 'serving',
      watermark: '0b',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    // Process more upstream changes.
    changes.push(['begin', messages.begin(), {commitWatermark: '0b'}]);
    changes.push(['data', messages.delete('foo', {id: 'world'})]);
    changes.push([
      'commit',
      messages.commit({more: 'stuff'}),
      {watermark: '0b'},
    ]);

    // Finally something the subscriber hasn't seen.
    changes.push(['begin', messages.begin(), {commitWatermark: '0c'}]);
    changes.push(['data', messages.insert('foo', {id: 'voila'})]);
    changes.push([
      'commit',
      messages.commit({something: 'new'}),
      {watermark: '0c'},
    ]);

    // The subscriber should only see what's new to it.
    const downstream = drainToQueue(sub);
    expect(await nextChange(downstream)).toMatchObject({tag: 'status'});
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'voila'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      something: 'new',
    });

    await expectAcks('09', '0b', '0c');

    // Only the changes for the committed (i.e. first) transaction are persisted.
    expect(
      await sql`SELECT watermark, change->'tag' FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "09",
          "begin",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "commit",
        ],
        [
          "0b",
          "begin",
        ],
        [
          "0b",
          "delete",
        ],
        [
          "0b",
          "commit",
        ],
        [
          "0c",
          "begin",
        ],
        [
          "0c",
          "insert",
        ],
        [
          "0c",
          "commit",
        ],
      ]
    `);
    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'task-id',
          ownerAddress: 'change.streamer:12345',
          lastWatermark: '0c',
        },
      ],
    });
  });

  test('data types (forwarded and catchup)', async () => {
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });
    const downstream = drainToQueue(sub);

    changes.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes.push([
      'data',
      messages.insert('foo', {
        id: 'hello',
        int: 123456789,
        big: 987654321987654321n,
        flt: 123.456,
        bool: true,
      }),
    ]);
    changes.push([
      'commit',
      messages.commit({extra: 'info'}),
      {watermark: '09'},
    ]);

    expect(await nextChange(downstream)).toMatchObject({tag: 'status'});
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {
        id: 'hello',
        int: 123456789,
        big: 987654321987654321n,
        flt: 123.456,
        bool: true,
      },
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      extra: 'info',
    });

    await expectAcks('09');

    expect(
      await sql`SELECT watermark, change FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "09",
          {
            "tag": "begin",
          },
        ],
        [
          "09",
          {
            "new": {
              "big": 987654321987654321n,
              "bool": true,
              "flt": 123.456,
              "id": "hello",
              "int": 123456789,
            },
            "relation": {
              "name": "foo",
              "rowKey": {
                "columns": [
                  "id",
                ],
                "type": "default",
              },
              "schema": "public",
              "tag": "relation",
            },
            "tag": "insert",
          },
        ],
        [
          "09",
          {
            "extra": "info",
            "tag": "commit",
          },
        ],
      ]
    `);

    // Also verify when loading from the Store as opposed to direct forwarding.
    const catchupSub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid2',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });
    const catchup = drainToQueue(catchupSub);
    expect(await nextChange(catchup)).toMatchObject({tag: 'status'});
    expect(await nextChange(catchup)).toMatchObject({tag: 'begin'});
    expect(await nextChange(catchup)).toMatchObject({
      tag: 'insert',
      new: {
        id: 'hello',
        int: 123456789,
        big: 987654321987654321n,
        flt: 123.456,
        bool: true,
      },
    });
    expect(await nextChange(catchup)).toMatchObject({
      tag: 'commit',
      extra: 'info',
    });
    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'task-id',
          ownerAddress: 'change.streamer:12345',
          lastWatermark: '09',
        },
      ],
    });
  });

  test('immediate subscription status', async () => {
    // Initialize the change log with entries that will be purged.
    await sql`
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('04', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('04', 1, '{"tag":"commit"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('06', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('06', 1, '{"tag":"commit"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('08', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('08', 1, '{"tag":"commit"}'::json);
      UPDATE "zoro_3/cdc"."replicationState" SET "lastWatermark" = '08';
    `.simple();

    const sub04 = drainToQueue(
      await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'task-id',
        id: 'myid1',
        mode: 'serving',
        watermark: '04',
        replicaVersion: REPLICA_VERSION,
        initial: true,
      }),
    );
    expect(await nextChange(sub04)).toMatchObject({tag: 'status'});

    const sub08 = drainToQueue(
      await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'task-id',
        id: 'myid1',
        mode: 'serving',
        watermark: '08',
        replicaVersion: REPLICA_VERSION,
        initial: true,
      }),
    );
    expect(await nextChange(sub08)).toMatchObject({tag: 'status'});

    const sub02 = drainToQueue(
      await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'task-id',
        id: 'myid1',
        mode: 'serving',
        watermark: '02',
        replicaVersion: REPLICA_VERSION,
        initial: true,
      }),
    );
    expect(await sub02.dequeue()).toEqual([
      'error',
      {
        type: ErrorType.WatermarkTooOld,
        message: 'earliest supported watermark is 04 (requested 02)',
      },
    ]);
  });

  test('change log cleanup', async () => {
    // Initialize the change log with entries that will be purged.
    await sql`
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('03', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('04', 0, '{"tag":"commit"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('05', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('06', 0, '{"tag":"commit"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('07', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('08', 0, '{"tag":"commit"}'::json);
      UPDATE "zoro_3/cdc"."replicationState" SET "lastWatermark" = '08';
    `.simple();

    expect(await streamer.getChangeLogState()).toEqual({
      replicaVersion: '01',
      minWatermark: '03',
    });

    // Start two subscribers: one at 06 and one at 04
    const sub1 = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid1',
      mode: 'serving',
      watermark: '06',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    const sub2 = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid2',
      mode: 'serving',
      watermark: '04',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    expect(
      await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toEqual([['03'], ['04'], ['05'], ['06'], ['07'], ['08']]);

    expect(setTimeoutFn).toHaveBeenCalledTimes(1);
    expect(setTimeoutFn.mock.calls[0][1]).toBe(30000);

    // The first purge should have deleted records before '04'.
    await (setTimeoutFn.mock.calls[0][0]() as unknown as Promise<void>);
    expect(
      await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toEqual([['04'], ['05'], ['06'], ['07'], ['08']]);

    expect(setTimeoutFn).toHaveBeenCalledTimes(2);

    // The second purge should be a noop, because sub2 is still at '04'.
    await (setTimeoutFn.mock.calls[1][0]() as unknown as Promise<void>);
    expect(
      await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toEqual([['04'], ['05'], ['06'], ['07'], ['08']]);

    // And the timer should thus be rescheduled.
    expect(setTimeoutFn).toHaveBeenCalledTimes(3);

    drainToQueue(sub1);
    for await (const msg of sub2) {
      if (msg[0] === 'commit' && msg[2].watermark === '08') {
        // Now that sub2 has consumed past '06',
        // a purge should successfully clear records before '06'
        await (setTimeoutFn.mock.calls[2][0]() as unknown as Promise<void>);
        expect(
          await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`.values(),
        ).toEqual([['06'], ['07'], ['08']]);
        break;
      }
    }
    // replicationState is unaffected
    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'task-id',
          ownerAddress: 'change.streamer:12345',
          lastWatermark: '08',
        },
      ],
    });

    expect(await streamer.getChangeLogState()).toEqual({
      replicaVersion: '01',
      minWatermark: '06',
    });

    // No more timeouts should have been scheduled because both initialWatermarks
    // were cleaned up.
    expect(setTimeoutFn).toHaveBeenCalledTimes(3);

    // New connections earlier than 06 should now be rejected.
    const sub3 = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid2',
      mode: 'serving',
      watermark: '04',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    const msgs = drainToQueue(sub3);
    expect(await msgs.dequeue()).toEqual([
      'error',
      {
        type: ErrorType.WatermarkTooOld,
        message: 'earliest supported watermark is 06 (requested 04)',
      },
    ]);
  });

  test('wrong replica version', async () => {
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid1',
      mode: 'serving',
      watermark: '06',
      replicaVersion: REPLICA_VERSION + 'foobar',
      initial: true,
    });

    const msgs = drainToQueue(sub);
    expect(await msgs.dequeue()).toEqual([
      'error',
      {
        type: ErrorType.WrongReplicaVersion,
        message: 'current replica version is 01 (requested 01foobar)',
      },
    ]);
  });

  test('retry on initial stream failure', async () => {
    const {promise: hasRetried, resolve: retried} = resolver<true>();
    const source = {
      startStream: vi
        .fn()
        .mockRejectedValueOnce('error')
        .mockImplementation(() => {
          retried(true);
          return resolver().promise;
        }),
    };
	    const streamer = await initializeStreamer(
	      lc,
	      shard,
	      'task-id',
	      'change.streamer:12345',
	      'ws',
	      sql,
	      source,
	      replicaConfig,
	      true,
	      0.04,
	    );
    void streamer.run();

    expect(await hasRetried).toBe(true);
  });

  test('starting point', async () => {
    const requests = new Queue<string>();
    const source = {
      startStream: vi.fn().mockImplementation(req => {
        requests.enqueue(req);
        return resolver().promise;
      }),
    };
	    let streamer = await initializeStreamer(
	      lc,
	      shard,
	      'task-id',
	      'change.streamer:12345',
      'ws',
      sql,
      source,
	      replicaConfig,
	      true,
	      0.04,
	    );
    void streamer.run();

    expect(await requests.dequeue()).toBe(REPLICA_VERSION);

    await sql`
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('03', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('04', 0, '{"tag":"commit"}'::json);
      UPDATE "zoro_3/cdc"."replicationState" SET "lastWatermark" = '04';
    `.simple();

	    streamer = await initializeStreamer(
	      lc,
	      shard,
	      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      source,
	      replicaConfig,
	      true,
	      0.04,
	    );
    void streamer.run();

    expect(await requests.dequeue()).toBe('04');
  });

  test('retry on change stream error', async () => {
    const {promise: hasRetried, resolve: retried} = resolver<true>();
    const source = {
      startStream: vi
        .fn()
        .mockImplementationOnce(() =>
          Promise.resolve({
            initialWatermark: '01',
            changes,
            acks: () => {},
          }),
        )
        .mockImplementation(() => {
          retried(true);
          return resolver().promise;
        }),
    };
	    const streamer = await initializeStreamer(
	      lc,
	      shard,
	      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      source,
	      replicaConfig,
	      true,
	      0.04,
	    );
    void streamer.run();

    changes.fail(new Error('doh'));

    expect(await hasRetried).toBe(true);
  });

  test('retries at right watermark', async () => {
    const {promise: hasRetried, resolve: retried} = resolver<true>();
    const changes = Subscription.create<ChangeStreamMessage>();
    const source = {
      startStream: vi
        .fn()
        .mockImplementationOnce(() =>
          Promise.resolve({
            initialWatermark: '01',
            changes,
            acks: () => {},
          }),
        )
        .mockImplementation(() => {
          retried(true);
          return resolver().promise;
        }),
    };
	    const streamer = await initializeStreamer(
	      lc,
	      shard,
	      'task-id',
      'change.streamer:54321',
      'ws',
      sql,
      source,
	      replicaConfig,
	      true,
	      0.04,
	    );
    void streamer.run();

    // Stream down a big (1MB) transaction, which should take time to commit.
    const NEW_WATERMARK = '0g';
    const bigString = 'a'.repeat(1024);
    changes.push(['begin', {tag: 'begin'}, {commitWatermark: NEW_WATERMARK}]);
    let lastInsertProcessed: Promise<Result> | undefined;
    for (let i = 0; i < 1024; i++) {
      lastInsertProcessed = changes.push([
        'data',
        {
          tag: 'insert',
          new: {id: i, val: bigString},
          relation: {
            schema: 'public',
            name: 'foo',
            rowKey: {
              columns: ['id'],
            },
          },
        },
      ]).result;
    }
    changes.push(['commit', {tag: 'commit'}, {watermark: NEW_WATERMARK}]);

    // Wait for the last 'data' message to have been processed, which
    // means the commit was dequeued.
    await lastInsertProcessed;
    // Simulate closing the connection.
    changes.cancel();

    // Verify that the next stream starts at the NEW_WATERMARK, indicating
    // that the change-streamer waited for the last (big) commit before
    // determining the next watermark to start from.
    expect(await hasRetried).toBe(true);
    expect(source.startStream.mock.calls[1][0]).toBe(NEW_WATERMARK);
  });

  test('rolls back pending transaction when change-source dies', async () => {
    const changes1 = Subscription.create<ChangeStreamMessage>();
    const changes2 = Subscription.create<ChangeStreamMessage>();
    const source = {
      startStream: vi
        .fn()
        .mockImplementationOnce(() =>
          Promise.resolve({
            initialWatermark: '01',
            changes: changes1,
            acks: () => {},
          }),
        )
        .mockImplementationOnce(() =>
          Promise.resolve({
            initialWatermark: '01',
            changes: changes2,
            acks: () => {},
          }),
        ),
    };
	    const streamer = await initializeStreamer(
	      lc,
	      shard,
	      'task-id',
      'change.streamer:54321',
      'ws',
      sql,
      source,
	      replicaConfig,
	      true,
	      0.04,
	    );
    void streamer.run();

    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });
    const downstream = drainToQueue(sub);

    changes1.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes1.push(['data', messages.insert('foo', {id: 'hello'})]);

    expect(await nextChange(downstream)).toMatchObject({tag: 'status'});
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'hello'},
    });

    changes1.cancel(); // simulate a connection close or error.

    expect(await nextChange(downstream)).toMatchObject({tag: 'rollback'});

    changes2.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes2.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes2.push(['data', messages.insert('foo', {id: 'world'})]);
    changes2.push([
      'commit',
      messages.commit({extra: 'fields'}),
      {watermark: '09'},
    ]);

    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'hello'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      extra: 'fields',
    });

    await streamer.stop();
  });

  test('ownership takeover before tx begins', async () => {
    changes.push(['begin', {tag: 'begin'}, {commitWatermark: '0d'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['commit', {tag: 'commit'}, {watermark: '0d'}]);

    // Wait for the ack of the first commit.
    await expectAcks('0d');
    // Take over ownership.
    await sql`
      UPDATE "zoro_3/cdc"."replicationState" 
        SET "owner" = 'other-task', "ownerAddress" = 'change.streamer3:7645'`;

    // The begin will read the new owner and eventually fail the transaction.
    changes.push(['begin', {tag: 'begin'}, {commitWatermark: '0f'}]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push(['commit', {tag: 'commit'}, {watermark: '0f'}]);

    await streamerDone;

    // Only the first changes should be committed.
    expect(
      await sql`SELECT watermark, change->'tag' FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "0d",
          "begin",
        ],
        [
          "0d",
          "insert",
        ],
        [
          "0d",
          "commit",
        ],
      ]
    `);

    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'other-task',
          ownerAddress: 'change.streamer3:7645',
          lastWatermark: '0d',
        },
      ],
    });
  });

  test('ownership takeover during tx', async () => {
    changes.push(['begin', {tag: 'begin'}, {commitWatermark: '0d'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['commit', {tag: 'commit'}, {watermark: '0d'}]);

    changes.push(['begin', {tag: 'begin'}, {commitWatermark: '0f'}]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);

    // Wait for the ack of the first commit.
    await expectAcks('0d');

    // Let the next transaction begin, reading the old owner.
    await sleep(10);

    // Take over ownership.
    await sql`
      UPDATE "zoro_3/cdc"."replicationState" 
        SET "owner" = 'other-task', "ownerAddress" = 'change.streamer2:9876'`;
    // The commit should fail (with a SERIALIZATION error).
    changes.push(['commit', {tag: 'commit'}, {watermark: '0f'}]);

    await streamerDone;

    // Only the first changes should be committed.
    expect(
      await sql`SELECT watermark, change->'tag' FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "0d",
          "begin",
        ],
        [
          "0d",
          "insert",
        ],
        [
          "0d",
          "commit",
        ],
      ]
    `);

    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'other-task',
          ownerAddress: 'change.streamer2:9876',
          lastWatermark: '0d',
        },
      ],
    });
  });

  test('reset required', async () => {
    changes.push(['control', {tag: 'reset-required'}]);
    await streamerDone;
    await expect(
      ensureReplicationConfig(lc, sql, replicaConfig, shard, true),
    ).rejects.toThrow(AutoResetSignal);
  });

  test('reset required if backup is behind', async () => {
    await sql`
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('03', 0, '{"tag":"begin"}'::json);
    `;

    void streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'backup-id',
      mode: 'backup',
      watermark: '02', // Too early
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    await streamerDone;
    await expect(
      ensureReplicationConfig(lc, sql, replicaConfig, shard, true),
    ).rejects.toThrow(AutoResetSignal);
  });

  test('shutdown on AbortError', async () => {
    changes.fail(new AbortError());
    await streamerDone;
  });

  test('shutdown on unexpected invalid stream', async () => {
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);

    // Streamer should be shut down because of the error.
    await streamerDone;

    // Nothing should be committed
    expect(await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`).toEqual(
      [],
    );
  });

  test('shutdown on unexpected storage error', async () => {
    // Insert unexpected data simulating that the stream and store are not in the expected state.
    await sql`INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change)
      VALUES ('05', 3, ${{conflicting: 'entry'}})`;

    changes.push(['begin', messages.begin(), {commitWatermark: '05'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push(['commit', messages.commit(), {watermark: '05'}]);

    // Streamer should be shut down because of the error.
    await streamerDone;

    // Commit should not have succeeded
    expect(
      await sql`SELECT watermark, pos FROM "zoro_3/cdc"."changeLog"`,
    ).toEqual([{watermark: '05', pos: 3n}]);
  });

  test('transaction aborted on unexpected termination', async () => {
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid1',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    const msgs = drainToQueue(sub);

    changes.push(['begin', messages.begin(), {commitWatermark: '05'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.end();

    expect(await nextChange(msgs)).toMatchObject({tag: 'status'});
    expect(await nextChange(msgs)).toMatchObject({tag: 'begin'});
    expect(await nextChange(msgs)).toMatchObject({tag: 'insert'});
    expect(await nextChange(msgs)).toMatchObject({tag: 'rollback'});

    // No more messages should have been sent
    // No more messages should have been sent
    await verifyNoMoreChanges(msgs);
  });

  test('transaction aborted only once', async () => {
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid1',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    const msgs = drainToQueue(sub);

    changes.push(['begin', messages.begin(), {commitWatermark: '05'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    // An explicit abort from change-source
    changes.push(['rollback', {tag: 'rollback'}]);
    changes.end();

    expect(await nextChange(msgs)).toMatchObject({tag: 'status'});
    expect(await nextChange(msgs)).toMatchObject({tag: 'begin'});
    expect(await nextChange(msgs)).toMatchObject({tag: 'insert'});
    expect(await nextChange(msgs)).toMatchObject({tag: 'rollback'});

    // No more messages should have been sent
    await verifyNoMoreChanges(msgs);
  });
});
