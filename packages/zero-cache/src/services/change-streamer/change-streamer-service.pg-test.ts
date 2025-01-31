import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
  vi,
  type Mock,
} from 'vitest';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {testDBs} from '../../test/db.ts';
import {stringify} from '../../types/bigint-json.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {type ChangeStreamMessage} from '../change-source/protocol/current/downstream.ts';
import type {StatusMessage} from '../change-source/protocol/current/status.ts';
import {
  getSubscriptionState,
  initReplicationState,
} from '../replicator/schema/replication-state.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {initializeStreamer} from './change-streamer-service.ts';
import {
  type ChangeStreamerService,
  type Downstream,
} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';
import {
  AutoResetSignal,
  ensureReplicationConfig,
  type ChangeLogEntry,
  type ReplicationConfig,
} from './schema/tables.ts';

describe('change-streamer/service', () => {
  let lc: LogContext;
  let replicaConfig: ReplicationConfig;
  let changeDB: PostgresDB;
  let streamer: ChangeStreamerService;
  let changes: Subscription<ChangeStreamMessage>;
  let acks: Queue<StatusMessage>;
  let streamerDone: Promise<void>;

  // vi.useFakeTimers() does not play well with the postgres client.
  // Inject a manual mock instead.
  let setTimeoutFn: Mock<typeof setTimeout>;

  const REPLICA_VERSION = '01';

  beforeEach(async () => {
    lc = createSilentLogContext();

    changeDB = await testDBs.create('change_streamer_test_change_db');

    const replica = new Database(lc, ':memory:');
    initReplicationState(replica, ['zero_data'], REPLICA_VERSION);
    replicaConfig = getSubscriptionState(new StatementRunner(replica));

    changes = Subscription.create();
    acks = new Queue();
    setTimeoutFn = vi.fn();

    streamer = await initializeStreamer(
      lc,
      changeDB,
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
      setTimeoutFn as unknown as typeof setTimeout,
    );
    streamerDone = streamer.run();
  });

  afterEach(async () => {
    await streamer.stop();
    await testDBs.drop(changeDB);
  });

  function drainToQueue(sub: Source<Downstream>): Queue<Downstream> {
    const queue = new Queue<Downstream>();
    void (async () => {
      for await (const msg of sub) {
        void queue.enqueue(msg);
      }
    })();
    return queue;
  }

  async function nextChange(sub: Queue<Downstream>) {
    const down = await sub.dequeue();
    assert(down[0] !== 'error', `Unexpected error ${stringify(down)}`);
    return down[1];
  }

  async function expectAcks(...watermarks: string[]) {
    for (const watermark of watermarks) {
      expect((await acks.dequeue())[2].watermark).toBe(watermark);
    }
  }

  const messages = new ReplicationMessages({foo: 'id'});

  test('immediate forwarding, transaction storage', async () => {
    const sub = await streamer.subscribe({
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

    const logEntries = await changeDB<
      ChangeLogEntry[]
    >`SELECT * FROM cdc."changeLog"`;
    expect(logEntries.map(e => e.change.tag)).toEqual([
      'begin',
      'insert',
      'insert',
      'commit',
    ]);
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

    const logEntries = await changeDB<
      ChangeLogEntry[]
    >`SELECT * FROM cdc."changeLog"`;
    expect(logEntries.map(e => e.change.tag)).toEqual([
      'begin',
      'insert',
      'insert',
      'commit',
      'begin',
      'delete',
      'commit',
    ]);
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
    const logEntries = await changeDB<
      ChangeLogEntry[]
    >`SELECT * FROM cdc."changeLog"`;
    expect(logEntries.map(e => e.change.tag)).toEqual([
      'begin',
      'insert',
      'insert',
      'commit',
    ]);
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
    const logEntries = await changeDB<
      ChangeLogEntry[]
    >`SELECT * FROM cdc."changeLog"`;
    expect(logEntries.map(e => e.change.tag)).toEqual([
      'begin',
      'insert',
      'insert',
      'commit',
      'begin',
      'delete',
      'commit',
      'begin',
      'insert',
      'commit',
    ]);
  });

  test('data types (forwarded and catchup)', async () => {
    const sub = await streamer.subscribe({
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

    const logEntries = await changeDB<
      ChangeLogEntry[]
    >`SELECT * FROM cdc."changeLog"`;
    expect(logEntries.map(e => e.change.tag)).toEqual([
      'begin',
      'insert',
      'commit',
    ]);
    const insert = logEntries[1].change;
    assert(insert.tag === 'insert');
    expect(insert.new).toEqual({
      id: 'hello',
      int: 123456789,
      big: 987654321987654321n,
      flt: 123.456,
      bool: true,
    });

    // Also verify when loading from the Store as opposed to direct forwarding.
    const catchupSub = await streamer.subscribe({
      id: 'myid2',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });
    const catchup = drainToQueue(catchupSub);
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
  });

  test('change log cleanup', async () => {
    // Initialize the change log with entries that will be purged.
    await changeDB`
      INSERT INTO cdc."changeLog" (watermark, pos, change) VALUES ('03', 0, '{"tag":"begin"}'::json);
      INSERT INTO cdc."changeLog" (watermark, pos, change) VALUES ('04', 0, '{"tag":"commit"}'::json);
      INSERT INTO cdc."changeLog" (watermark, pos, change) VALUES ('05', 0, '{"tag":"begin"}'::json);
      INSERT INTO cdc."changeLog" (watermark, pos, change) VALUES ('06', 0, '{"tag":"commit"}'::json);
      INSERT INTO cdc."changeLog" (watermark, pos, change) VALUES ('07', 0, '{"tag":"begin"}'::json);
      INSERT INTO cdc."changeLog" (watermark, pos, change) VALUES ('08', 0, '{"tag":"commit"}'::json);
    `.simple();

    // Start two subscribers: one at 06 and one at 04
    await streamer.subscribe({
      id: 'myid1',
      mode: 'serving',
      watermark: '06',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    const sub2 = await streamer.subscribe({
      id: 'myid2',
      mode: 'serving',
      watermark: '04',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    expect(
      await changeDB`SELECT watermark FROM cdc."changeLog"`.values(),
    ).toEqual([['03'], ['04'], ['05'], ['06'], ['07'], ['08']]);

    expect(setTimeoutFn).toHaveBeenCalledTimes(1);
    expect(setTimeoutFn.mock.calls[0][1]).toBe(30000);

    // The first purge should have deleted records before '04'.
    await (setTimeoutFn.mock.calls[0][0]() as unknown as Promise<void>);
    expect(
      await changeDB`SELECT watermark FROM cdc."changeLog"`.values(),
    ).toEqual([['04'], ['05'], ['06'], ['07'], ['08']]);

    expect(setTimeoutFn).toHaveBeenCalledTimes(2);

    // The second purge should be a noop, because sub2 is still at '04'.
    await (setTimeoutFn.mock.calls[1][0]() as unknown as Promise<void>);
    expect(
      await changeDB`SELECT watermark FROM cdc."changeLog"`.values(),
    ).toEqual([['04'], ['05'], ['06'], ['07'], ['08']]);

    // And the timer should thus be rescheduled.
    expect(setTimeoutFn).toHaveBeenCalledTimes(3);

    for await (const msg of sub2) {
      if (msg[0] === 'commit' && msg[2].watermark === '08') {
        // Now that sub2 has consumed past '06',
        // a purge should successfully clear records before '06'
        await (setTimeoutFn.mock.calls[2][0]() as unknown as Promise<void>);
        expect(
          await changeDB`SELECT watermark FROM cdc."changeLog"`.values(),
        ).toEqual([['06'], ['07'], ['08']]);
        break;
      }
    }

    // No more timeouts should have been scheduled because both initialWatermarks
    // were cleaned up.
    expect(setTimeoutFn).toHaveBeenCalledTimes(3);

    // New connections earlier than 06 should now be rejected.
    const sub3 = await streamer.subscribe({
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
      changeDB,
      source,
      replicaConfig,
      true,
    );
    void streamer.run();

    // Kick off the initial stream with a serving request.
    void streamer.subscribe({
      id: 'myid',
      mode: 'serving',
      watermark: '06',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    expect(await hasRetried).toBe(true);
  });

  test('starting point', async () => {
    const requests = new Queue<string>();
    const source = {
      startStream: vi.fn().mockImplementation(req => {
        void requests.enqueue(req);
        return resolver().promise;
      }),
    };
    let streamer = await initializeStreamer(
      lc,
      changeDB,
      source,
      replicaConfig,
      true,
    );
    void streamer.run();

    // Kick off the initial stream with a serving request.
    void streamer.subscribe({
      id: 'myid',
      mode: 'serving',
      watermark: '06',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    expect(await requests.dequeue()).toBe(REPLICA_VERSION);

    await changeDB`
      INSERT INTO cdc."changeLog" (watermark, pos, change) VALUES ('03', 0, '{"tag":"begin"}'::json);
      INSERT INTO cdc."changeLog" (watermark, pos, change) VALUES ('04', 0, '{"tag":"commit"}'::json);
    `.simple();

    streamer = await initializeStreamer(
      lc,
      changeDB,
      source,
      replicaConfig,
      true,
    );
    void streamer.run();

    // Kick off the initial stream with a serving request.
    void streamer.subscribe({
      id: 'myid',
      mode: 'serving',
      watermark: '06',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

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
      changeDB,
      source,
      replicaConfig,
      true,
    );
    void streamer.run();

    // Kick off the initial stream with a serving request.
    void streamer.subscribe({
      id: 'myid',
      mode: 'serving',
      watermark: '06',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    changes.fail(new Error('doh'));

    expect(await hasRetried).toBe(true);
  });

  test('reset required', async () => {
    // Kick off the initial stream with a serving request.
    void streamer.subscribe({
      id: 'myid',
      mode: 'serving',
      watermark: '06',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    changes.push(['control', {tag: 'reset-required'}]);
    await streamerDone;
    await expect(
      ensureReplicationConfig(lc, changeDB, replicaConfig, true),
    ).rejects.toThrow(AutoResetSignal);
  });

  test('shutdown on AbortError', async () => {
    // Kick off the initial stream with a serving request.
    void streamer.subscribe({
      id: 'myid',
      mode: 'serving',
      watermark: '06',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    changes.fail(new AbortError());
    await streamerDone;
  });

  test('shutdown on unexpected invalid stream', async () => {
    await streamer.subscribe({
      id: 'myid',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    changes.push(['data', messages.insert('foo', {id: 'hello'})]);

    // Streamer should be shut down because of the error.
    await streamerDone;

    // Nothing should be committed
    expect(await changeDB`SELECT watermark FROM cdc."changeLog"`).toEqual([]);
  });

  test('shutdown on unexpected storage error', async () => {
    await streamer.subscribe({
      id: 'myid',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    // Insert unexpected data simulating that the stream and store are not in the expected state.
    await changeDB`INSERT INTO cdc."changeLog" (watermark, pos, change)
      VALUES ('05', 3, ${{conflicting: 'entry'}})`;

    changes.push(['begin', messages.begin(), {commitWatermark: '05'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push(['commit', messages.commit(), {watermark: '05'}]);

    // Streamer should be shut down because of the error.
    await streamerDone;

    // Commit should not have succeeded
    expect(await changeDB`SELECT watermark, pos FROM cdc."changeLog"`).toEqual([
      {watermark: '05', pos: 3n},
    ]);
  });
});
