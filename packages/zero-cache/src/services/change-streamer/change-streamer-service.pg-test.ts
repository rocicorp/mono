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
import {AbortError} from '../../../../shared/src/abort-error.js';
import {assert} from '../../../../shared/src/asserts.js';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import {Queue} from '../../../../shared/src/queue.js';
import {Database} from '../../../../zqlite/src/db.js';
import {StatementRunner} from '../../db/statements.js';
import {testDBs} from '../../test/db.js';
import type {PostgresDB} from '../../types/pg.js';
import type {Source} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import {
  getSubscriptionState,
  initReplicationState,
} from '../replicator/schema/replication-state.js';
import {ReplicationMessages} from '../replicator/test-utils.js';
import {initializeStreamer} from './change-streamer-service.js';
import {
  ErrorType,
  type ChangeStreamerService,
  type Commit,
  type Downstream,
  type DownstreamChange,
} from './change-streamer.js';
import type {ChangeLogEntry} from './schema/tables.js';

describe('change-streamer/service', () => {
  let lc: LogContext;
  let changeDB: PostgresDB;
  let streamer: ChangeStreamerService;
  let changes: Subscription<DownstreamChange>;
  let acks: Queue<Commit>;
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
            acks: {push: commit => acks.enqueue(commit)},
          }),
      },
      getSubscriptionState(new StatementRunner(replica)),
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
    assert(down[0] !== 'error');
    return down[1];
  }

  const messages = new ReplicationMessages({foo: 'id'});

  test('immediate forwarding, transaction storage', async () => {
    const sub = await streamer.subscribe({
      id: 'myid',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });
    const downstream = drainToQueue(sub);

    changes.push(['begin', messages.begin()]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push([
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

    // Await the ACK for the single commit.
    await acks.dequeue();

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
    changes.push(['begin', messages.begin()]);
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
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    // Process more upstream changes.
    changes.push(['begin', messages.begin()]);
    changes.push(['data', messages.delete('foo', {id: 'world'})]);
    changes.push([
      'commit',
      messages.commit({more: 'stuff'}),
      {watermark: '0b'},
    ]);

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

    // Two commits
    await acks.dequeue();
    await acks.dequeue();

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
    changes.push(['begin', messages.begin()]);
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
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    // Process more upstream changes.
    changes.push(['begin', messages.begin()]);
    changes.push(['data', messages.delete('foo', {id: 'world'})]);
    changes.push(['rollback', messages.rollback()]);

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

    // One commit to ACK
    await acks.dequeue();

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

  test('data types (forwarded and catchup)', async () => {
    const sub = await streamer.subscribe({
      id: 'myid',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });
    const downstream = drainToQueue(sub);

    changes.push(['begin', messages.begin()]);
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

    await acks.dequeue();

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
      watermark: '06',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    const sub2 = await streamer.subscribe({
      id: 'myid2',
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
    const replica = new Database(lc, ':memory:');
    initReplicationState(replica, ['zero_data'], REPLICA_VERSION);

    const streamer = await initializeStreamer(
      lc,
      changeDB,
      source,
      getSubscriptionState(new StatementRunner(replica)),
    );
    void streamer.run();

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
    const replica = new Database(lc, ':memory:');
    initReplicationState(replica, ['zero_data'], REPLICA_VERSION);
    const config = getSubscriptionState(new StatementRunner(replica));

    let streamer = await initializeStreamer(lc, changeDB, source, config);
    void streamer.run();

    expect(await requests.dequeue()).toBe(REPLICA_VERSION);

    await changeDB`
      INSERT INTO cdc."changeLog" (watermark, pos, change) VALUES ('03', 0, '{"tag":"begin"}'::json);
      INSERT INTO cdc."changeLog" (watermark, pos, change) VALUES ('04', 0, '{"tag":"commit"}'::json);
    `.simple();

    streamer = await initializeStreamer(lc, changeDB, source, config);
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
    const replica = new Database(lc, ':memory:');
    initReplicationState(replica, ['zero_data'], REPLICA_VERSION);

    const streamer = await initializeStreamer(
      lc,
      changeDB,
      source,
      getSubscriptionState(new StatementRunner(replica)),
    );
    void streamer.run();

    changes.fail(new Error('doh'));

    expect(await hasRetried).toBe(true);
  });

  test('shutdown on AbortError', async () => {
    changes.fail(new AbortError());
    await streamerDone;
  });

  test('shutdown on unexpected storage error', async () => {
    await streamer.subscribe({
      id: 'myid',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    // Insert unexpected data simulating that the stream and store are not in the expected state.
    await changeDB`INSERT INTO cdc."changeLog" (watermark, pos, change)
      VALUES ('03', 0, ${{intervening: 'entry'}})`;

    changes.push(['begin', messages.begin()]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push(['commit', messages.commit(), {watermark: '05'}]);

    // Commit should not have succeeded
    expect(await changeDB`SELECT watermark FROM cdc."changeLog"`).toEqual([
      {watermark: '03'},
    ]);

    // Streamer should be shut down because of the error.
    await streamerDone;
  });
});
