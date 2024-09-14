import {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {Queue} from 'shared/src/queue.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from 'zero-cache/src/test/db.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {Source} from 'zero-cache/src/types/streams.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {Database} from 'zqlite/src/db.js';
import {initReplicationState} from '../replicator/schema/replication-state.js';
import {ReplicationMessages} from '../replicator/test-utils.js';
import {initializeStreamer} from './change-streamer-service.js';
import {
  ChangeStreamerService,
  Commit,
  Downstream,
  DownstreamChange,
} from './change-streamer.js';
import {ChangeLogEntry} from './schema/tables.js';

describe('change-streamer/service', {retry: 3}, () => {
  let lc: LogContext;
  let changeDB: PostgresDB;
  let streamer: ChangeStreamerService;
  let changes: Subscription<DownstreamChange>;
  let acks: Queue<Commit>;

  const REPLICA_VERSION = '01';

  beforeEach(async () => {
    lc = createSilentLogContext();

    changeDB = await testDBs.create('change_streamer_test_change_db');

    const replica = new Database(lc, ':memory:');
    initReplicationState(replica, ['zero_data'], REPLICA_VERSION);

    changes = Subscription.create();
    acks = new Queue();

    streamer = await initializeStreamer(
      lc,
      changeDB,
      {
        startStream: () =>
          Promise.resolve({
            initialWatermark: '01',
            changes,
            acks: {push: commit => acks.enqueue(commit)},
          }),
      },
      replica,
    );
    void streamer.run();
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
    const sub = streamer.subscribe({
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
      {watermark: '05'},
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
    >`SELECT * FROM cdc."ChangeLog"`;
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
      {watermark: '05'},
    ]);

    // Subscribe to the original watermark.
    const sub = streamer.subscribe({
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
      {watermark: '08'},
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
    >`SELECT * FROM cdc."ChangeLog"`;
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

  test('data types (forwarded and catchup)', async () => {
    const sub = streamer.subscribe({
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
      {watermark: '04'},
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
    >`SELECT * FROM cdc."ChangeLog"`;
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
    const catchupSub = streamer.subscribe({
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
});
