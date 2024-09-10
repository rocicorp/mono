import {LogContext} from '@rocicorp/logger';
import {Pgoutput} from 'pg-logical-replication';
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
  ChangeEntry,
  ChangeStreamerService,
  Downstream,
} from './change-streamer.js';
import {ChangeLogEntry} from './schema/tables.js';

describe('change-streamer/service', {retry: 3}, () => {
  let lc: LogContext;
  let changeDB: PostgresDB;
  let streamer: ChangeStreamerService;
  let changes: Subscription<ChangeEntry>;
  let acks: Queue<Pgoutput.MessageCommit>;

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
        startStream: () => ({
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
    assert(down[0] === 'change');
    return down[1].change;
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

    changes.push({watermark: '02', change: messages.begin()});
    changes.push({
      watermark: '02',
      change: messages.insert('foo', {id: 'hello'}),
    });
    changes.push({
      watermark: '03',
      change: messages.insert('foo', {id: 'world'}),
    });
    changes.push({
      watermark: '04',
      change: messages.commit('04'),
    });

    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'hello'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({tag: 'commit'});

    // Await the ACK for the single commit.
    await acks.dequeue();

    const logEntries = await changeDB<
      ChangeLogEntry[]
    >`SELECT * FROM cdc."ChangeLog"`;
    expect(logEntries.map(e => [e.pos, e.change.tag])).toEqual([
      [0n, 'begin'],
      [1n, 'insert'],
      [2n, 'insert'],
      [3n, 'commit'],
    ]);
  });

  test('subscriber catchup and continuation', async () => {
    // Process some changes upstream.
    changes.push({watermark: '02', change: messages.begin()});
    changes.push({
      watermark: '02',
      change: messages.insert('foo', {id: 'hello'}),
    });
    changes.push({
      watermark: '03',
      change: messages.insert('foo', {id: 'world'}),
    });
    changes.push({
      watermark: '04',
      change: messages.commit('04'),
    });

    // Subscribe to the original watermark.
    const sub = streamer.subscribe({
      id: 'myid',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
    });

    // Process more upstream changes.
    changes.push({watermark: '05', change: messages.begin()});
    changes.push({
      watermark: '05',
      change: messages.delete('foo', {id: 'world'}),
    });
    changes.push({
      watermark: '06',
      change: messages.commit('06'),
    });

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
    expect(await nextChange(downstream)).toMatchObject({tag: 'commit'});
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'delete',
      key: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({tag: 'commit'});

    // Two commits
    await acks.dequeue();
    await acks.dequeue();

    const logEntries = await changeDB<
      ChangeLogEntry[]
    >`SELECT * FROM cdc."ChangeLog"`;
    expect(logEntries.map(e => [e.pos, e.change.tag])).toEqual([
      [0n, 'begin'],
      [1n, 'insert'],
      [2n, 'insert'],
      [3n, 'commit'],
      [0n, 'begin'],
      [1n, 'delete'],
      [2n, 'commit'],
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

    changes.push({watermark: '02', change: messages.begin()});
    changes.push({
      watermark: '02',
      change: messages.insert('foo', {
        id: 'hello',
        int: 123456789,
        big: 987654321987654321n,
        flt: 123.456,
        bool: true,
      }),
    });
    changes.push({watermark: '03', change: messages.commit('03')});

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
    expect(await nextChange(downstream)).toMatchObject({tag: 'commit'});

    await acks.dequeue();

    const logEntries = await changeDB<
      ChangeLogEntry[]
    >`SELECT * FROM cdc."ChangeLog"`;
    expect(logEntries.map(e => [e.pos, e.change.tag])).toEqual([
      [0n, 'begin'],
      [1n, 'insert'],
      [2n, 'commit'],
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
    expect(await nextChange(catchup)).toMatchObject({tag: 'commit'});
  });
});
