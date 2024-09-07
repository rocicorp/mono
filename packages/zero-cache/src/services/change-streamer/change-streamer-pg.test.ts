import {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {Queue} from 'shared/src/queue.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {
  dropReplicationSlot,
  getConnectionURI,
  testDBs,
} from 'zero-cache/src/test/db.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {CancelableAsyncIterable} from 'zero-cache/src/types/streams.js';
import {Database} from 'zqlite/src/db.js';
import {initialSync, replicationSlot} from '../replicator/initial-sync.js';
import {getSubscriptionState} from '../replicator/schema/replication-state.js';
import {initializeStreamer} from './change-streamer-pg.js';
import {ChangeStreamerService, Downstream} from './change-streamer.js';
import {ChangeLogEntry} from './schema/tables.js';

const REPLICA_ID = 'change_streamer_test_id';

describe('change-streamer/service', {retry: 3}, () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let changeDB: PostgresDB;
  let replica: Database;
  let streamer: ChangeStreamerService;

  beforeEach(async () => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('change_streamer_test_upstream');
    changeDB = await testDBs.create('change_streamer_test_change_db');
    replica = new Database(lc, ':memory:');

    const upstreamURI = getConnectionURI(upstream);
    await upstream`CREATE TABLE foo(id TEXT PRIMARY KEY)`;

    await initialSync(lc, REPLICA_ID, replica, upstreamURI);

    streamer = await initializeStreamer(
      lc,
      changeDB,
      upstreamURI,
      REPLICA_ID,
      replica,
    );
    void streamer.run();
  });

  afterEach(async () => {
    await streamer.stop();
    await dropReplicationSlot(upstream, replicationSlot(REPLICA_ID));
    await testDBs.drop(upstream, changeDB);
  });

  function drainToQueue(
    sub: CancelableAsyncIterable<Downstream>,
  ): Queue<Downstream> {
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

  test('immediate forwarding, transaction storage', async () => {
    const {replicaVersion, watermark} = getSubscriptionState(
      new StatementRunner(replica),
    );
    const sub = streamer.subscribe({
      id: 'myid',
      watermark,
      replicaVersion,
      initial: true,
    });
    const downstream = drainToQueue(sub);

    await upstream.begin(async tx => {
      await tx`INSERT INTO foo(id) VALUES('hello')`;
      await tx`INSERT INTO foo(id) VALUES('world')`;
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
    // Capture watermark before any changes.
    const {replicaVersion, watermark} = getSubscriptionState(
      new StatementRunner(replica),
    );

    // Write some changes upstream.
    await upstream.begin(async tx => {
      await tx`INSERT INTO foo(id) VALUES('hello')`;
      await tx`INSERT INTO foo(id) VALUES('world')`;
    });

    // Subscribe to the original watermark.
    const sub = streamer.subscribe({
      id: 'myid',
      watermark,
      replicaVersion,
      initial: true,
    });

    // Write more upstream changes.
    await upstream`DELETE FROM foo WHERE id = 'world'`;

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

    // and stored to the DB.
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
});
