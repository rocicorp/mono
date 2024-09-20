import {LogContext} from '@rocicorp/logger';
import {AbortError} from 'shared/src/abort-error.js';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {Queue} from 'shared/src/queue.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {
  dropReplicationSlot,
  getConnectionURI,
  testDBs,
} from 'zero-cache/src/test/db.js';
import {DbFile} from 'zero-cache/src/test/lite.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {Source} from 'zero-cache/src/types/streams.js';
import {getSubscriptionState} from '../../replicator/schema/replication-state.js';
import {ChangeSource} from '../change-streamer-service.js';
import {DownstreamChange} from '../change-streamer.js';
import {initializeChangeSource} from './change-source.js';
import {replicationSlot} from './initial-sync.js';

const REPLICA_ID = 'change_source_test_id';

describe('change-source/pg', () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let replicaDbFile: DbFile;
  let source: ChangeSource;

  beforeEach(async () => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('change_source_pg_test_upstream');
    replicaDbFile = new DbFile('change_source_pg_test_replica');

    const upstreamURI = getConnectionURI(upstream);
    await upstream.unsafe(`
    CREATE TABLE foo(
      id TEXT PRIMARY KEY,
      int INT4,
      big BIGINT,
      flt FLOAT8,
      bool BOOLEAN
    );
    CREATE PUBLICATION zero_all FOR TABLE foo WHERE (id != 'exclude-me');
    `);

    source = await initializeChangeSource(
      lc,
      upstreamURI,
      REPLICA_ID,
      replicaDbFile.path,
    );
  });

  afterEach(async () => {
    await dropReplicationSlot(upstream, replicationSlot(REPLICA_ID));
    await testDBs.drop(upstream);
    await replicaDbFile.unlink();
  });

  function drainToQueue(
    sub: Source<DownstreamChange>,
  ): Queue<DownstreamChange> {
    const queue = new Queue<DownstreamChange>();
    void (async () => {
      for await (const msg of sub) {
        void queue.enqueue(msg);
      }
    })();
    return queue;
  }

  const WATERMARK_REGEX = /[0-9a-z]{2,}/;

  test('changes', async () => {
    const {replicaVersion} = getSubscriptionState(
      new StatementRunner(replicaDbFile.connect(lc)),
    );

    const {initialWatermark, changes} = await source.startStream();
    const downstream = drainToQueue(changes);

    await upstream.begin(async tx => {
      await tx`INSERT INTO foo(id) VALUES('hello')`;
      await tx`INSERT INTO foo(id) VALUES('world')`;
      await tx`
      INSERT INTO foo(id, int, big, flt, bool) 
        VALUES('datatypes', 123456789, 987654321987654321, 123.456, true)`;
    });

    expect(initialWatermark).toEqual(replicaVersion);
    expect(await downstream.dequeue()).toMatchObject(['begin', {tag: 'begin'}]);
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
        },
      },
    ]);
    expect(await downstream.dequeue()).toMatchObject([
      'commit',
      {tag: 'commit'},
      {watermark: expect.stringMatching(WATERMARK_REGEX)},
    ]);

    // Write more upstream changes.
    await upstream.begin(async tx => {
      await tx`DELETE FROM foo WHERE id = 'world'`;
      await tx`UPDATE foo SET int = 123 WHERE id = 'hello';`;
      await tx`TRUNCATE foo`;
      await tx`INSERT INTO foo(id) VALUES ('exclude-me')`;
      await tx`INSERT INTO foo(id) VALUES ('include-me')`;
    });

    expect(await downstream.dequeue()).toMatchObject(['begin', {tag: 'begin'}]);
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
    expect(await downstream.dequeue()).toMatchObject([
      'commit',
      {tag: 'commit'},
      {watermark: expect.stringMatching(WATERMARK_REGEX)},
    ]);

    // Close the stream.
    changes.cancel();
  });

  test('error handling', async () => {
    // Purposely drop the replication slot to test the error case.
    await dropReplicationSlot(upstream, replicationSlot(REPLICA_ID));

    let err;
    try {
      await source.startStream();
    } catch (e) {
      err = e;
    }
    expect(err).not.toBeUndefined();
  });

  test('abort', async () => {
    const {changes} = await source.startStream();

    const results = await upstream<{pid: number}[]>`
      SELECT active_pid as pid from pg_replication_slots WHERE
        slot_name = ${replicationSlot(REPLICA_ID)}`;
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
    const {changes} = await source.startStream();

    // Starting another stream should stop the first.
    const {changes: changes2} = await source.startStream();

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
});
