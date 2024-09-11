import {LogContext} from '@rocicorp/logger';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {Queue} from 'shared/src/queue.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {
  dropReplicationSlot,
  getConnectionURI,
  testDBs,
} from 'zero-cache/src/test/db.js';
import {DbFile} from 'zero-cache/src/test/lite.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {Source} from 'zero-cache/src/types/streams.js';
import {ChangeSource} from '../change-streamer-service.js';
import {ChangeEntry} from '../change-streamer.js';
import {initializeChangeSource} from './change-source.js';
import {replicationSlot} from './initial-sync.js';

const REPLICA_ID = 'change_streamer_test_id';

describe('change-source/pg', {retry: 3}, () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let replicaDbFile: DbFile;
  let source: ChangeSource;
  let watermarks: string[];

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
    watermarks = [];
  });

  afterEach(async () => {
    await dropReplicationSlot(upstream, replicationSlot(REPLICA_ID));
    await testDBs.drop(upstream);
    await replicaDbFile.unlink();
  });

  function drainToQueue(sub: Source<ChangeEntry>): Queue<ChangeEntry> {
    const queue = new Queue<ChangeEntry>();
    void (async () => {
      for await (const msg of sub) {
        void queue.enqueue(msg);
      }
    })();
    return queue;
  }

  async function nextChange(sub: Queue<ChangeEntry>) {
    const entry = await sub.dequeue();
    watermarks.push(entry.watermark);
    return entry.change;
  }

  test('changes', async () => {
    const {changes} = source.startStream();
    const downstream = drainToQueue(changes);

    await upstream.begin(async tx => {
      await tx`INSERT INTO foo(id) VALUES('hello')`;
      await tx`INSERT INTO foo(id) VALUES('world')`;
      await tx`
      INSERT INTO foo(id, int, big, flt, bool) 
        VALUES('datatypes', 123456789, 987654321987654321, 123.456, true)`;
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
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {
        id: 'datatypes',
        int: 123456789,
        big: 987654321987654321n,
        flt: 123.456,
        bool: true,
      },
    });
    expect(await nextChange(downstream)).toMatchObject({tag: 'commit'});

    // Write more upstream changes.
    await upstream.begin(async tx => {
      await tx`DELETE FROM foo WHERE id = 'world'`;
      await tx`UPDATE foo SET int = 123 WHERE id = 'hello';`;
      await tx`TRUNCATE foo`;
      await tx`INSERT INTO foo(id) VALUES ('exclude-me')`;
      await tx`INSERT INTO foo(id) VALUES ('include-me')`;
    });

    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'delete',
      key: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'update',
      new: {id: 'hello', int: 123},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'truncate',
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'include-me'},
    });
    expect(await nextChange(downstream)).toMatchObject({tag: 'commit'});

    // Close the stream.
    changes.cancel();

    expect(watermarks).toHaveLength(11);
    expect(new Set(watermarks).size).toBe(11);

    expect([...watermarks].sort()).toEqual(watermarks);
  });
});
