import {PG_OBJECT_IN_USE} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {PostgresError} from 'postgres';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../../shared/src/queue.ts';
import {sleep} from '../../../../../shared/src/sleep.ts';
import {getConnectionURI, type PgTest, test} from '../../../test/db.ts';
import {DbFile} from '../../../test/lite.ts';
import {type PostgresDB} from '../../../types/pg.ts';
import type {Source} from '../../../types/streams.ts';
import type {
  ChangeSource,
  ChangeStream,
} from '../../change-streamer/change-streamer-service.ts';
import type {MessageInsert} from '../protocol/current.ts';
import type {
  ChangeStreamMessage,
  Data,
} from '../protocol/current/downstream.ts';
import {initializePostgresChangeSource} from './change-source.ts';

const APP_ID = '23';
const SHARD_NUM = 1;

// The purpose of this test is to empirically verify the behavior of PG
// with respect to TOASTED values and publications. By default it is
// skipped to avoid unnecessary load on the CI runner.
describe.skip('toasted values', {timeout: 30000, retry: 3}, () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let upstreamURI: string;
  let replicaDbFile: DbFile;
  let source: ChangeSource;
  let streams: ChangeStream[];

  beforeEach<PgTest>(async ({testDBs}) => {
    streams = [];
    lc = createSilentLogContext();
    upstream = await testDBs.create('change_source_toasted_values_upstream');
    replicaDbFile = new DbFile('change_source_toasted_values_replica');

    upstreamURI = getConnectionURI(upstream);
    await upstream.unsafe(`
    CREATE TABLE foo(
      id INT4 PRIMARY KEY,
      int INT4,
      big1 TEXT,
      big2 TEXT,
      big3 TEXT
    );
    CREATE PUBLICATION zero_foo FOR TABLE foo(id, int, big1);
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

  async function startReplication() {
    ({changeSource: source} = await initializePostgresChangeSource(
      lc,
      upstreamURI,
      {
        appID: APP_ID,
        publications: ['zero_foo'],
        shardNum: SHARD_NUM,
      },
      replicaDbFile.path,
      {tableCopyWorkers: 5},
    ));
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
          // oxlint-disable-next-line no-console
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

  async function expectMessage(
    tag: 'begin' | 'commit' | Data[1]['tag'],
    downstream: Queue<ChangeStreamMessage>,
  ) {
    const msg = await downstream.dequeue();
    expect((msg as Data)[1].tag).toBe(tag);
  }

  const NUM_ROWS = 10;

  test('toasted values omitted for all updates', async () => {
    await startReplication();
    const {changes} = await startStream('00');
    const downstream = drainToQueue(changes);

    await upstream.begin(sql => {
      for (let i = 0; i < NUM_ROWS; i++) {
        void sql`
        INSERT INTO foo(id, int, big1, big2, big3)
          VALUES ${sql({
            id: i,
            int: i,
            big1: String(i) + 'a'.repeat(1_000_000),
            big2: String(i) + 'b'.repeat(1_000_000),
            big3: String(i) + 'c'.repeat(1_000_000),
          })}
        `.execute();
      }
    });

    await expectMessage('begin', downstream);
    for (let i = 0; i < NUM_ROWS; i++) {
      const change = (await downstream.dequeue()) as Data;
      expect(change).toMatchObject([
        'data',
        {
          tag: 'insert',
          new: {
            id: i,
            int: i,
            big1: expect.any(String),
          },
        },
      ]);
      // Sanity check: big2 and big3 should not be present.
      expect(Object.keys((change[1] as MessageInsert).new)).toMatchObject([
        'id',
        'int',
        'big1',
      ]);
    }
    await expectMessage('commit', downstream);

    await upstream /*sql*/ `UPDATE foo SET int = int+1`;
    await expectMessage('begin', downstream);
    for (let i = 0; i < NUM_ROWS; i++) {
      const change = (await downstream.dequeue()) as Data;
      expect(change).toMatchObject([
        'data',
        {
          tag: 'update',
          new: {
            id: expect.any(Number),
            int: expect.any(Number),
            big1: undefined, // Note: undefined means omitted as TOAST
          },
        },
      ]);
    }
    await expectMessage('commit', downstream);

    // Add (big) columns with existing data.
    await upstream /*sql*/ `
      ALTER PUBLICATION zero_foo SET TABLE foo(id, int, big1, big2, big3)
    `;

    await expectMessage('begin', downstream);
    await expectMessage('add-column', downstream);
    await expectMessage('add-column', downstream);
    await expectMessage('commit', downstream);

    await upstream /*sql*/ `UPDATE foo SET int = int+1`;
    await expectMessage('begin', downstream);

    // New columns are still omitted for UPDATES.
    for (let i = 0; i < NUM_ROWS; i++) {
      const change = (await downstream.dequeue()) as Data;
      expect(change).toMatchObject([
        'data',
        {
          tag: 'update',
          new: {
            id: expect.any(Number),
            int: expect.any(Number),
            big1: undefined, // Note: undefined means omitted as TOAST
            big2: undefined, // Note: undefined means omitted as TOAST
            big3: undefined, // Note: undefined means omitted as TOAST
          },
        },
      ]);
    }
    await expectMessage('commit', downstream);
  });
});
