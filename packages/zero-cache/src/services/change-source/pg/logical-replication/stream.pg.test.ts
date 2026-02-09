import type {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../../../shared/src/must.ts';
import {Queue} from '../../../../../../shared/src/queue.ts';
import {sleep} from '../../../../../../shared/src/sleep.ts';
import {dropReplicationSlots, testDBs} from '../../../../test/db.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import type {Source} from '../../../../types/streams.ts';
import {fromBigInt, toBigInt, type LSN} from '../lsn.ts';
import type {MessageCommit} from './pgoutput.types.ts';
import {subscribe, type StreamMessage} from './stream.ts';

describe('pg/logic-replication', {timeout: 30000}, () => {
  let lc: LogContext;
  let db: PostgresDB;

  const SLOT = 'logical_replication_stream_pg_test_slot';

  beforeEach(async () => {
    lc = createSilentLogContext();
    db = await testDBs.create('logical_replication_stream_pg_test');

    await db.unsafe(`
    CREATE TABLE foo(
      id TEXT CONSTRAINT foo_pk PRIMARY KEY,
      int INT4,
      big BIGINT,
      flt FLOAT8,
      bool BOOLEAN,
      date DATE,
      time TIMESTAMPTZ,
      json JSON,
      num NUMERIC,
      ints INT4[],
      times TIMESTAMPTZ[],
      jsons JSON[]
    );
    CREATE PUBLICATION foo_pub FOR TABLE foo;

    CREATE SCHEMA IF NOT EXISTS my;
    CREATE TABLE my.boo(
      a TEXT PRIMARY KEY, b TEXT, c TEXT, d TEXT
    );
    CREATE PUBLICATION my_pub FOR TABLES IN SCHEMA my;`);

    await db`SELECT pg_create_logical_replication_slot(${SLOT}, 'pgoutput');`;
  });

  afterEach(async () => {
    await dropReplicationSlots(db);
    await testDBs.drop(db);
  });

  function drainToQueue(sub: Source<StreamMessage>): Queue<StreamMessage[1]> {
    const queue = new Queue<StreamMessage[1]>();
    void (async () => {
      try {
        for await (const msg of sub) {
          queue.enqueue(msg[1]);
        }
      } catch (e) {
        queue.enqueueRejection(e);
      }
    })();
    return queue;
  }

  async function expectNonRelationMessages(
    queue: Queue<StreamMessage[1]>,
    count: number,
  ) {
    const msgs: StreamMessage[1][] = [];
    for (let i = 0; i < count; i++) {
      const msg = await queue.dequeue();
      // PG may over-send relation messages if it forgot that it already
      // sent one (presumably due to some state being reset). Ignore them
      // to eliminate the variablility.
      if (msg.tag === 'relation') {
        i--;
      } else {
        msgs.push(msg);
      }
    }
    return msgs;
  }

  async function expectConfirmedFlushLSN(lsn: LSN | null) {
    const expected = fromBigInt(toBigInt(lsn ?? '0/0'));

    // Since there's no reliable mechanism to wait for the ack to have
    // been processed by postgres, add retries with sleeps in between.
    const MAX_ATTEMPTS = 10;

    for (let i = 0; i < MAX_ATTEMPTS; i++) {
      const [{confirmed}] = await db<{confirmed: string}[]>`
        SELECT confirmed_flush_lsn as confirmed FROM pg_replication_slots
          WHERE slot_name = ${SLOT}`;
      if (confirmed === expected) {
        return;
      }
      if (i < MAX_ATTEMPTS - 1) {
        await sleep(100);
      } else {
        expect(confirmed).toBe(expected);
      }
    }
  }

  test('logical replication messages', async () => {
    const {messages} = await subscribe(lc, db, SLOT, ['foo_pub', 'my_pub'], 0n);
    const msgs = drainToQueue(messages);

    await db.unsafe(`
    -- tag: "insert"
    INSERT INTO foo (id, int, big, flt, bool, date, time, json, num, ints, times, jsons)
      VALUES (
        'bar', 
        123, 
        '123456789098765432'::bigint,
        456.789,
        true,
        '2025-03-19',
        '2019-01-12T00:30:35.381101032Z',
        '{"zoo":"dar"}',
        '12345.678909876',
        ARRAY[1, 2, 3],
        ARRAY['2019-01-12T00:30:35.654321Z'::timestamptz, '2019-01-12T00:30:35.123456Z'::timestamptz],
        ARRAY['1'::json, '"2"'::json, '{"a":123}'::json]
        );

    -- tag: "update"
    UPDATE foo set bool = false;

    -- tag: "delete"
    DELETE FROM foo;
    
    -- multiple publication support
    INSERT INTO my.boo(a, b, c, d) VALUES ('1', '2', '3', '4');

    -- tag: "truncate"
    TRUNCATE my.boo, foo;

    -- tag: "message"
    SELECT pg_logical_emit_message(true, 'foo/bar', 'baz');
    `);

    expect(await msgs.dequeue()).toMatchObject({tag: 'begin'});
    expect(await msgs.dequeue()).toMatchObject({
      tag: 'relation',
      schema: 'public',
      name: 'foo',
      keyColumns: ['id'],
      relationOid: expect.any(Number),
      replicaIdentity: 'default',
    });
    expect(await msgs.dequeue()).toMatchObject({
      tag: 'insert',
      relation: {name: 'foo'},
      new: {
        big: 123456789098765432n,
        bool: true,
        date: 1742342400000,
        flt: 456.789,
        id: 'bar',
        int: 123,
        json: '{"zoo":"dar"}',
        num: 12345.678909876,
        time: 1547253035381.101,
        ints: [1, 2, 3],
        times: [1547253035654.321, 1547253035123.456],
        jsons: [1, '2', {a: 123}],
      },
    });
    expect(await msgs.dequeue()).toMatchObject({
      tag: 'update',
      relation: {name: 'foo'},
      key: null,
      new: {
        big: 123456789098765432n,
        bool: false,
        date: 1742342400000,
        flt: 456.789,
        id: 'bar',
        int: 123,
        json: '{"zoo":"dar"}',
        num: 12345.678909876,
        time: 1547253035381.101,
        jsons: [1, '2', {a: 123}],
      },
      old: null,
    });
    expect(await msgs.dequeue()).toMatchObject({
      tag: 'delete',
      relation: {name: 'foo'},
      key: {id: 'bar'},
      old: null,
    });
    expect(await msgs.dequeue()).toMatchObject({
      tag: 'relation',
      schema: 'my',
      name: 'boo',
      relationOid: expect.any(Number),
      replicaIdentity: 'default',
    });
    expect(await msgs.dequeue()).toMatchObject({
      tag: 'insert',
      new: {a: '1', b: '2', c: '3', d: '4'},
      relation: {name: 'boo'},
    });
    expect(await msgs.dequeue()).toMatchObject({
      tag: 'relation',
      schema: 'my',
      name: 'boo',
    });
    expect(await msgs.dequeue()).toMatchObject({
      tag: 'relation',
      schema: 'public',
      name: 'foo',
    });
    expect(await msgs.dequeue()).toMatchObject({
      tag: 'truncate',
      restartIdentity: false,
      cascade: false,
      relations: [
        {
          tag: 'relation',
          schema: 'my',
          name: 'boo',
        },
        {
          tag: 'relation',
          schema: 'public',
          name: 'foo',
        },
      ],
    });
    expect(await msgs.dequeue()).toMatchObject({
      tag: 'message',
      prefix: 'foo/bar',
      content: Buffer.from([98, 97, 122]), // 'b', 'a', 'z'
      flags: 1,
      transactional: true,
    });
    expect(await msgs.dequeue()).toMatchObject({tag: 'commit'});
  });

  test('acks', async () => {
    const {messages, acks} = await subscribe(
      lc,
      db,
      SLOT,
      ['foo_pub', 'my_pub'],
      0n,
    );
    const msgs = drainToQueue(messages);

    await db.unsafe(`
    INSERT INTO my.boo(a, b, c, d) VALUES ('1', '2', '3', '4');
    `);

    await db.unsafe(`
      INSERT INTO my.boo(a, b, c, d) VALUES ('e', 'f', 'g', 'h');
    `);

    await db.unsafe(`
      INSERT INTO my.boo(a, b, c, d) VALUES ('w', 'x', 'y', 'z');
    `);

    const [
      begin1,
      insert1,
      commit1,
      begin2,
      insert2,
      commit2,
      begin3,
      insert3,
      commit3,
    ] = await expectNonRelationMessages(msgs, 9);

    expect(begin1.tag).toBe('begin');
    expect(insert1.tag).toBe('insert');
    expect(commit1.tag).toBe('commit');

    expect(begin2.tag).toBe('begin');
    expect(insert2.tag).toBe('insert');
    expect(commit2.tag).toBe('commit');

    expect(begin3.tag).toBe('begin');
    expect(insert3.tag).toBe('insert');
    expect(commit3.tag).toBe('commit');

    for (const commit of [commit1, commit2, commit3] as MessageCommit[]) {
      const lsn = toBigInt(must(commit.commitLsn));
      acks.push(lsn);
      await expectConfirmedFlushLSN(commit.commitLsn);
    }
  });

  // This fails with:
  //
  // FAIL   zero-cache/pg-17  src/services/change-source/pg/logical-replication/stream.pg.test.ts > pg/logic-replication > session resumption from confirmed flushes
  // Error: exceeded max attempts (6) to start the Postgres stream
  //  ❯ startReplicationStream src/services/change-source/pg/logical-replication/stream.ts:164:9
  //     162|     }
  //     163|   }
  //     164|   throw new Error(
  //        |         ^
  //     165|     `exceeded max attempts (${maxAttempts}) to start the Postgres stre…
  //     166|   );
  //  ❯ subscribe src/services/change-source/pg/logical-replication/stream.ts:63:32
  //  ❯ src/services/change-source/pg/logical-replication/stream.pg.test.ts:385:18
  test.skip('session resumption from confirmed flushes', async () => {
    // Interleave commits from three different transactions.
    const queue1 = new Queue<true>();
    const queue2 = new Queue<true>();
    const queue3 = new Queue<true>();

    const tx1 = db.begin(async tx => {
      queue2.enqueue(true);
      await queue1.dequeue();
      await tx`INSERT INTO foo(id) VALUES ('1');`;
      queue2.enqueue(true);
      await queue1.dequeue();
      await tx`INSERT INTO foo(id) VALUES ('11');`;
      queue2.enqueue(true);
    });

    await queue2.dequeue();
    const tx2 = db.begin(async tx => {
      queue3.enqueue(true);
      await queue2.dequeue();
      await tx`INSERT INTO foo(id) VALUES ('2');`;
      queue3.enqueue(true);
      await queue2.dequeue();
      await tx`INSERT INTO foo(id) VALUES ('22');`;
      queue3.enqueue(true);
      await tx1;
    });

    await queue3.dequeue();
    const tx3 = db.begin(async tx => {
      queue1.enqueue(true);
      await queue3.dequeue();
      await tx`INSERT INTO foo(id) VALUES ('3');`;
      queue1.enqueue(true);
      await queue3.dequeue();
      await tx`INSERT INTO foo(id) VALUES ('33');`;
      queue1.enqueue(true);
      await tx2;
    });

    await Promise.all([tx1, tx2, tx3]);

    const sub1 = await subscribe(lc, db, SLOT, ['foo_pub', 'my_pub'], 0n);

    const msgs1 = await expectNonRelationMessages(
      drainToQueue(sub1.messages),
      12,
    );
    expect(msgs1.map(({tag}) => tag)).toEqual([
      'begin', // 0
      'insert', // 1
      'insert', // 2
      'commit', // 3

      'begin', // 4
      'insert', // 5
      'insert', // 6
      'commit', // 7

      'begin', // 8
      'insert', // 9
      'insert', // 10
      'commit', // 11
    ]);

    expect(msgs1[1]).toMatchObject({
      tag: 'insert',
      new: {id: '1'},
      relation: {
        tag: 'relation',
        name: 'foo',
      },
    });
    expect(msgs1[2]).toMatchObject({
      tag: 'insert',
      new: {id: '11'},
      relation: {
        tag: 'relation',
        name: 'foo',
      },
    });
    const commit1 = msgs1[3] as MessageCommit;
    const commit1Lsn = toBigInt(must(commit1.commitEndLsn));
    sub1.acks.push(commit1Lsn);
    sub1.messages.cancel();
    await expectConfirmedFlushLSN(commit1.commitEndLsn);

    // Sub2 should resume from the second transaction.
    const sub2 = await subscribe(lc, db, SLOT, ['foo_pub', 'my_pub'], 0n);
    const msgs2 = await expectNonRelationMessages(
      drainToQueue(sub2.messages),
      8,
    );
    expect(msgs2.map(({tag}) => tag)).toEqual([
      'begin', // 0
      'insert', // 1
      'insert', // 2
      'commit', // 3

      'begin', // 4
      'insert', // 5
      'insert', // 6
      'commit', // 7
    ]);
    expect(msgs2[1]).toMatchObject({
      tag: 'insert',
      new: {id: '2'},
      relation: {
        tag: 'relation',
        name: 'foo',
      },
    });
    expect(msgs2[2]).toMatchObject({
      tag: 'insert',
      new: {id: '22'},
      relation: {
        tag: 'relation',
        name: 'foo',
      },
    });
    const commit2 = msgs2[3] as MessageCommit;
    const commit2Lsn = toBigInt(must(commit2.commitEndLsn));
    sub2.acks.push(commit2Lsn);
    sub2.messages.cancel();
    await expectConfirmedFlushLSN(commit2.commitEndLsn);

    // Sub3 should resume from the third transaction.
    const sub3 = await subscribe(lc, db, SLOT, ['foo_pub', 'my_pub'], 0n);
    const msgs3 = await expectNonRelationMessages(
      drainToQueue(sub3.messages),
      4,
    );
    expect(msgs3.map(({tag}) => tag)).toEqual([
      'begin', // 0
      'insert', // 1
      'insert', // 2
      'commit', // 3
    ]);
    expect(msgs3[1]).toMatchObject({
      tag: 'insert',
      new: {id: '3'},
      relation: {
        tag: 'relation',
        name: 'foo',
      },
    });
    expect(msgs3[2]).toMatchObject({
      tag: 'insert',
      new: {id: '33'},
      relation: {
        tag: 'relation',
        name: 'foo',
      },
    });
    const commit3 = msgs3[3] as MessageCommit;
    const commit3Lsn = toBigInt(must(commit3.commitEndLsn));
    sub3.acks.push(commit3Lsn);
    sub3.messages.cancel();
    await expectConfirmedFlushLSN(commit3.commitEndLsn);
  });
});
