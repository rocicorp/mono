import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../../shared/src/must.ts';
import {getConnectionURI, type PgTest, test} from '../../../test/db.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {BackfillRequest, MessageBackfill} from '../protocol/current.ts';
import {streamBackfill} from './backfill-stream.ts';
import {getPublicationInfo} from './schema/published.ts';

const SLOT_NAME = 'backfill_test_slot';
const RUN_ID = 'test-run';

describe('backfill-stream', () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let upstreamURI: string;
  let columnBackfillRequest: BackfillRequest;
  let tableBackfillRequest: BackfillRequest;

  beforeEach<PgTest>(async ({testDBs}) => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('backfill_stream_test_db');
    upstreamURI = getConnectionURI(upstream);

    await upstream.unsafe(/*sql*/ `
      CREATE TABLE foo(
        id1 INT8 NOT NULL,
        id2 INT4 NOT NULL,
        a TEXT,
        b JSON,
        c JSON[],
        PRIMARY KEY(id1, id2)
      );
      CREATE TABLE bar(
        id1 INT8 NOT NULL,
        id2 INT4 NOT NULL,
        a TEXT,
        b JSON,
        c JSON[],
        PRIMARY KEY(id1, id2)
      );
      CREATE PUBLICATION the_pub FOR TABLE foo;

      DO $$
      BEGIN
        FOR i IN 1..10 LOOP
          INSERT INTO foo (id1, id2, a, b, c)
            VALUES(i, i+1, 
              REPEAT(i::text, 10), 
              json_build_object('d', i),  
              ARRAY[to_json(i), to_json(i+1), to_json((i+2)::text), json_build_object('e', i+3)]
            );
        END LOOP;
      END $$;

      ANALYZE foo;
      ANALYZE bar;
    `);

    const {tables} = await getPublicationInfo(upstream, ['the_pub']);
    const tableSpec = tables[0];

    columnBackfillRequest = {
      table: {
        schema: 'public',
        name: 'foo',
        metadata: {
          schemaOID: must(tableSpec.schemaOID),
          relationOID: tableSpec.oid,
          rowKey: {
            id1: {attNum: tableSpec.columns.id1.pos},
            id2: {attNum: tableSpec.columns.id2.pos},
          },
        },
      },
      columns: {
        c: {attNum: tableSpec.columns.c.pos},
        b: {attNum: tableSpec.columns.b.pos},
      },
    };

    tableBackfillRequest = {
      table: {
        schema: 'public',
        name: 'foo',
        metadata: {
          schemaOID: must(tableSpec.schemaOID),
          relationOID: tableSpec.oid,
          rowKey: {
            id2: {attNum: tableSpec.columns.id2.pos},
            id1: {attNum: tableSpec.columns.id1.pos},
          },
        },
      },
      columns: {
        id1: {attNum: tableSpec.columns.id1.pos},
        id2: {attNum: tableSpec.columns.id2.pos},
        a: {attNum: tableSpec.columns.a.pos},
        c: {attNum: tableSpec.columns.c.pos},
        b: {attNum: tableSpec.columns.b.pos},
      },
    };

    return async () => {
      expect(
        await upstream /*sql*/ `
          SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'backfill_test_slot_%'`,
      ).toEqual([]);

      await testDBs.drop(upstream);
    };
  });

  test.each([
    {mode: 'binary', textCopy: false, dataBytes: 922},
    {mode: 'text', textCopy: true, dataBytes: 474},
  ])(`column backfill ($mode)`, async ({textCopy, dataBytes}) => {
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      columnBackfillRequest,
      {textCopy, newRunID: () => RUN_ID},
    );
    const results = [];
    for await (const msg of stream) {
      results.push(msg);
    }

    // Binary mode returns JSON[] as a stringified array.
    // Text mode returns JSON[] as a parsed JS array.
    const arr = (vals: unknown[]) => (textCopy ? vals : JSON.stringify(vals));

    expect(results).toMatchObject([
      {
        byteSize: 0,
        message: {
          tag: 'backfill-started',
          watermark: expect.any(String),
          relation: {
            schema: 'public',
            name: 'foo',
            rowKey: {columns: ['id1', 'id2']},
          },
          columns: ['c', 'b'],
          runID: RUN_ID,
          resumeFrom: null,
        },
      },
      {
        byteSize: dataBytes,
        message: {
          tag: 'backfill',
          watermark: expect.any(String),
          relation: {
            schema: 'public',
            name: 'foo',
            rowKey: {columns: ['id1', 'id2']},
          },
          columns: ['c', 'b'],
          runID: RUN_ID,
          lastKey: ['10', '11'],
          rowValues: [
            [1n, 2, arr([1, 2, '3', {e: 4}]), '{"d" : 1}'],
            [2n, 3, arr([2, 3, '4', {e: 5}]), '{"d" : 2}'],
            [3n, 4, arr([3, 4, '5', {e: 6}]), '{"d" : 3}'],
            [4n, 5, arr([4, 5, '6', {e: 7}]), '{"d" : 4}'],
            [5n, 6, arr([5, 6, '7', {e: 8}]), '{"d" : 5}'],
            [6n, 7, arr([6, 7, '8', {e: 9}]), '{"d" : 6}'],
            [7n, 8, arr([7, 8, '9', {e: 10}]), '{"d" : 7}'],
            [8n, 9, arr([8, 9, '10', {e: 11}]), '{"d" : 8}'],
            [9n, 10, arr([9, 10, '11', {e: 12}]), '{"d" : 9}'],
            [10n, 11, arr([10, 11, '12', {e: 13}]), '{"d" : 10}'],
          ],
          status: {rows: 10, totalRows: 10, totalBytes: expect.any(Number)},
        },
      },
      {
        byteSize: 0,
        message: {
          tag: 'backfill-completed',
          relation: {
            schema: 'public',
            name: 'foo',
            rowKey: {columns: ['id1', 'id2']},
          },
          columns: ['c', 'b'],
          runID: RUN_ID,
          status: {rows: 10, totalRows: 10, totalBytes: expect.any(Number)},
        },
      },
    ]);
  });

  test.each([
    {mode: 'binary', textCopy: false, dataBytes: 1072},
    {mode: 'text', textCopy: true, dataBytes: 594},
  ])(`table backfill ($mode)`, async ({textCopy, dataBytes}) => {
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      tableBackfillRequest,
      {textCopy, newRunID: () => RUN_ID},
    );
    const results = [];
    for await (const msg of stream) {
      results.push(msg);
    }

    const arr = (vals: unknown[]) => (textCopy ? vals : JSON.stringify(vals));

    // Columns should deduped and ordered: [id2, id1, a, c, b]
    expect(results).toMatchObject([
      {
        byteSize: 0,
        message: {
          tag: 'backfill-started',
          watermark: expect.any(String),
          relation: {
            schema: 'public',
            name: 'foo',
            rowKey: {columns: ['id2', 'id1']},
          },
          columns: ['a', 'c', 'b'],
          runID: RUN_ID,
          resumeFrom: null,
        },
      },
      {
        byteSize: dataBytes,
        message: {
          tag: 'backfill',
          watermark: expect.any(String),
          relation: {
            schema: 'public',
            name: 'foo',
            rowKey: {columns: ['id2', 'id1']},
          },
          columns: ['a', 'c', 'b'],
          runID: RUN_ID,
          lastKey: ['11', '10'],
          rowValues: [
            [2, 1n, '1111111111', arr([1, 2, '3', {e: 4}]), '{"d" : 1}'],
            [3, 2n, '2222222222', arr([2, 3, '4', {e: 5}]), '{"d" : 2}'],
            [4, 3n, '3333333333', arr([3, 4, '5', {e: 6}]), '{"d" : 3}'],
            [5, 4n, '4444444444', arr([4, 5, '6', {e: 7}]), '{"d" : 4}'],
            [6, 5n, '5555555555', arr([5, 6, '7', {e: 8}]), '{"d" : 5}'],
            [7, 6n, '6666666666', arr([6, 7, '8', {e: 9}]), '{"d" : 6}'],
            [8, 7n, '7777777777', arr([7, 8, '9', {e: 10}]), '{"d" : 7}'],
            [9, 8n, '8888888888', arr([8, 9, '10', {e: 11}]), '{"d" : 8}'],
            [10, 9n, '9999999999', arr([9, 10, '11', {e: 12}]), '{"d" : 9}'],
            [
              11,
              10n,
              '10101010101010101010',
              arr([10, 11, '12', {e: 13}]),
              '{"d" : 10}',
            ],
          ],
          status: {rows: 10, totalRows: 10, totalBytes: expect.any(Number)},
        },
      },
      {
        byteSize: 0,
        message: {
          tag: 'backfill-completed',
          relation: {
            schema: 'public',
            name: 'foo',
            rowKey: {columns: ['id2', 'id1']},
          },
          columns: ['a', 'c', 'b'],
          runID: RUN_ID,
          status: {rows: 10, totalRows: 10, totalBytes: expect.any(Number)},
        },
      },
    ]);
  });

  test.each([
    ['Rename unrelated column', 'ALTER TABLE foo RENAME a TO z'],
    ['Rename unrelated table', 'ALTER TABLE bar RENAME TO baz'],
  ])('Compatible backfill request: %s', async (_name, sqlStmts) => {
    await upstream.unsafe(sqlStmts);
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      columnBackfillRequest,
    );
    for await (const _ of stream) {
      break;
    }
  });

  test.each([
    [
      'Rename table',
      `ALTER TABLE foo RENAME TO baz`,
      'Table has been renamed or dropped',
    ],
    [
      'Rename backfilling row key column',
      `ALTER TABLE foo RENAME id1 TO id`,
      'Row key (e.g. PRIMARY KEY or INDEX) has changed',
    ],
    [
      'Rename backfilling column',
      `ALTER TABLE foo RENAME b TO d`,
      'Column b has been renamed or dropped',
    ],
    [
      'Drop backfilling row key column',
      `ALTER TABLE foo DROP id2`,
      'Row key (e.g. PRIMARY KEY or INDEX) has changed',
    ],
    [
      'Drop backfilling column',
      `ALTER TABLE foo DROP c`,
      'Column c has been renamed or dropped',
    ],
    [
      'Drop backfilling table',
      `DROP TABLE foo`,
      'Table has been renamed or dropped',
    ],
    [
      'Swap backfilling row key names',
      /*sql*/ `
      ALTER TABLE foo RENAME id1 to id;
      ALTER TABLE foo RENAME id2 to id1;
      ALTER TABLE foo RENAME id to id2;
      `,
      'Column id1 no longer corresponds to the original column',
    ],
    [
      'Swap backfilling column names',
      /*sql*/ `
      ALTER TABLE foo RENAME b to d;
      ALTER TABLE foo RENAME c to b;
      ALTER TABLE foo RENAME d to c;
      `,
      'Column c no longer corresponds to the original column',
    ],
    [
      'Swap table names',
      /*sql*/ `
      ALTER TABLE foo RENAME TO boo;
      ALTER TABLE bar RENAME TO foo;
      ALTER TABLE boo RENAME TO bar;
      `,
      'Table has been renamed or dropped',
    ],
    [
      'Change backfilling row key',
      /*sql*/ `
      ALTER TABLE foo DROP CONSTRAINT foo_pkey;
      ALTER TABLE foo ADD CONSTRAINT foo_pkey PRIMARY KEY(id1);
      `,
      'Row key (e.g. PRIMARY KEY or INDEX) has changed',
    ],
  ])('Incompatible backfill request: %s', async (_name, sqlStmts, reason) => {
    await upstream.unsafe(sqlStmts);
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      columnBackfillRequest,
    );

    let result: unknown = null;
    try {
      for await (const _ of stream) {
        break;
      }
    } catch (e) {
      result = e;
    }
    expect(String(result)).toBe(
      `SchemaIncompatibilityError: Cannot backfill public.foo[c,b]: ${reason}`,
    );
  });
});

describe('ordered, resumable runs', () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let upstreamURI: string;

  beforeEach<PgTest>(async ({testDBs}) => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('backfill_resume_stream_test');
    upstreamURI = getConnectionURI(upstream);

    await upstream.unsafe(/*sql*/ `
      CREATE TABLE ints(id INT8 NOT NULL PRIMARY KEY, v TEXT);
      CREATE TABLE texts(k TEXT NOT NULL PRIMARY KEY, v TEXT);
      CREATE TABLE uuids(u UUID NOT NULL PRIMARY KEY, v TEXT);
      CREATE TABLE composite(
        a INT8 NOT NULL, b TEXT NOT NULL, v TEXT, PRIMARY KEY(a, b));
      -- A timestamp key decodes lossily, so it is never resumable.
      CREATE TABLE stamps(t TIMESTAMPTZ NOT NULL PRIMARY KEY, v TEXT);
      -- A random key is resumable in principle, but ordering it would be a
      -- scattered heap scan, so the run is not ordered.
      CREATE TABLE randoms(u UUID NOT NULL PRIMARY KEY, v TEXT);
      CREATE PUBLICATION the_pub FOR TABLE ints, texts, uuids, composite,
        stamps, randoms;

      INSERT INTO ints(id, v) SELECT i, 'v' || i FROM generate_series(1, 20) i;
      INSERT INTO texts(k, v)
        SELECT 'k' || lpad(i::text, 3, '0'), 'v' || i
          FROM generate_series(1, 20) i;
      INSERT INTO uuids(u, v)
        SELECT ('00000000-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
               'v' || i FROM generate_series(1, 20) i;
      INSERT INTO composite(a, b, v)
        SELECT i / 2, 'b' || i, 'v' || i FROM generate_series(1, 20) i;
      INSERT INTO stamps(t, v)
        SELECT timestamptz '2020-01-01' + (i || ' days')::interval, 'v' || i
          FROM generate_series(1, 20) i;
      INSERT INTO randoms(u, v)
        SELECT gen_random_uuid(), 'v' || i FROM generate_series(1, 5000) i;

      ANALYZE ints; ANALYZE texts; ANALYZE uuids; ANALYZE composite;
      ANALYZE stamps; ANALYZE randoms;
    `);

    return async () => {
      await testDBs.drop(upstream);
    };
  });

  async function request(
    table: string,
    keyColumns: string[],
    columns: string[],
    resumeFrom?: string[] | null,
  ): Promise<BackfillRequest> {
    const {tables} = await getPublicationInfo(upstream, ['the_pub']);
    const spec = must(tables.find(t => t.name === table));
    return {
      table: {
        schema: 'public',
        name: table,
        metadata: {
          schemaOID: must(spec.schemaOID),
          relationOID: spec.oid,
          rowKey: Object.fromEntries(
            keyColumns.map(col => [col, {attNum: spec.columns[col].pos}]),
          ),
        },
      },
      columns: Object.fromEntries(
        columns.map(col => [col, {attNum: spec.columns[col].pos}]),
      ),
      ...(resumeFrom === undefined ? {} : {resumeFrom}),
    };
  }

  async function collect(bf: BackfillRequest, opts = {}) {
    const messages = [];
    for await (const {message} of streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      bf,
      {newRunID: () => RUN_ID, ...opts},
    )) {
      messages.push(message);
    }
    return messages;
  }

  /** The row key values of every backfilled row, in stream order. */
  function keysOf(messages: {tag: string}[], keyLen: number) {
    return messages
      .filter(m => m.tag === 'backfill')
      .flatMap(m =>
        (m as MessageBackfill).rowValues.map(row =>
          row.slice(0, keyLen).map(String),
        ),
      );
  }

  test.each([
    {table: 'ints', key: ['id'], resumeAt: ['12']},
    {table: 'texts', key: ['k'], resumeAt: ['k012']},
    {
      table: 'uuids',
      key: ['u'],
      resumeAt: ['00000000-0000-4000-8000-000000000012'],
    },
    {table: 'composite', key: ['a', 'b'], resumeAt: ['6', 'b12']},
  ])('$table is ordered by its row key', async ({table, key, resumeAt}) => {
    const messages = await collect(await request(table, key, ['v']));

    // The announcement precedes every row of the run.
    expect(messages[0]).toMatchObject({
      tag: 'backfill-started',
      runID: RUN_ID,
      resumeFrom: null,
    });
    expect(messages.at(-1)).toMatchObject({
      tag: 'backfill-completed',
      runID: RUN_ID,
    });

    const keys = keysOf(messages, key.length);
    expect(keys.length).toBe(20);
    expect(keys).toEqual(keys.toSorted(compareKeys));

    // Every batch carries the mark of its last row, and the run's final mark
    // is the last key.
    const batches = messages.filter(m => m.tag === 'backfill');
    for (const batch of batches) {
      expect((batch as MessageBackfill).lastKey).toEqual(
        keysOf([batch], key.length).at(-1),
      );
    }
    expect((batches.at(-1) as MessageBackfill).lastKey).toEqual(keys.at(-1));

    // And resuming at an interior key yields exactly the suffix.
    const resumed = await collect(await request(table, key, ['v'], resumeAt));
    expect(resumed[0]).toMatchObject({
      tag: 'backfill-started',
      resumeFrom: resumeAt,
    });
    const markIndex = keys.findIndex(
      k => JSON.stringify(k) === JSON.stringify(resumeAt),
    );
    expect(markIndex).toBeGreaterThan(0);
    expect(keysOf(resumed, key.length)).toEqual(keys.slice(markIndex + 1));
  });

  test('a resumed run reports the progress of what remains', async () => {
    const resumed = await collect(await request('ints', ['id'], ['v'], ['12']));
    expect(resumed.at(-1)).toMatchObject({
      tag: 'backfill-completed',
      status: {rows: 8, totalRows: 8},
    });
  });

  test('resume is ANDed with the publication row filter', async () => {
    await upstream.unsafe(
      /*sql*/ `CREATE PUBLICATION filtered FOR TABLE ints WHERE (id > 5)`,
    );
    const bf = await request('ints', ['id'], ['v'], ['12']);
    const messages = [];
    for await (const {message} of streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['filtered']},
      bf,
      {newRunID: () => RUN_ID},
    )) {
      messages.push(message);
    }
    // The filter leaves 6..20 and the mark leaves 13..20.
    expect(keysOf(messages, 1).map(k => k[0])).toEqual(
      Array.from({length: 8}, (_, i) => String(13 + i)),
    );
  });

  test.each([
    {
      table: 'stamps',
      key: ['t'],
      why: 'a timestamp key has no exact text form',
    },
    {
      table: 'randoms',
      key: ['u'],
      why: 'a random key would be a scattered heap scan',
    },
  ])('$table is not ordered: $why', async ({table, key}) => {
    const messages = await collect(await request(table, key, ['v']));

    expect(messages[0]).toMatchObject({
      tag: 'backfill-started',
      runID: RUN_ID,
      // An unordered run covers every subscriber, so that they can all follow
      // it and honor its completion.
      resumeFrom: null,
    });
    for (const message of messages) {
      if (message.tag === 'backfill') {
        expect(message.lastKey).toBe(undefined);
      }
    }
  });

  test('a mark for a non-resumable table is ignored', async () => {
    const messages = await collect(
      await request(
        'randoms',
        ['u'],
        ['v'],
        ['00000000-0000-4000-8000-000000000001'],
      ),
    );
    expect(messages[0]).toMatchObject({
      tag: 'backfill-started',
      resumeFrom: null,
    });
    expect(keysOf(messages, 1).length).toBe(5000);
  });

  test('a never-analyzed table is not ordered', async () => {
    await upstream.unsafe(/*sql*/ `
      CREATE TABLE fresh(id INT8 NOT NULL PRIMARY KEY, v TEXT);
      INSERT INTO fresh SELECT i, 'v' || i FROM generate_series(1, 20) i;
      ALTER PUBLICATION the_pub ADD TABLE fresh;
    `);
    const messages = await collect(await request('fresh', ['id'], ['v']));
    expect(messages[0]).toMatchObject({resumeFrom: null});
    for (const message of messages) {
      if (message.tag === 'backfill') {
        expect(message.lastKey).toBe(undefined);
      }
    }
  });

  test('resume: false turns off ordering entirely', async () => {
    const messages = await collect(
      await request('ints', ['id'], ['v'], ['12']),
      {
        resume: false,
      },
    );
    // The run is still announced -- following is what lets a subscriber honor
    // the completion -- but it covers everyone and carries no marks.
    expect(messages[0]).toMatchObject({
      tag: 'backfill-started',
      runID: RUN_ID,
      resumeFrom: null,
    });
    expect(keysOf(messages, 1).length).toBe(20);
    for (const message of messages) {
      if (message.tag === 'backfill') {
        expect(message.lastKey).toBe(undefined);
      }
    }
  });

  test('resuming from an intermediate batch mark yields the rest', async () => {
    // A run large enough to span several COPY chunks, and therefore several
    // `backfill` messages. That is the shape a subscriber actually
    // interrupts: it holds the mark of some batch in the middle, not the
    // mark of the last one.
    const opts = {flushThresholdBytes: 1, minKeyCorrelation: 0};
    const full = await collect(await request('randoms', ['u'], ['v']), opts);
    const batches = full.filter(m => m.tag === 'backfill') as MessageBackfill[];
    expect(batches.length).toBeGreaterThan(2);

    // Every batch carries the mark of its own last row.
    const allKeys = keysOf(full, 1);
    expect(batches.map(b => must(b.lastKey))).toEqual(
      batches.map(b => keysOf([b], 1).at(-1)),
    );

    const middle = must(batches[0].lastKey);
    const resumed = await collect(
      await request('randoms', ['u'], ['v'], middle),
      opts,
    );
    const markIndex = allKeys.findIndex(k => k[0] === middle[0]);
    expect(markIndex).toBeGreaterThanOrEqual(0);
    expect(keysOf(resumed, 1)).toEqual(allKeys.slice(markIndex + 1));
  });

  test('a mark with the wrong arity is ignored', async () => {
    const messages = await collect(
      await request('composite', ['a', 'b'], ['v'], ['6']),
    );
    expect(messages[0]).toMatchObject({resumeFrom: null});
    expect(keysOf(messages, 2).length).toBe(20);
  });

  test('the minimum correlation is configurable', async () => {
    // `randoms` is excluded by the default threshold; a threshold of 0 admits
    // any correlation, which is what a deployment that wants resume on random
    // keys (and will pay for it) would set.
    const messages = await collect(await request('randoms', ['u'], ['v']), {
      minKeyCorrelation: 0,
    });
    const keys = keysOf(messages, 1);
    expect(keys.length).toBe(5000);
    expect(keys).toEqual(keys.toSorted(compareKeys));
    const batches = messages.filter(
      m => m.tag === 'backfill',
    ) as MessageBackfill[];
    expect(must(batches.at(-1)).lastKey).toEqual(keys.at(-1));
  });
});

/** Compares marks the way Postgres compares row keys in the C locale. */
function compareKeys(a: string[], b: string[]): number {
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      // The int keys in these fixtures are compared numerically; the text and
      // uuid keys are zero-padded so that lexical order matches.
      const [x, y] = [Number(a[i]), Number(b[i])];
      if (!Number.isNaN(x) && !Number.isNaN(y)) {
        return x - y;
      }
      return a[i] < b[i] ? -1 : 1;
    }
  }
  return 0;
}
