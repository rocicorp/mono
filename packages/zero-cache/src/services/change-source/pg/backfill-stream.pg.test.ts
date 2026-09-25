import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect} from 'vitest';
import {assert} from '../../../../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../../shared/src/must.ts';
import {getConnectionURI, type PgTest, test} from '../../../test/db.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {
  BackfillProgressMark,
  BackfillRequest,
} from '../protocol/current.ts';
import {ctidToProgressMark, streamBackfill} from './backfill-stream.ts';
import {getPublicationInfo} from './schema/published.ts';

const SLOT_NAME = 'backfill_test_slot';

function mark(block: number, offset: number, timeline?: string) {
  return {
    progressMark: ctidToProgressMark(`(${block},${offset})`),
    timeline: timeline ?? expect.any(String),
  };
}

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
        c: {id: {attNum: tableSpec.columns.c.pos}},
        b: {id: {attNum: tableSpec.columns.b.pos}},
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
        id1: {id: {attNum: tableSpec.columns.id1.pos}},
        id2: {id: {attNum: tableSpec.columns.id2.pos}},
        a: {id: {attNum: tableSpec.columns.a.pos}},
        c: {id: {attNum: tableSpec.columns.c.pos}},
        b: {id: {attNum: tableSpec.columns.b.pos}},
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
    {mode: 'binary', textCopy: false, dataBytes: 1013},
    {mode: 'text', textCopy: true, dataBytes: 535},
  ])(`column backfill ($mode)`, async ({textCopy, dataBytes}) => {
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      columnBackfillRequest,
      {textCopy},
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
          progressMarks: {current: mark(0, 10)},
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
          status: {rows: 10, totalRows: 10, totalBytes: expect.any(Number)},
          progressMarks: {previous: mark(0, 10)},
        },
      },
    ]);
    // A backfill from scratch has no `previous` mark.
    const [first] = results;
    expect(
      first.message.tag === 'backfill' && first.message.progressMarks?.previous,
    ).toBeUndefined();
  });

  test.each([
    {mode: 'binary', textCopy: false, dataBytes: 1163},
    {mode: 'text', textCopy: true, dataBytes: 655},
  ])(`table backfill ($mode)`, async ({textCopy, dataBytes}) => {
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      tableBackfillRequest,
      {textCopy},
    );
    const results = [];
    for await (const msg of stream) {
      results.push(msg);
    }

    const arr = (vals: unknown[]) => (textCopy ? vals : JSON.stringify(vals));

    // Columns should deduped and ordered: [id2, id1, a, c, b]
    expect(results).toMatchObject([
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
          progressMarks: {current: mark(0, 10)},
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
          status: {rows: 10, totalRows: 10, totalBytes: expect.any(Number)},
          progressMarks: {previous: mark(0, 10)},
        },
      },
    ]);
  });

  async function streamAll(
    req: BackfillRequest,
    opts?: Parameters<typeof streamBackfill>[4],
  ) {
    const results = [];
    for await (const msg of streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      req,
      opts,
    )) {
      results.push(msg.message);
    }
    return results;
  }

  function withProgress(
    req: BackfillRequest,
    progress: BackfillProgressMark,
  ): BackfillRequest {
    return {
      ...req,
      columns: Object.fromEntries(
        Object.entries(req.columns).map(([col, {id}]) => [col, {id, progress}]),
      ),
    };
  }

  async function currentTimeline() {
    const [{filenode}] = await upstream<{filenode: string}[]>`
      SELECT pg_relation_filenode('foo')::text AS filenode`;
    return filenode;
  }

  test.each([
    {mode: 'binary', textCopy: false},
    {mode: 'text', textCopy: true},
  ])('resumes from a progress mark ($mode)', async ({textCopy}) => {
    const timeline = await currentTimeline();
    const start = {progressMark: ctidToProgressMark('(0,6)'), timeline};
    const results = await streamAll(
      withProgress(columnBackfillRequest, start),
      {textCopy},
    );

    expect(results).toMatchObject([
      {
        tag: 'backfill',
        rowValues: [
          [7n, 8, expect.anything(), expect.anything()],
          [8n, 9, expect.anything(), expect.anything()],
          [9n, 10, expect.anything(), expect.anything()],
          [10n, 11, expect.anything(), expect.anything()],
        ],
        // Only the remaining rows are counted.
        status: {rows: 4, totalRows: 4},
        progressMarks: {previous: start, current: mark(0, 10, timeline)},
      },
      {
        tag: 'backfill-completed',
        progressMarks: {previous: mark(0, 10, timeline)},
      },
    ]);
  });

  test('resuming at the end of the table only completes', async () => {
    const timeline = await currentTimeline();
    const start = {progressMark: ctidToProgressMark('(0,10)'), timeline};
    expect(
      await streamAll(withProgress(columnBackfillRequest, start)),
    ).toMatchObject([
      {tag: 'backfill-completed', progressMarks: {previous: start}},
    ]);
  });

  test('restarts from scratch on a different timeline', async () => {
    const timeline = await currentTimeline();
    // A heap rewrite changes the table's relfilenode, and thus its timeline.
    await upstream.unsafe(`VACUUM FULL foo`);
    const newTimeline = await currentTimeline();
    expect(newTimeline).not.toBe(timeline);

    const results = await streamAll(
      withProgress(columnBackfillRequest, {
        progressMark: ctidToProgressMark('(0,6)'),
        timeline,
      }),
    );
    expect(results).toMatchObject([
      {
        tag: 'backfill',
        status: {rows: 10},
        progressMarks: {current: {timeline: newTimeline}},
      },
      {tag: 'backfill-completed'},
    ]);
    const [first] = results;
    expect(
      first.tag === 'backfill' && first.progressMarks?.previous,
    ).toBeUndefined();
  });

  test.each([
    ['VACUUM FULL', `VACUUM FULL foo`],
    // Not MVCC-safe: the table would appear empty to the snapshot.
    ['rewriting ALTER TABLE', `ALTER TABLE foo ALTER COLUMN id2 TYPE INT8`],
  ])(
    'fails if the table is rewritten after the snapshot (%s)',
    async (_, stmt) => {
      await expect(
        streamAll(columnBackfillRequest, {
          afterSnapshotForTesting: () => upstream.unsafe(stmt).then(() => {}),
        }),
      ).rejects.toThrow(/was rewritten after the backfill snapshot/);
    },
  );

  test('fails if the table is replaced after the snapshot', async () => {
    await expect(
      streamAll(columnBackfillRequest, {
        afterSnapshotForTesting: () =>
          upstream
            .unsafe(/*sql*/ `ALTER TABLE foo RENAME TO foo_old;
                       CREATE TABLE foo (LIKE foo_old INCLUDING ALL);`)
            .then(() => {}),
      }),
    ).rejects.toThrow(
      'Cannot backfill public.foo[c,b]: Table has been renamed or replaced',
    );
  });

  async function insertToastedRow() {
    // A large, poorly compressible value is stored out-of-line in TOAST.
    await upstream.unsafe(/*sql*/ `
      INSERT INTO foo (id1, id2, a) VALUES (11, 12,
        (SELECT string_agg(md5(i::text), '') FROM generate_series(1, 1000) i));
    `);
    const [{toasted}] = await upstream<{toasted: boolean}[]>`
      SELECT pg_relation_size(reltoastrelid) > 0 AS toasted
        FROM pg_class WHERE oid = 'foo'::regclass`;
    expect(toasted).toBe(true);
  }

  test('restarts from scratch if TOAST-able columns may be TOASTed', async () => {
    await insertToastedRow();
    const timeline = await currentTimeline();
    const results = await streamAll(
      withProgress(columnBackfillRequest, {
        progressMark: ctidToProgressMark('(0,6)'),
        timeline,
      }),
    );
    expect(results).toMatchObject([
      {
        tag: 'backfill',
        status: {rows: 11},
        progressMarks: {current: {timeline}},
      },
      {tag: 'backfill-completed'},
    ]);
    const [first] = results;
    expect(
      first.tag === 'backfill' && first.progressMarks?.previous,
    ).toBeUndefined();
  });

  test('resumes non-TOAST-able columns of a table with TOASTed values', async () => {
    await insertToastedRow();
    await upstream.unsafe(`ALTER TABLE foo ADD COLUMN d INT4 DEFAULT 5`);
    const [{attnum}] = await upstream<{attnum: number}[]>`
      SELECT attnum FROM pg_attribute
        WHERE attrelid = 'foo'::regclass AND attname = 'd'`;

    const timeline = await currentTimeline();
    const start = {progressMark: ctidToProgressMark('(0,6)'), timeline};
    const results = await streamAll(
      withProgress(
        {...columnBackfillRequest, columns: {d: {id: {attNum: attnum}}}},
        start,
      ),
    );
    expect(results).toMatchObject([
      {
        tag: 'backfill',
        rowValues: [
          [7n, 8, 5],
          [8n, 9, 5],
          [9n, 10, 5],
          [10n, 11, 5],
          [11n, 12, 5],
        ],
        status: {rows: 5},
        progressMarks: {previous: start, current: mark(0, 11, timeline)},
      },
      {tag: 'backfill-completed'},
    ]);
  });

  test('empty table', async () => {
    await upstream.unsafe(`TRUNCATE foo`);
    expect(await streamAll(columnBackfillRequest)).toEqual([
      expect.objectContaining({
        tag: 'backfill-completed',
        progressMarks: {},
      }),
    ]);
  });

  test('previous and current progress marks across two messages', async () => {
    // Enough data to span multiple COPY chunks (~64 KiB each).
    await upstream.unsafe(/*sql*/ `
      INSERT INTO foo (id1, id2, b)
        SELECT i, i+1, json_build_object('d', i, 'pad', repeat('x', 200))
          FROM generate_series(11, 2000) AS i;
    `);
    const ctids = new Map(
      (
        await upstream<{id1: bigint; ctid: string}[]>`
          SELECT id1, ctid::text FROM foo`
      ).map(({id1, ctid}) => [BigInt(id1), ctid]),
    );
    // The mark of the last row of a message, looked up by its `id1`.
    const lastRowMark = (rowValues: unknown[][]) => ({
      progressMark: ctidToProgressMark(
        must(ctids.get(BigInt(must(rowValues.at(-1))[0] as bigint))),
      ),
      timeline: expect.any(String),
    });

    const stream = (flushThresholdBytes?: number) => {
      const results = [];
      return (async () => {
        for await (const msg of streamBackfill(
          lc,
          upstreamURI,
          {slot: SLOT_NAME, publications: ['the_pub']},
          columnBackfillRequest,
          {flushThresholdBytes},
        )) {
          results.push(msg);
        }
        return results;
      })();
    };

    // Flush at half of the total bytes, which results in exactly two
    // `backfill` messages (given that no single chunk exceeds that).
    const totalBytes = (await stream()).reduce((t, m) => t + m.byteSize, 0);
    const results = (await stream(Math.ceil(totalBytes / 2))).map(
      m => m.message,
    );
    expect(results.map(m => m.tag)).toEqual([
      'backfill',
      'backfill',
      'backfill-completed',
    ]);
    const [first, second, completed] = results;
    assert(
      first.tag === 'backfill' && second.tag === 'backfill',
      'expected backfill messages',
    );
    expect(first.rowValues.length + second.rowValues.length).toBe(2000);

    const firstMark = lastRowMark(first.rowValues);
    const secondMark = lastRowMark(second.rowValues);
    expect(firstMark).not.toEqual(secondMark);

    // The first message of a backfill from scratch has no `previous`.
    expect(first.progressMarks).toEqual({current: firstMark});
    // The second continues from the first.
    expect(second.progressMarks).toEqual({
      previous: first.progressMarks?.current,
      current: secondMark,
    });
    // The completion continues from the second.
    expect(completed.progressMarks).toEqual({
      previous: second.progressMarks?.current,
    });
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
