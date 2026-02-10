import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../../shared/src/must.ts';
import {getConnectionURI, type PgTest, test} from '../../../test/db.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {BackfillRequest} from '../protocol/current.ts';
import {streamBackfill} from './backfill-stream.ts';
import {getPublicationInfo} from './schema/published.ts';

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

    return () => testDBs.drop(upstream);
  });

  test(`column backfill`, async () => {
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: 'slot_name', publications: ['the_pub']},
      columnBackfillRequest,
    );
    const results = [];
    for await (const msg of stream) {
      results.push(msg);
    }
    expect(results).toMatchObject([
      {
        tag: 'backfill',
        watermark: expect.any(String),
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id1', 'id2']},
        },
        columns: ['c', 'b'],
        rowValues: [
          // Column 'c JSON[]' should be an array of JSON values
          // Column 'b JSON' should be stringified JSON
          [1n, 2, [1, 2, '3', {e: 4}], '{"d" : 1}'],
          [2n, 3, [2, 3, '4', {e: 5}], '{"d" : 2}'],
          [3n, 4, [3, 4, '5', {e: 6}], '{"d" : 3}'],
          [4n, 5, [4, 5, '6', {e: 7}], '{"d" : 4}'],
          [5n, 6, [5, 6, '7', {e: 8}], '{"d" : 5}'],
          [6n, 7, [6, 7, '8', {e: 9}], '{"d" : 6}'],
          [7n, 8, [7, 8, '9', {e: 10}], '{"d" : 7}'],
          [8n, 9, [8, 9, '10', {e: 11}], '{"d" : 8}'],
          [9n, 10, [9, 10, '11', {e: 12}], '{"d" : 9}'],
          [10n, 11, [10, 11, '12', {e: 13}], '{"d" : 10}'],
        ],
      },
      {
        tag: 'backfill-completed',
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id1', 'id2']},
        },
        columns: ['c', 'b'],
      },
    ]);
  });

  test(`column backfill: flushed in small batches`, async () => {
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: 'slot_name', publications: ['the_pub']},
      columnBackfillRequest,
      {flushThresholdBytes: 1024}, // 1K flush threshold
    );
    const results = [];
    for await (const msg of stream) {
      results.push(msg);
    }
    expect(results).toMatchObject([
      {
        tag: 'backfill',
        watermark: expect.any(String),
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id1', 'id2']},
        },
        columns: ['c', 'b'],
        rowValues: [
          [1n, 2, [1, 2, '3', {e: 4}], '{"d" : 1}'],
          [2n, 3, [2, 3, '4', {e: 5}], '{"d" : 2}'],
          [3n, 4, [3, 4, '5', {e: 6}], '{"d" : 3}'],
        ],
      },
      {
        tag: 'backfill',
        watermark: expect.any(String),
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id1', 'id2']},
        },
        columns: ['c', 'b'],
        rowValues: [
          [4n, 5, [4, 5, '6', {e: 7}], '{"d" : 4}'],
          [5n, 6, [5, 6, '7', {e: 8}], '{"d" : 5}'],
          [6n, 7, [6, 7, '8', {e: 9}], '{"d" : 6}'],
        ],
      },
      {
        tag: 'backfill',
        watermark: expect.any(String),
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id1', 'id2']},
        },
        columns: ['c', 'b'],
        rowValues: [
          [7n, 8, [7, 8, '9', {e: 10}], '{"d" : 7}'],
          [8n, 9, [8, 9, '10', {e: 11}], '{"d" : 8}'],
          [9n, 10, [9, 10, '11', {e: 12}], '{"d" : 9}'],
        ],
      },
      {
        tag: 'backfill',
        watermark: expect.any(String),
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id1', 'id2']},
        },
        columns: ['c', 'b'],
        rowValues: [[10n, 11, [10, 11, '12', {e: 13}], '{"d" : 10}']],
      },
      {
        tag: 'backfill-completed',
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id1', 'id2']},
        },
        columns: ['c', 'b'],
      },
    ]);
  });

  test(`table backfill`, async () => {
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: 'slot_name', publications: ['the_pub']},
      tableBackfillRequest,
    );
    const results = [];
    for await (const msg of stream) {
      results.push(msg);
    }
    // Columns should deduped and ordered: [id2, id1, a, c, b]
    expect(results).toMatchObject([
      {
        tag: 'backfill',
        watermark: expect.any(String),
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id2', 'id1']},
        },
        columns: ['a', 'c', 'b'],
        rowValues: [
          [2, 1n, '1111111111', [1, 2, '3', {e: 4}], '{"d" : 1}'],
          [3, 2n, '2222222222', [2, 3, '4', {e: 5}], '{"d" : 2}'],
          [4, 3n, '3333333333', [3, 4, '5', {e: 6}], '{"d" : 3}'],
          [5, 4n, '4444444444', [4, 5, '6', {e: 7}], '{"d" : 4}'],
          [6, 5n, '5555555555', [5, 6, '7', {e: 8}], '{"d" : 5}'],
          [7, 6n, '6666666666', [6, 7, '8', {e: 9}], '{"d" : 6}'],
          [8, 7n, '7777777777', [7, 8, '9', {e: 10}], '{"d" : 7}'],
          [9, 8n, '8888888888', [8, 9, '10', {e: 11}], '{"d" : 8}'],
          [10, 9n, '9999999999', [9, 10, '11', {e: 12}], '{"d" : 9}'],
          [
            11,
            10n,
            '10101010101010101010',
            [10, 11, '12', {e: 13}],
            '{"d" : 10}',
          ],
        ],
      },
      {
        tag: 'backfill-completed',
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id2', 'id1']},
        },
        columns: ['a', 'c', 'b'],
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
      {slot: 'slot_name', publications: ['the_pub']},
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
      {slot: 'slot_name', publications: ['the_pub']},
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
