/* eslint-disable no-irregular-whitespace */
import {Readable, Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {beforeEach, describe, expect} from 'vitest';
import type {JSONValue} from '../../../shared/src/json.ts';
import {randInt} from '../../../shared/src/rand.ts';
import {type PgTest, test} from '../test/db.ts';
import {type PostgresDB} from '../types/pg.ts';
import {NULL_BYTE, TextTransform} from './pg-copy.ts';

describe('pg-copy', () => {
  let sql: PostgresDB;

  beforeEach<PgTest>(async ({testDBs}) => {
    sql = await testDBs.create('pg_copy_test');

    await sql`
      CREATE TABLE foo (
        a int8,
        b int4,
        c int[],
        d text,
        e varchar(4096),
        f json,
        g jsonb,
        h float8,
        i text[],
        j text
      )`;

    return () => testDBs.drop(sql);
  });

  type Row = {
    a?: bigint;
    b?: number;
    c?: number[];
    d?: string;
    e?: string;
    f?: JSONValue;
    g?: JSONValue;
    h?: number;
    i?: string[];
    j?: string;
  };

  // JSONB is formatted with spaces after the colons and commas
  const jsonbStringify = (val: unknown) =>
    JSON.stringify(val).replaceAll(':', ': ').replaceAll(',', ', ');

  function toStrings({a, b, c, d, e, f, g, h, i, j}: Row): (string | null)[] {
    function isNull(v: unknown) {
      return v === null || v === undefined;
    }
    return [
      isNull(a) ? NULL_BYTE : String(a),
      isNull(b) ? NULL_BYTE : String(b),
      isNull(c) ? NULL_BYTE : `{${c}}`,
      d ?? NULL_BYTE,
      e ?? NULL_BYTE,
      isNull(f) ? NULL_BYTE : JSON.stringify(f),
      isNull(g) ? NULL_BYTE : jsonbStringify(g),
      isNull(h) ? NULL_BYTE : String(h),
      isNull(i) ? NULL_BYTE : `{${i}}`,
      j ?? NULL_BYTE,
    ];
  }

  async function testCopy(...input: Row[]) {
    for (const row of input) {
      await sql`INSERT INTO foo ${sql(row)}`;
    }

    // Get the raw text output from Postgres to feed it into
    // the TextTransform with various chunking scenarios.
    let rawResponse = '';
    await pipeline(
      await sql`COPY foo TO stdout`.readable(),
      new Writable({
        write: (chunk, _encoding, callback) => {
          rawResponse += chunk.toString();
          callback();
        },
      }),
    );

    const textOutput =
      rawResponse.length === 0
        ? await testAllEquivalent(
            // Direct from postgres
            await sql`COPY foo TO stdout`.readable(),
            // Empty stream
            Readable.from([]),
          )
        : await testAllEquivalent(
            // Direct from postgres
            await sql`COPY foo TO stdout`.readable(),
            // All in one string (no chunking)
            Readable.from(Buffer.from(rawResponse)),
            // One character chunks
            Readable.from(rawResponse.split('').map(s => Buffer.from(s))),
            // Random splits
            Readable.from(randomSplits(rawResponse).map(s => Buffer.from(s))),
          );

    expect(textOutput).toEqual(input.flatMap(toStrings));
    return textOutput.map(v => (v === NULL_BYTE ? null : v));
  }

  function randomSplits(input: string, maxSegmentLen = 3): string[] {
    const outputs: string[] = [];
    let l = 0;
    let r = 0;
    while (r < input.length) {
      r = Math.min(input.length, l + randInt(1, maxSegmentLen));
      outputs.push(input.substring(l, r));
      l = r;
    }
    return outputs;
  }

  async function testAllEquivalent(...streams: Readable[]) {
    const results: (string | null)[][] = [];
    for (const readable of streams) {
      const values: (string | null)[] = [];
      await pipeline(
        readable,
        new TextTransform(),
        new Writable({
          objectMode: true,
          write: (value, _encoding, callback) => {
            values.push(value);
            callback();
          },
        }),
      );
      results.push(values);
    }
    for (let i = 1; i < results.length; i++) {
      expect(results[i]).toEqual(results[0]);
    }
    return results[0];
  }

  test('empty table', async () => {
    expect(await testCopy()).toMatchInlineSnapshot(`[]`);
  });

  test('empty row (all nulls)', async () => {
    expect(await testCopy({a: null} as unknown as Row)).toMatchInlineSnapshot(`
      [
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
      ]
    `);
  });

  test('empty string', async () => {
    expect(await testCopy({d: ''})).toMatchInlineSnapshot(`
      [
        null,
        null,
        null,
        "",
        null,
        null,
        null,
        null,
        null,
        null,
      ]
    `);
  });

  test('adjacent empty strings', async () => {
    expect(await testCopy({d: '', e: ''})).toMatchInlineSnapshot(`
      [
        null,
        null,
        null,
        "",
        "",
        null,
        null,
        null,
        null,
        null,
      ]
    `);
  });

  test('strings with slashes', async () => {
    expect(await testCopy({d: '\\\\', e: '\\\\\\', j: '\\'}))
      .toMatchInlineSnapshot(`
        [
          null,
          null,
          null,
          "\\\\",
          "\\\\\\",
          null,
          null,
          null,
          null,
          "\\",
        ]
      `);
  });

  test('unicode escaped characters', async () => {
    expect(
      await testCopy({
        d: '\u1234',
        e: '\u293f\u32d9',
        // the NULL character can only appear in JSON values
        f: {a: `\u0000`, b: 'abc\u0000123'},
      }),
    ).toMatchInlineSnapshot(`
      [
        null,
        null,
        null,
        "ሴ",
        "⤿㋙",
        "{"a":"\\u0000","b":"abc\\u0000123"}",
        null,
        null,
        null,
        null,
      ]
    `);
  });

  test('single null byte as json value', async () => {
    expect(
      await testCopy({
        // The NULL character can only appear in JSON values,
        // and they will be encoded in a double-quoted string.
        f: `\u0000`,
      }),
    ).toMatchInlineSnapshot(`
      [
        null,
        null,
        null,
        null,
        null,
        ""\\u0000"",
        null,
        null,
        null,
        null,
      ]
    `);
  });

  test('backslash sequence escape characters', async () => {
    expect(
      await testCopy({
        d: '\t\\N\n',
        e: '\n\t\\N',
        f: {all: '\b\v\f\n\r\t'},
        j: '\f\n\t\b\r\v',
      }),
    ).toMatchInlineSnapshot(`
      [
        null,
        null,
        null,
        "	\\N
      ",
        "
      	\\N",
        "{"all":"\\b\\u000b\\f\\n\\r\\t"}",
        null,
        null,
        null,
        "
      	
      ",
      ]
    `);
  });

  const FUZZ_TEST_CHARS = `\\\r\n\t\f\b\r\v\u0123rntfbrv abc123/-_.,"'{}`;

  function randomFuzz() {
    let s = '';
    const length = randInt(1, 4096);
    for (let i = 0; i < length; i++) {
      s += FUZZ_TEST_CHARS[randInt(0, FUZZ_TEST_CHARS.length - 1)];
    }
    return s;
  }

  test('fuzz test', async () => {
    const rows: Row[] = [];
    for (let i = 0; i < 10; i++) {
      rows.push({
        d: randomFuzz(),
        e: randomFuzz(),
        f: {
          a: randomFuzz(),
          b: randomFuzz(),
          c: randomFuzz(),
          d: randomFuzz(),
          e: randomFuzz(),
        },
        j: randomFuzz(),
      });
    }

    // Can't do an inline snapshot with random test inputs, but the
    // testCopy() function itself does a lot of testing as well.
    await testCopy(...rows);
  });

  test('data types', async () => {
    expect(
      await testCopy({
        a: 123n,
        b: 456,
        c: [789, 10, 11, 12],
        d: 'foo-bar',
        f: {json: 'object'},
        g: {jsonb: ['object', 'yo']},
        h: 0,
        i: ['array', 'of', 'strings'],
        j: '\\.',
      }),
    ).toMatchInlineSnapshot(`
      [
        "123",
        "456",
        "{789,10,11,12}",
        "foo-bar",
        null,
        "{"json":"object"}",
        "{"jsonb": ["object", "yo"]}",
        "0",
        "{array,of,strings}",
        "\\.",
      ]
    `);
  });

  test('multiple rows, lots of data', {timeout: 30_000}, async () => {
    const rows: Row[] = Array.from({length: 50}, (_, i) => ({
      a: BigInt(i),
      b: i * 100,
      c: [i, i + 1, i + 2],
      d: `hello\t${i}`,
      e: `${i}\nworld`,
      f: {[`field${i}`]: `value${i}`},
      g: null,
      h: 1000 - i,
      i: [`${i}`, `${i + 1}`],
      j: 'abc'.repeat(i),
    }));
    const output = await testCopy(...rows);

    // Sampled sanity check
    expect([
      output.slice(0, 10),
      output.slice(130, 140),
      output.slice(330, 340),
      output.slice(490, 500),
    ]).toMatchInlineSnapshot(`
      [
        [
          "0",
          "0",
          "{0,1,2}",
          "hello	0",
          "0
      world",
          "{"field0":"value0"}",
          null,
          "1000",
          "{0,1}",
          "",
        ],
        [
          "13",
          "1300",
          "{13,14,15}",
          "hello	13",
          "13
      world",
          "{"field13":"value13"}",
          null,
          "987",
          "{13,14}",
          "abcabcabcabcabcabcabcabcabcabcabcabcabc",
        ],
        [
          "33",
          "3300",
          "{33,34,35}",
          "hello	33",
          "33
      world",
          "{"field33":"value33"}",
          null,
          "967",
          "{33,34}",
          "abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc",
        ],
        [
          "49",
          "4900",
          "{49,50,51}",
          "hello	49",
          "49
      world",
          "{"field49":"value49"}",
          null,
          "951",
          "{49,50}",
          "abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc",
        ],
      ]
    `);
  });
});
