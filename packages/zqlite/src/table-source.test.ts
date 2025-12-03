import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import type {JSONValue} from '../../shared/src/json.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {Row, Value} from '../../zero-protocol/src/data.ts';
import {Catch} from '../../zql/src/ivm/catch.ts';
import type {Change} from '../../zql/src/ivm/change.ts';
import {makeComparator} from '../../zql/src/ivm/data.ts';
import {Database} from './db.ts';
import {format} from './internal/sql.ts';
import {
  fromSQLiteTypes,
  TableSource,
  UnsupportedValueError,
} from './table-source.ts';
import {filtersToSQL} from './query-builder.ts';
import {assert} from '../../shared/src/asserts.ts';
import {consume} from '../../zql/src/ivm/stream.ts';

const columns = {
  id: {type: 'string'},
  a: {type: 'number'},
  b: {type: 'number'},
  c: {type: 'number'},
} as const;

const lc = createSilentLogContext();

describe('fetching from a table source', () => {
  type Foo = {id: string; a: number; b: number; c: number};
  const allRows: Foo[] = [];
  const compoundOrder = [
    ['a', 'asc'],
    ['b', 'desc'],
    ['c', 'asc'],
    ['id', 'asc'],
  ] as const;
  const compoundComparator = makeComparator(compoundOrder);
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(
    /* sql */ `CREATE TABLE foo (id TEXT PRIMARY KEY, a, b, c, ignored, columns);`,
  );
  const stmt = db.prepare(
    /* sql */ `INSERT INTO foo (id, a, b, c) VALUES (?, ?, ?, ?);`,
  );
  let id = 0;
  for (let a = 1; a <= 3; ++a) {
    for (let b = 1; b <= 3; ++b) {
      for (let c = 1; c <= 3; ++c) {
        const row = [(++id).toString().padStart(2, '0'), a, b, c] as const;
        allRows.push({
          id: row[0],
          a: row[1],
          b: row[2],
          c: row[3],
        });
        stmt.run(...row);
      }
    }
  }

  test.each([
    {
      name: 'simple source with `id` order',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {constraint: undefined, start: undefined},
      expectedRows: allRows,
    },
    {
      name: 'simple source with `id` order and constraint',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {constraint: {a: 2}, start: undefined},
      expectedRows: allRows.filter(r => r.a === 2),
    },
    {
      name: 'simple source with `id` order and start `after`',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'after'},
      },
      expectedRows: allRows.slice(5),
    },
    {
      name: 'simple source with `id` order and start `after` and constraint',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {
        constraint: {b: 2},
        start: {row: allRows[4], basis: 'after'},
      },
      expectedRows: allRows.slice(5).filter(r => r.b === 2),
    },
    {
      name: 'simple source with `id` order and start `at`',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'at'},
      },
      expectedRows: allRows.slice(4),
    },
    {
      name: 'simple source with `id` order and start `at` and constraint',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {
        constraint: {b: 2},
        start: {row: allRows[4], basis: 'at'},
      },
      expectedRows: allRows.slice(4).filter(r => r.b === 2),
    },
    {
      name: 'complex source with compound order',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {constraint: undefined, start: undefined},
      expectedRows: allRows.slice().sort(compoundComparator),
    },
    {
      name: 'complex source with compound order and constraint',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {constraint: {a: 2}, start: undefined},
      expectedRows: allRows.filter(r => r.a === 2).sort(compoundComparator),
    },
    {
      name: 'complex source with compound order and start `after`',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'after'},
      },
      expectedRows: allRows.slice().sort(compoundComparator).slice(5),
    },
    {
      name: 'complex source with compound order and start `after` and constraint',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {
        constraint: {b: 2},
        start: {row: allRows[4], basis: 'after'},
      },
      expectedRows: allRows
        .slice(5)
        .filter(r => r.b === 2)
        .sort(compoundComparator),
    },
    {
      name: 'complex source with compound order and start `at`',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'at'},
      },
      expectedRows: allRows.slice().sort(compoundComparator).slice(4),
    },
    {
      name: 'complex source with compound order and start `at` and constraint',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {
        constraint: {b: 2},
        start: {row: allRows[4], basis: 'at'},
      },
      expectedRows: allRows
        .slice(4)
        .filter(r => r.b === 2)
        .sort(compoundComparator),
    },

    {
      name: 'with compound key constraint',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {constraint: {a: 1, b: 2}, start: undefined},
      expectedRows: allRows.filter(r => r.a === 1 && r.b === 2),
    },
    {
      name: 'with compound key constraint (order should not matter)',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {constraint: {b: 2, a: 1}, start: undefined},
      expectedRows: allRows.filter(r => r.a === 1 && r.b === 2),
    },
  ] as const)('$name', ({sourceArgs, fetchArgs, expectedRows}) => {
    const source = new TableSource(
      lc,
      testLogConfig,
      db,
      sourceArgs[0],
      sourceArgs[1],
      ['id'],
    );
    const c = source.connect(sourceArgs[2]);
    const out = new Catch(c);
    c.setOutput(out);
    const rows = out.fetch(fetchArgs);
    expect(
      rows.map(r => {
        assert(r !== 'yield');
        return r.row;
      }),
    ).toEqual(expectedRows);
  });
});

describe('fetched value types', () => {
  type Foo = {
    id: string;
    a: number | null;
    b: number | null;
    c: boolean | null;
    d: JSONValue | null;
  };
  const columns = {
    id: {type: 'string'},
    a: {type: 'number'},
    b: {type: 'number'},
    c: {type: 'boolean'},
    d: {type: 'json'},
  } as const;

  type Case = {
    name: string;
    input: unknown[];
    output?: Foo;
  };

  const cases: Case[] = [
    {
      name: 'nulls',
      input: ['0', null, null, null, null],
      output: {id: '0', a: null, b: null, c: null, d: null},
    },
    {
      name: 'number, float, false boolean, json string',
      input: ['1', 1, 2.123, 0, '"json string"'],
      output: {id: '1', a: 1, b: 2.123, c: false, d: 'json string'},
    },
    {
      name: 'bigint, float, true boolean, json null',
      input: ['2', 2n, 3.456, 1n, 'null'],
      output: {id: '2', a: 2, b: 3.456, c: true, d: null},
    },
    {
      name: 'bigint, float, true boolean, json object',
      input: ['2', 2n, 3.456, 1n, '{}'],
      output: {id: '2', a: 2, b: 3.456, c: true, d: {}},
    },
    {
      name: 'bigint, float, true boolean, json array',
      input: ['2', 2n, 3.456, 1n, '[]'],
      output: {id: '2', a: 2, b: 3.456, c: true, d: []},
    },
    {
      name: 'safe integer boundaries',
      input: [
        '3',
        BigInt(Number.MAX_SAFE_INTEGER),
        BigInt(Number.MIN_SAFE_INTEGER),
        1,
        'true',
      ],
      output: {
        id: '3',
        a: 9007199254740991,
        b: -9007199254740991,
        c: true,
        d: true,
      },
    },
    {
      name: 'bigint too big',
      input: ['3', BigInt(Number.MAX_SAFE_INTEGER) + 1n, 0, 1n, '{}'],
    },
    {
      name: 'bigint too small',
      input: ['3', BigInt(Number.MIN_SAFE_INTEGER) - 1n, 0, 1n, '{}'],
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      const db = new Database(createSilentLogContext(), ':memory:');
      db.exec(
        /* sql */ `CREATE TABLE foo (id TEXT PRIMARY KEY, a, b, c, d, ignored, columns);`,
      );
      const stmt = db.prepare(
        /* sql */ `INSERT INTO foo (id, a, b, c, d) VALUES (?, ?, ?, ?, ?);`,
      );
      stmt.run(c.input);
      const source = new TableSource(lc, testLogConfig, db, 'foo', columns, [
        'id',
      ]);
      const input = source.connect([['id', 'asc']]);

      if (c.output) {
        expect(
          [...input.fetch({})].map(node =>
            node === 'yield' ? node : node.row,
          ),
        ).toEqual([c.output]);
      } else {
        expect(() => [...input.fetch({})]).toThrow(UnsupportedValueError);
      }
    });
  }
});

test('pushing values does the correct writes and outputs', () => {
  const db1 = new Database(createSilentLogContext(), ':memory:');
  const db2 = new Database(createSilentLogContext(), ':memory:');
  db1.exec(
    /* sql */ `CREATE TABLE foo (a, b, c, d, ignored, columns, PRIMARY KEY (a, b));`,
  );
  db2.exec(
    /* sql */ `CREATE TABLE foo (a, b, c, d, ignored, columns, PRIMARY KEY (a, b));`,
  );
  const source = new TableSource(
    lc,
    testLogConfig,
    db1,
    'foo',
    {
      a: {type: 'number'},
      b: {type: 'number'},
      c: {type: 'boolean'},
      d: {type: 'json'},
    },
    ['a', 'b'],
  );
  const outputted: Change[] = [];
  source
    .connect([
      ['a', 'asc'],
      ['b', 'asc'],
    ])
    .setOutput({
      push: function* (change) {
        outputted.push(change);
      },
    });

  for (const db of [db1, db2]) {
    const read = db.prepare('SELECT a, b, c, d FROM foo');
    source.setDB(db);

    /**
     * Test:
     * 1. add a row
     * 2. remove a row
     * 3. remove a row that doesn't exist throws
     * 4. add a row that already exists throws
     */
    consume(
      source.push({
        type: 'add',
        row: {a: 1, b: 2.123, c: false, d: 'json string'},
      }),
    );

    expect(outputted.shift()).toEqual({
      type: 'add',
      node: {
        relationships: {},
        row: {
          a: 1,
          b: 2.123,
          c: false,
          d: 'json string',
        },
      },
    });
    expect(read.all()).toEqual([{a: 1, b: 2.123, c: 0, d: '"json string"'}]);

    consume(
      source.push({
        type: 'remove',
        row: {a: 1, b: 2.123},
      }),
    );

    expect(outputted.shift()).toEqual({
      type: 'remove',
      node: {
        relationships: {},
        row: {
          a: 1,
          b: 2.123,
        },
      },
    });
    expect(read.all()).toEqual([]);

    expect(() => {
      consume(
        source.push({
          type: 'remove',
          row: {a: 1, b: 2.123},
        }),
      );
    }).toThrow();
    expect(read.all()).toEqual([]);

    consume(
      source.push({
        type: 'add',
        row: {a: 1, b: 2.123, c: true, d: {}},
      }),
    );

    expect(outputted.shift()).toEqual({
      type: 'add',
      node: {
        relationships: {},
        row: {
          a: 1,
          b: 2.123,
          c: true,
          d: {},
        },
      },
    });
    expect(read.all()).toEqual([{a: 1, b: 2.123, c: 1, d: '{}'}]);

    expect(() => {
      consume(
        source.push({
          type: 'add',
          row: {a: 1, b: 2.123, c: true, d: null},
        }),
      );
    }).toThrow();

    // bigint rows
    consume(
      source.push({
        type: 'add',
        row: {
          a: BigInt(Number.MAX_SAFE_INTEGER),
          b: 3.456,
          c: true,
          d: [],
        } as unknown as Row,
      }),
    );

    expect(outputted.shift()).toEqual({
      type: 'add',
      node: {
        relationships: {},
        row: {
          a: 9007199254740991n,
          b: 3.456,
          c: true,
          d: [],
        },
      },
    });

    expect(read.all()).toEqual([
      {a: 1, b: 2.123, c: 1, d: '{}'},
      {a: 9007199254740991, b: 3.456, c: 1, d: '[]'},
    ]);

    consume(
      source.push({
        type: 'add',
        row: {
          a: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
          b: 0,
          c: true,
          d: true,
        } as unknown as Row,
      }),
    );
    outputted.shift();

    consume(
      source.push({
        type: 'add',
        row: {
          a: 0,
          b: BigInt(Number.MIN_SAFE_INTEGER) - 1n,
          c: true,
          d: false,
        } as unknown as Row,
      }),
    );
    outputted.shift();

    read.safeIntegers(true);
    expect(read.all()).toEqual([
      {a: 1, b: 2.123, c: 1, d: '{}'},
      {a: 9007199254740991n, b: 3.456, c: 1, d: '[]'},
      {
        a: 9007199254740992n,
        b: 0,
        c: 1,
        d: 'true',
      },
      {
        a: 0,
        b: -9007199254740992n,
        c: 1,
        d: 'false',
      },
    ]);
    read.safeIntegers(false);

    consume(
      source.push({
        type: 'remove',
        row: {
          a: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
          b: 0,
          c: true,
        } as unknown as Row,
      }),
    );
    outputted.shift();

    consume(
      source.push({
        type: 'remove',
        row: {
          a: 0,
          b: BigInt(Number.MIN_SAFE_INTEGER) - 1n,
          c: true,
        } as unknown as Row,
      }),
    );
    outputted.shift();

    // edit changes
    consume(
      source.push({
        type: 'edit',
        row: {a: 1, b: 2.123, c: false, d: {a: true}} as unknown as Row,
        oldRow: {a: 1, b: 2.123, c: true, d: {}} as unknown as Row,
      }),
    );

    expect(outputted.shift()).toEqual({
      type: 'edit',
      oldNode: {row: {a: 1, b: 2.123, c: true, d: {}}, relationships: {}},
      node: {row: {a: 1, b: 2.123, c: false, d: {a: true}}, relationships: {}},
    });

    expect(read.all()).toEqual([
      {a: 1, b: 2.123, c: 0, d: '{"a":true}'},
      {a: 9007199254740991, b: 3.456, c: 1, d: '[]'},
    ]);

    // edit pk should fall back to remove and insert
    consume(
      source.push({
        type: 'edit',
        oldRow: {a: 1, b: 2.123, c: false, d: {a: true}},
        row: {a: 1, b: 3, c: false, d: {a: true}},
      }),
    );
    expect(outputted.shift()).toEqual({
      type: 'edit',
      oldNode: {
        row: {a: 1, b: 2.123, c: false, d: {a: true}},
        relationships: {},
      },
      node: {row: {a: 1, b: 3, c: false, d: {a: true}}, relationships: {}},
    });
    expect(read.all()).toEqual([
      {a: 9007199254740991, b: 3.456, c: 1, d: '[]'},
      {a: 1, b: 3, c: 0, d: '{"a":true}'},
    ]);

    // non existing old row
    expect(() => {
      consume(
        source.push({
          type: 'edit',
          row: {a: 11, b: 2.123, c: 0},
          oldRow: {a: 12, b: 2.123, c: 1},
        }),
      );
    }).toThrow('Row not found');
  }
});

test('getByKey', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(
    /* sql */ `CREATE TABLE foo (id TEXT, a INTEGER, b, c, ignored, columns, PRIMARY KEY(id, a));`,
  );
  const stmt = db.prepare(
    /* sql */ `INSERT INTO foo (id, a, b, c) VALUES (?, ?, ?, ?);`,
  );
  stmt.run('1', 2, 3.123, 0);
  stmt.run('2', 3n, 4.567, 1);
  stmt.run(
    '3',
    BigInt(Number.MAX_SAFE_INTEGER),
    BigInt(Number.MIN_SAFE_INTEGER),
    1,
  );
  stmt.run(
    '4',
    BigInt(Number.MAX_SAFE_INTEGER) + 1n,
    BigInt(Number.MIN_SAFE_INTEGER),
    1,
  );

  const source = new TableSource(
    lc,
    testLogConfig,
    db,
    'foo',
    {
      id: {type: 'string'},
      a: {type: 'number'},
      b: {type: 'number'},
      c: {type: 'boolean'},
    },
    ['id', 'a'],
  );

  expect(source.getRow({id: '1', a: 2})).toEqual({
    id: '1',
    a: 2,
    b: 3.123,
    c: false,
  });

  expect(source.getRow({id: '2', a: 3})).toEqual({
    id: '2',
    a: 3,
    b: 4.567,
    c: true,
  });

  expect(source.getRow({id: '3', a: Number.MAX_SAFE_INTEGER})).toEqual({
    id: '3',
    a: Number.MAX_SAFE_INTEGER,
    b: Number.MIN_SAFE_INTEGER,
    c: true,
  });

  // Ensure that it works with any unique key
  expect(source.getRow({id: '3'})).toEqual({
    id: '3',
    a: Number.MAX_SAFE_INTEGER,
    b: Number.MIN_SAFE_INTEGER,
    c: true,
  });

  // Exists but contains an out-of-bounds value.
  expect(() =>
    source.getRow({
      id: '4',
      a: (BigInt(Number.MAX_SAFE_INTEGER) + 1n) as unknown as Value,
    }),
  ).toThrow(UnsupportedValueError);

  // Does not exist.
  expect(
    source.getRow({
      id: '5',
      a: (BigInt(Number.MAX_SAFE_INTEGER) + 1n) as unknown as Value,
    }),
  ).toBeUndefined();
});

describe('optional filters to sql', () => {
  test('simple condition', () => {
    expect(
      format(
        filtersToSQL({
          type: 'simple',
          left: {type: 'column', name: 'a'},
          op: '=',
          right: {type: 'literal', value: 1},
        }),
      ).text,
    ).toEqual('"a" = ?');
  });
  test('anded conditions', () => {
    expect(
      format(
        filtersToSQL({
          type: 'and',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'a'},
              op: '=',
              right: {type: 'literal', value: 1},
            },
            {
              type: 'simple',
              left: {type: 'column', name: 'b'},
              op: '=',
              right: {type: 'literal', value: 2},
            },
          ],
        }),
      ).text,
    ).toEqual('("a" = ? AND "b" = ?)');
  });
  test('ored conditions', () => {
    expect(
      format(
        filtersToSQL({
          type: 'or',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'a'},
              op: '=',
              right: {type: 'literal', value: 1},
            },
            {
              type: 'simple',
              left: {type: 'column', name: 'b'},
              op: '=',
              right: {type: 'literal', value: 2},
            },
          ],
        }),
      ).text,
    ).toEqual('("a" = ? OR "b" = ?)');
  });
  test('dnf conditions', () => {
    expect(
      format(
        filtersToSQL({
          type: 'or',
          conditions: [
            {
              type: 'and',
              conditions: [
                {
                  type: 'simple',
                  left: {type: 'column', name: 'a'},
                  op: '=',
                  right: {type: 'literal', value: 1},
                },
                {
                  type: 'simple',
                  left: {type: 'column', name: 'b'},
                  op: '=',
                  right: {type: 'literal', value: 2},
                },
              ],
            },
            {
              type: 'and',
              conditions: [
                {
                  type: 'simple',
                  left: {type: 'column', name: 'a'},
                  op: '=',
                  right: {type: 'literal', value: 3},
                },
                {
                  type: 'simple',
                  left: {type: 'column', name: 'b'},
                  op: '=',
                  right: {type: 'literal', value: 4},
                },
              ],
            },
          ],
        }),
      ).text,
    ).toEqual('(("a" = ? AND "b" = ?) OR ("a" = ? AND "b" = ?))');
  });
  test('literal conditions', () => {
    expect(
      format(
        filtersToSQL({
          type: 'simple',
          left: {
            type: 'literal',
            value: 'a',
          },
          op: '=',
          right: {
            type: 'literal',
            value: 'b',
          },
        }),
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "? = ?",
        "values": [
          "a",
          "b",
        ],
      }
    `);
    expect(
      format(
        filtersToSQL({
          type: 'simple',
          left: {
            type: 'literal',
            value: 1,
          },
          op: '=',
          right: {
            type: 'literal',
            value: 2,
          },
        }),
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "? = ?",
        "values": [
          1,
          2,
        ],
      }
    `);
    expect(
      format(
        filtersToSQL({
          type: 'simple',
          left: {
            type: 'literal',
            value: true,
          },
          op: '=',
          right: {
            type: 'literal',
            value: false,
          },
        }),
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "? = ?",
        "values": [
          1,
          0,
        ],
      }
    `);
    expect(
      format(
        filtersToSQL({
          type: 'simple',
          left: {type: 'literal', value: 1},
          op: '=',
          right: {type: 'literal', value: [1, 2, 3]},
        }),
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "? = ?",
        "values": [
          1,
          "[1,2,3]",
        ],
      }
    `);
  });
});

// TODO: Add constraint test with compound keys

describe('fromSQLiteTypes error messages', () => {
  test('invalid column error includes table name', () => {
    const valueTypes = {
      id: {type: 'string'},
      name: {type: 'string'},
    } as const;

    const row = {
      id: '123',
      name: 'test',
      invalidColumn: 'oops',
    };

    expect(() => fromSQLiteTypes(valueTypes, row, 'users')).toThrow(
      'Invalid column "invalidColumn" for table "users". Synced columns include id, name',
    );
  });

  test('bigint out of range error includes table name and column', () => {
    const db = new Database(createSilentLogContext(), ':memory:');
    db.exec(
      /* sql */ `CREATE TABLE test_table (id TEXT PRIMARY KEY, big_value INTEGER);`,
    );
    db.exec(
      /* sql */ `INSERT INTO test_table (id, big_value) VALUES ('1', ${
        BigInt(Number.MAX_SAFE_INTEGER) + 1n
      });`,
    );

    const source = new TableSource(
      lc,
      testLogConfig,
      db,
      'test_table',
      {
        id: {type: 'string'},
        big_value: {type: 'number'},
      },
      ['id'],
    );
    const input = source.connect([['id', 'asc']]);

    expect(() => [...input.fetch({})]).toThrow(
      /value .* \(in test_table\.big_value\) is outside of supported bounds/,
    );
  });

  test('invalid JSON error includes table name and column', () => {
    const db = new Database(createSilentLogContext(), ':memory:');
    db.exec(
      /* sql */ `CREATE TABLE test_table (id TEXT PRIMARY KEY, json_data TEXT);`,
    );
    db.exec(
      /* sql */ `INSERT INTO test_table (id, json_data) VALUES ('1', 'invalid json {');`,
    );

    const source = new TableSource(
      lc,
      testLogConfig,
      db,
      'test_table',
      {
        id: {type: 'string'},
        json_data: {type: 'json'},
      },
      ['id'],
    );
    const input = source.connect([['id', 'asc']]);

    expect(() => [...input.fetch({})]).toThrow(
      /Failed to parse JSON for test_table\.json_data/,
    );
  });

  test('error cause is preserved for JSON parse errors', () => {
    const db = new Database(createSilentLogContext(), ':memory:');
    db.exec(
      /* sql */ `CREATE TABLE test_table (id TEXT PRIMARY KEY, json_data TEXT);`,
    );
    db.exec(
      /* sql */ `INSERT INTO test_table (id, json_data) VALUES ('1', 'not valid json');`,
    );

    const source = new TableSource(
      lc,
      testLogConfig,
      db,
      'test_table',
      {
        id: {type: 'string'},
        json_data: {type: 'json'},
      },
      ['id'],
    );
    const input = source.connect([['id', 'asc']]);

    let caughtError: unknown;
    try {
      for (const _ of input.fetch({})) {
        // Consume the iterator to trigger the error
      }
    } catch (error) {
      caughtError = error;
    }

    expect(caughtError).toBeInstanceOf(UnsupportedValueError);
    expect((caughtError as Error).message).toMatchInlineSnapshot(
      `"Failed to parse JSON for test_table.json_data: Unexpected token 'o', "not valid json" is not valid JSON"`,
    );
    expect((caughtError as Error).cause).toMatchInlineSnapshot(
      `[SyntaxError: Unexpected token 'o', "not valid json" is not valid JSON]`,
    );
  });
});
