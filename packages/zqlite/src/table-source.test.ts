import {describe, expect, test} from 'vitest';
import type {JSONValue} from '../../shared/src/json.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {Ordering} from '../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../zql/src/ivm/data.ts';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';
import {Catch} from '../../zql/src/ivm/catch.ts';
import type {Change} from '../../zql/src/ivm/change.ts';
import {makeComparator} from '../../zql/src/ivm/data.ts';
import {Database} from './db.ts';
import {format} from './internal/sql.ts';
import {
  filtersToSQL,
  TableSource,
  UnsupportedValueError,
} from './table-source.ts';
import {testLogConfig} from '../../otel/src/test-log-config.ts';

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
      'table-source.test.ts',
      db,
      sourceArgs[0],
      sourceArgs[1],
      ['id'],
    );
    const c = source.connect(sourceArgs[2]);
    const out = new Catch(c);
    c.setOutput(out);
    const rows = out.fetch(fetchArgs);
    expect(rows.map(r => r.row)).toEqual(expectedRows);
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
      const source = new TableSource(
        lc,
        testLogConfig,
        'table-source.test.ts',
        db,
        'foo',
        columns,
        ['id'],
      );
      const input = source.connect([['id', 'asc']]);

      if (c.output) {
        expect([...input.fetch({})].map(node => node.row)).toEqual([c.output]);
      } else {
        expect(() => [...input.fetch({})]).toThrow(UnsupportedValueError);
      }
    });
  }
});

describe('no primary key', () => {
  type Foo = {id: string; a: number; b: number; c: number};
  const columns = {
    id: {type: 'string'},
    a: {type: 'number'},
    b: {type: 'number'},
    c: {type: 'number'},
  } as const;

  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(/* sql */ `
        CREATE TABLE foo (id TEXT, a INT, b INT, c INT);
        CREATE UNIQUE INDEX foo_id_key ON foo (id);
        CREATE UNIQUE INDEX foo_ab_key ON foo (a, b);
        CREATE INDEX foo_c_not_unique ON foo (c);
        `);
  const stmt = db.prepare(
    /* sql */ `INSERT INTO foo (id, a, b, c) VALUES (?, ?, ?, ?);`,
  );
  stmt.run(['far', 234, 567, 333]);
  stmt.run(['boo', 345, 112, 444]);
  stmt.run(['foo', 345, 789, 555]);
  const source = new TableSource(
    lc,
    testLogConfig,
    'table-source.test.ts',
    db,
    'foo',
    columns,
    ['id'],
  );

  test.each([
    [['id'], true],
    [['a', 'b'], true],
    [['b', 'a'], true],
    [['a'], false],
    [['b'], false],
    [['c'], false],
    [['b', 'c'], false],
    [['a', 'c'], false],
  ] satisfies [PrimaryKey, boolean][])(
    'requires primary key to be uniquely indexed: %o',
    (key, valid) => {
      const createSource = () =>
        new TableSource(
          lc,
          testLogConfig,
          'table-source.test.ts',
          db,
          'foo',
          columns,
          key,
        );
      if (valid) {
        createSource();
      } else {
        expect(createSource).toThrowError('does not have a UNIQUE index');
      }
    },
  );

  test.each([
    [[['a', 'asc']]],
    [[['b', 'asc']]],
    [[['c', 'asc']]],
    [
      [
        ['b', 'asc'],
        ['c', 'desc'],
      ],
    ],
  ] satisfies [Ordering][])('disallows non-unique orderings: %o', sort => {
    expect(() => source.connect(sort)).toThrowError('Cannot orderBy(');
  });

  test.each([
    [
      [['id', 'asc']],
      [
        {id: 'boo', a: 345, b: 112, c: 444},
        {id: 'far', a: 234, b: 567, c: 333},
        {id: 'foo', a: 345, b: 789, c: 555},
      ],
    ],
    [
      [
        ['a', 'asc'],
        ['b', 'desc'],
      ],
      [
        {id: 'far', a: 234, b: 567, c: 333},
        {id: 'foo', a: 345, b: 789, c: 555},
        {id: 'boo', a: 345, b: 112, c: 444},
      ],
    ],
    [
      [
        ['c', 'asc'],
        ['a', 'asc'],
        ['b', 'desc'],
      ],
      [
        {id: 'far', a: 234, b: 567, c: 333},
        {id: 'boo', a: 345, b: 112, c: 444},
        {id: 'foo', a: 345, b: 789, c: 555},
      ],
    ],
    [
      [
        ['c', 'desc'],
        ['id', 'asc'],
      ],
      [
        {id: 'foo', a: 345, b: 789, c: 555},
        {id: 'boo', a: 345, b: 112, c: 444},
        {id: 'far', a: 234, b: 567, c: 333},
      ],
    ],
  ] satisfies [Ordering, Foo[]][])(
    'allows orderings with unique indexes: %o',
    (sort, output) => {
      const input = source.connect(sort);
      const schema = input.getSchema();
      expect(schema.sort.map(([col]) => col)).toEqual(
        expect.arrayContaining([...schema.primaryKey]),
      );
      expect([...input.fetch({})].map(node => node.row)).toEqual(output);
    },
  );
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
    'table-source.test.ts',
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
      push: change => outputted.push(change),
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
    source.push({
      type: 'add',
      row: {a: 1, b: 2.123, c: false, d: 'json string'},
    });

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

    source.push({
      type: 'remove',
      row: {a: 1, b: 2.123},
    });

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
      source.push({
        type: 'remove',
        row: {a: 1, b: 2.123},
      });
    }).toThrow();
    expect(read.all()).toEqual([]);

    source.push({
      type: 'add',
      row: {a: 1, b: 2.123, c: true, d: {}},
    });

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
      source.push({
        type: 'add',
        row: {a: 1, b: 2.123, c: true, d: null},
      });
    }).toThrow();

    // bigint rows
    source.push({
      type: 'add',
      row: {
        a: BigInt(Number.MAX_SAFE_INTEGER),
        b: 3.456,
        c: true,
        d: [],
      } as unknown as Row,
    });

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

    source.push({
      type: 'add',
      row: {
        a: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
        b: 0,
        c: true,
        d: true,
      } as unknown as Row,
    });
    outputted.shift();

    source.push({
      type: 'add',
      row: {
        a: 0,
        b: BigInt(Number.MIN_SAFE_INTEGER) - 1n,
        c: true,
        d: false,
      } as unknown as Row,
    });
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

    source.push({
      type: 'remove',
      row: {
        a: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
        b: 0,
        c: true,
      } as unknown as Row,
    });
    outputted.shift();

    source.push({
      type: 'remove',
      row: {
        a: 0,
        b: BigInt(Number.MIN_SAFE_INTEGER) - 1n,
        c: true,
      } as unknown as Row,
    });
    outputted.shift();

    // edit changes
    source.push({
      type: 'edit',
      row: {a: 1, b: 2.123, c: false, d: {a: true}} as unknown as Row,
      oldRow: {a: 1, b: 2.123, c: true, d: {}} as unknown as Row,
    });

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
    source.push({
      type: 'edit',
      oldRow: {a: 1, b: 2.123, c: false, d: {a: true}},
      row: {a: 1, b: 3, c: false, d: {a: true}},
    });
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
      source.push({
        type: 'edit',
        row: {a: 11, b: 2.123, c: 0},
        oldRow: {a: 12, b: 2.123, c: 1},
      });
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
    'table-source.test.ts',
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
  ).toBeUndefined;
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
