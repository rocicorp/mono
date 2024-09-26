import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {describe, expect, test} from 'vitest';
import {Catch} from 'zql/src/zql/ivm/catch.js';
import type {Change} from 'zql/src/zql/ivm/change.js';
import {makeComparator, type Row, type Value} from 'zql/src/zql/ivm/data.js';
import type {SchemaValue} from 'zql/src/zql/ivm/schema.js';
import {runCases} from 'zql/src/zql/ivm/test/source-cases.js';
import {Database} from 'zqlite/src/db.js';
import {compile, sql} from './internal/sql.js';
import {TableSource, UnsupportedValueError} from './table-source.js';

const columns = {
  id: {type: 'string'},
  a: {type: 'number'},
  b: {type: 'number'},
  c: {type: 'number'},
} as const;

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
  db.exec(/* sql */ `CREATE TABLE foo (id TEXT PRIMARY KEY, a, b, c);`);
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
      fetchArgs: {constraint: {key: 'a', value: 2}, start: undefined},
      expectedRows: allRows.filter(r => r.a === 2),
    },
    {
      name: 'simple source with `id` order and start `before`',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'before'},
      },
      expectedRows: allRows.slice(3),
    },
    {
      name: 'simple source with `id` order and start `before` and constraint',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {
        constraint: {key: 'b', value: 2},
        start: {row: allRows[4], basis: 'before'},
      },
      expectedRows: allRows.slice(3).filter(r => r.b === 2),
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
        constraint: {key: 'b', value: 2},
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
        constraint: {key: 'b', value: 2},
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
      fetchArgs: {constraint: {key: 'a', value: 2}, start: undefined},
      expectedRows: allRows.filter(r => r.a === 2).sort(compoundComparator),
    },
    {
      name: 'complex source with compound order and start `before`',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'before'},
      },
      expectedRows: allRows.slice().sort(compoundComparator).slice(3),
    },
    {
      name: 'complex source with compound order and start `before` and constraint',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {
        constraint: {key: 'b', value: 2},
        start: {row: allRows[4], basis: 'before'},
      },
      expectedRows: allRows
        .slice(3)
        .filter(r => r.b === 2)
        .sort(compoundComparator),
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
        constraint: {key: 'b', value: 2},
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
        constraint: {key: 'b', value: 2},
        start: {row: allRows[4], basis: 'at'},
      },
      expectedRows: allRows
        .slice(4)
        .filter(r => r.b === 2)
        .sort(compoundComparator),
    },
  ] as const)('$name', ({sourceArgs, fetchArgs, expectedRows}) => {
    const source = new TableSource(db, sourceArgs[0], sourceArgs[1], ['id']);
    const c = source.connect(sourceArgs[2]);
    const out = new Catch(c);
    c.setOutput(out);
    const rows = out.fetch(fetchArgs);
    expect(rows.map(r => r.row)).toEqual(expectedRows);
  });
});

describe('fetched value types', () => {
  type Foo = {id: string; a: number; b: number; c: boolean};
  const columns = {
    id: {type: 'string'},
    a: {type: 'number'},
    b: {type: 'number'},
    c: {type: 'boolean'},
  } as const;

  type Case = {
    name: string;
    input: unknown[];
    output?: Foo;
  };

  const cases: Case[] = [
    {
      name: 'number, float and false boolean',
      input: ['1', 1, 2.123, 0],
      output: {id: '1', a: 1, b: 2.123, c: false},
    },
    {
      name: 'bigint, float, and true boolean',
      input: ['2', 2n, 3.456, 1n],
      output: {id: '2', a: 2, b: 3.456, c: true},
    },
    {
      name: 'safe integer boundaries',
      input: [
        '3',
        BigInt(Number.MAX_SAFE_INTEGER),
        BigInt(Number.MIN_SAFE_INTEGER),
        1n,
      ],
      output: {id: '3', a: 9007199254740991, b: -9007199254740991, c: true},
    },
    {
      name: 'bigint too big',
      input: ['3', BigInt(Number.MAX_SAFE_INTEGER) + 1n, 0, 1n],
    },
    {
      name: 'bigint too small',
      input: ['3', BigInt(Number.MIN_SAFE_INTEGER) - 1n, 0, 1n],
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      const db = new Database(createSilentLogContext(), ':memory:');
      db.exec(/* sql */ `CREATE TABLE foo (id TEXT PRIMARY KEY, a, b, c);`);
      const stmt = db.prepare(
        /* sql */ `INSERT INTO foo (id, a, b, c) VALUES (?, ?, ?, ?);`,
      );
      stmt.run(c.input);
      const source = new TableSource(db, 'foo', columns, ['id']);
      const input = source.connect([['id', 'asc']]);

      if (c.output) {
        expect([...input.fetch({})].map(node => node.row)).toEqual([c.output]);
      } else {
        expect(() => [...input.fetch({})]).toThrow(UnsupportedValueError);
      }
    });
  }
});

test('pushing values does the correct writes and outputs', () => {
  const db1 = new Database(createSilentLogContext(), ':memory:');
  const db2 = new Database(createSilentLogContext(), ':memory:');
  db1.exec(/* sql */ `CREATE TABLE foo (a, b, c, PRIMARY KEY (a, b));`);
  db2.exec(/* sql */ `CREATE TABLE foo (a, b, c, PRIMARY KEY (a, b));`);
  const source = new TableSource(
    db1,
    'foo',
    {a: {type: 'number'}, b: {type: 'number'}, c: {type: 'boolean'}},
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
    const read = db.prepare('SELECT * FROM foo');
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
      row: {a: 1, b: 2.123, c: 0},
    });

    expect(outputted.shift()).toEqual({
      type: 'add',
      node: {
        relationships: {},
        row: {
          a: 1,
          b: 2.123,
          c: false,
        },
      },
    });
    expect(read.all()).toEqual([{a: 1, b: 2.123, c: 0}]);

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
      row: {a: 1, b: 2.123, c: 1},
    });

    expect(outputted.shift()).toEqual({
      type: 'add',
      node: {
        relationships: {},
        row: {
          a: 1,
          b: 2.123,
          c: true,
        },
      },
    });
    expect(read.all()).toEqual([{a: 1, b: 2.123, c: 1}]);

    expect(() => {
      source.push({
        type: 'add',
        row: {a: 1, b: 2.123, c: 3},
      });
    }).toThrow();

    // bigint rows
    source.push({
      type: 'add',
      row: {
        a: BigInt(Number.MAX_SAFE_INTEGER),
        b: 3.456,
        c: 1,
      } as unknown as Row,
    });

    expect(outputted.shift()).toEqual({
      type: 'add',
      node: {
        relationships: {},
        row: {
          a: 9007199254740991,
          b: 3.456,
          c: true,
        },
      },
    });

    expect(read.all()).toEqual([
      {a: 1, b: 2.123, c: 1},
      {a: 9007199254740991, b: 3.456, c: 1},
    ]);

    // out of bounds
    expect(() => {
      source.push({
        type: 'add',
        row: {
          a: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
          b: 0,
          c: 1,
        } as unknown as Row,
      });
    }).toThrow(UnsupportedValueError);

    // out of bounds
    expect(() => {
      source.push({
        type: 'add',
        row: {
          a: 0,
          b: BigInt(Number.MIN_SAFE_INTEGER) - 1n,
          c: 1,
        } as unknown as Row,
      });
    }).toThrow(UnsupportedValueError);

    expect(read.all()).toEqual([
      {a: 1, b: 2.123, c: 1},
      {a: 9007199254740991, b: 3.456, c: 1},
    ]);

    // edit changes
    source.push({
      type: 'edit',
      row: {a: BigInt(1), b: 2.123, c: false} as unknown as Row,
      oldRow: {a: BigInt(1), b: 2.123, c: true} as unknown as Row,
    });

    expect(outputted.shift()).toEqual({
      type: 'edit',
      oldRow: {a: 1, b: 2.123, c: true},
      row: {a: 1, b: 2.123, c: false},
    });

    expect(read.all()).toEqual([
      {a: 1, b: 2.123, c: 0},
      {a: 9007199254740991, b: 3.456, c: 1},
    ]);

    // edit pk should fall back to remove and insert
    source.push({
      type: 'edit',
      oldRow: {a: 1, b: 2.123, c: false},
      row: {a: 1, b: 3, c: false},
    });
    expect(outputted.shift()).toEqual({
      type: 'edit',
      oldRow: {a: 1, b: 2.123, c: false},
      row: {a: 1, b: 3, c: false},
    });
    expect(read.all()).toEqual([
      {a: 9007199254740991, b: 3.456, c: 1},
      {a: 1, b: 3, c: 0},
    ]);

    // non existing old row
    expect(() => {
      source.push({
        type: 'edit',
        row: {a: 11, b: 2.123, c: false},
        oldRow: {a: 12, b: 2.123, c: true},
      });
    }).toThrow('Row not found');

    // out of bounds
    expect(() => {
      source.push({
        type: 'edit',
        row: {
          a: BigInt(Number.MAX_SAFE_INTEGER),
          b: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
          c: 1,
        } as unknown as Row,
        oldRow: {
          a: BigInt(Number.MAX_SAFE_INTEGER),
          b: 3.456,
          c: true,
        } as unknown as Row,
      });
    }).toThrow(UnsupportedValueError);
  }
});

test('getByKey', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(
    /* sql */ `CREATE TABLE foo (id TEXT, a INTEGER, b, c, PRIMARY KEY(id, a));`,
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

describe('shared test cases', () => {
  runCases(
    (
      name: string,
      columns: Record<string, SchemaValue>,
      primaryKey: readonly [string, ...string[]],
    ) => {
      const db = new Database(createSilentLogContext(), ':memory:');
      // create a table with desired columns and primary keys
      const query = compile(
        sql`CREATE TABLE ${sql.ident(name)} (${sql.join(
          Object.keys(columns).map(c => sql.ident(c)),
          sql`, `,
        )}, PRIMARY KEY (${sql.join(
          primaryKey.map(p => sql.ident(p)),
          sql`, `,
        )}));`,
      );
      db.exec(query);
      return new TableSource(db, name, columns, primaryKey);
    },
    new Set(),
    new Set(),
  );
});
