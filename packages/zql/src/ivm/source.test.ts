import {expect, suite, test} from 'vitest';
import type {
  Condition,
  SimpleOperator,
} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import {Catch, expandNode, type CaughtNode} from './catch.ts';
import type {Constraint} from './constraint.ts';
import type {FetchRequest, Input, Output, Start} from './operator.ts';
import type {SourceChange} from './source.ts';
import {createSource} from './test/source-factory.ts';
import {consume} from './stream.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';

const lc = createSilentLogContext();

function asNodes(rows: Row[]) {
  return rows.map(row => ({
    row,
    relationships: {},
  }));
}

function asChanges(sc: SourceChange[]) {
  return sc.map(c => ({
    type: c.type,
    node: {
      row: c.row,
      relationships: {},
    },
  }));
}

class OverlaySpy implements Output {
  #input: Input;
  fetches: CaughtNode[][] = [];

  onPush = () => {};

  constructor(input: Input) {
    this.#input = input;
    input.setOutput(this);
  }

  fetch(req: FetchRequest) {
    this.fetches.push([...this.#input.fetch(req)].map(expandNode));
  }

  push() {
    this.onPush();
    return [];
  }
}

test('simple-fetch', () => {
  const sort = [['a', 'asc']] as const;
  const s = createSource(lc, testLogConfig, 'table', {a: {type: 'number'}}, [
    'a',
  ]);
  const out = new Catch(s.connect(sort));
  expect(out.fetch()).toEqual([]);

  consume(s.push({type: 'add', row: {a: 3}}));
  expect(out.fetch()).toEqual(asNodes([{a: 3}]));

  consume(s.push({type: 'add', row: {a: 1}}));
  consume(s.push({type: 'add', row: {a: 2}}));
  expect(out.fetch()).toEqual(asNodes([{a: 1}, {a: 2}, {a: 3}]));

  consume(s.push({type: 'remove', row: {a: 1}}));
  expect(out.fetch()).toEqual(asNodes([{a: 2}, {a: 3}]));

  consume(s.push({type: 'remove', row: {a: 2}}));
  consume(s.push({type: 'remove', row: {a: 3}}));
  expect(out.fetch()).toEqual([]);
});

test('sort missing primary key columns', () => {
  const s = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}, c: {type: 'string'}},
    ['a'],
  );
  expect(() => s.connect([])).toThrowErrorMatchingInlineSnapshot(
    `[Error: Ordering must include all primary key fields. Missing: a.]`,
  );
  expect(() => s.connect([['b', 'asc']])).toThrowErrorMatchingInlineSnapshot(
    `[Error: Ordering must include all primary key fields. Missing: a.]`,
  );
  expect(() =>
    s.connect([
      ['b', 'asc'],
      ['c', 'desc'],
    ]),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Ordering must include all primary key fields. Missing: a.]`,
  );
});

test('fetch-with-constraint', () => {
  const sort = [['a', 'asc']] as const;
  const s = createSource(
    lc,
    testLogConfig,
    'table',
    {
      a: {type: 'number'},
      b: {type: 'boolean'},
      c: {type: 'number'},
      d: {type: 'string'},
    },
    ['a'],
  );
  const out = new Catch(s.connect(sort));
  consume(s.push({type: 'add', row: {a: 3, b: true, c: 1, d: null}}));
  consume(s.push({type: 'add', row: {a: 1, b: true, c: 2, d: null}}));
  consume(s.push({type: 'add', row: {a: 2, b: false, c: null, d: null}}));

  expect(out.fetch({constraint: {b: true}})).toEqual(
    asNodes([
      {a: 1, b: true, c: 2, d: null},
      {a: 3, b: true, c: 1, d: null},
    ]),
  );

  expect(out.fetch({constraint: {b: false}})).toEqual(
    asNodes([{a: 2, b: false, c: null, d: null}]),
  );

  expect(out.fetch({constraint: {c: 1}})).toEqual(
    asNodes([{a: 3, b: true, c: 1, d: null}]),
  );

  expect(out.fetch({constraint: {c: 0}})).toEqual(asNodes([]));

  // Constraints are used to implement joins and so should use join
  // semantics for equality. null !== null.
  expect(out.fetch({constraint: {c: null}})).toEqual(asNodes([]));
  expect(out.fetch({constraint: {c: undefined}})).toEqual(asNodes([]));

  // Not really a feature, but because of loose typing of joins and how we
  // accept undefined we can't really tell when constraining on a field that
  // doesn't exist.
  expect(out.fetch({constraint: {d: null}})).toEqual(asNodes([]));
  expect(out.fetch({constraint: {d: undefined}})).toEqual(asNodes([]));

  expect(out.fetch({constraint: {b: true, c: 1}})).toEqual(
    asNodes([{a: 3, b: true, c: 1, d: null}]),
  );
  expect(out.fetch({constraint: {b: true, c: 2}})).toEqual(
    asNodes([{a: 1, b: true, c: 2, d: null}]),
  );

  // nulls are not equal to each other
  expect(out.fetch({constraint: {b: true, d: null}})).toEqual([]);
});

test('fetch-start', () => {
  const sort = [['a', 'asc']] as const;
  const s = createSource(
    lc,
    testLogConfig,
    'table',
    {
      a: {type: 'number'},
    },
    ['a'],
  );
  const out = new Catch(s.connect(sort));

  expect(out.fetch({start: {row: {a: 2}, basis: 'at'}})).toEqual(asNodes([]));
  expect(out.fetch({start: {row: {a: 2}, basis: 'after'}})).toEqual(
    asNodes([]),
  );

  consume(s.push({type: 'add', row: {a: 2}}));
  consume(s.push({type: 'add', row: {a: 3}}));
  expect(out.fetch({start: {row: {a: 2}, basis: 'at'}})).toEqual(
    asNodes([{a: 2}, {a: 3}]),
  );
  expect(out.fetch({start: {row: {a: 2}, basis: 'after'}})).toEqual(
    asNodes([{a: 3}]),
  );

  expect(out.fetch({start: {row: {a: 3}, basis: 'at'}})).toEqual(
    asNodes([{a: 3}]),
  );
  expect(out.fetch({start: {row: {a: 3}, basis: 'after'}})).toEqual(
    asNodes([]),
  );

  consume(s.push({type: 'remove', row: {a: 3}}));
  expect(out.fetch({start: {row: {a: 2}, basis: 'at'}})).toEqual(
    asNodes([{a: 2}]),
  );
  expect(out.fetch({start: {row: {a: 2}, basis: 'after'}})).toEqual(
    asNodes([]),
  );
});

test('fetch-start reverse', () => {
  const sort = [['a', 'asc']] as const;
  const s = createSource(
    lc,
    testLogConfig,
    'table',
    {
      a: {type: 'number'},
    },
    ['a'],
  );
  const out = new Catch(s.connect(sort));

  expect(out.fetch({start: {row: {a: 2}, basis: 'at'}, reverse: true})).toEqual(
    asNodes([]),
  );
  expect(
    out.fetch({start: {row: {a: 2}, basis: 'after'}, reverse: true}),
  ).toEqual(asNodes([]));

  consume(s.push({type: 'add', row: {a: 2}}));
  consume(s.push({type: 'add', row: {a: 3}}));
  expect(out.fetch({start: {row: {a: 2}, basis: 'at'}, reverse: true})).toEqual(
    asNodes([{a: 2}]),
  );
  expect(
    out.fetch({start: {row: {a: 2}, basis: 'after'}, reverse: true}),
  ).toEqual(asNodes([]));

  expect(out.fetch({start: {row: {a: 3}, basis: 'at'}, reverse: true})).toEqual(
    asNodes([{a: 3}, {a: 2}]),
  );
  expect(
    out.fetch({start: {row: {a: 3}, basis: 'after'}, reverse: true}),
  ).toEqual(asNodes([{a: 2}]));

  consume(s.push({type: 'remove', row: {a: 3}}));
  expect(out.fetch({start: {row: {a: 2}, basis: 'at'}, reverse: true})).toEqual(
    asNodes([{a: 2}]),
  );
  expect(
    out.fetch({start: {row: {a: 2}, basis: 'after'}, reverse: true}),
  ).toEqual(asNodes([]));
});

suite('fetch-with-constraint-and-start', () => {
  function t(c: {
    columns?: Record<string, SchemaValue> | undefined;
    startData: Row[];
    start: Start;
    constraint: Constraint;
    reverse?: boolean | undefined;
  }) {
    const sort = [['a', 'asc']] as const;
    const s = createSource(
      lc,
      testLogConfig,
      'table',
      c.columns ?? {
        a: {type: 'number'},
        b: {type: 'boolean'},
      },
      ['a'],
    );
    for (const row of c.startData) {
      consume(s.push({type: 'add', row}));
    }
    const out = new Catch(s.connect(sort));
    return out.fetch({
      constraint: c.constraint,
      start: c.start,
      reverse: c.reverse,
    });
  }

  test('c1', () => {
    expect(
      t({
        columns: {
          a: {type: 'number'},
          b: {type: 'string'},
        },
        startData: [
          {a: 1, b: '1000'},
          {a: 2, b: '3000'},
          {a: 3, b: '2000'},
          {a: 5, b: '1000'},
          {a: 6, b: '4000'},
          {a: 7, b: '0000'},
        ],
        start: {
          row: {a: 3, b: '2000'},
          basis: 'at',
        },
        constraint: {b: '1000'},
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "a": 5,
            "b": "1000",
          },
        },
      ]
    `);
  });

  test('c1 reverse', () => {
    expect(
      t({
        columns: {
          a: {type: 'number'},
          b: {type: 'string'},
        },
        startData: [
          {a: 1, b: '1000'},
          {a: 2, b: '3000'},
          {a: 3, b: '2000'},
          {a: 5, b: '1000'},
          {a: 6, b: '4000'},
          {a: 7, b: '0000'},
        ],
        start: {
          row: {a: 3, b: '2000'},
          basis: 'at',
        },
        constraint: {b: '1000'},
        reverse: true,
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "a": 1,
            "b": "1000",
          },
        },
      ]
    `);
  });

  test('c2', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 3, b: false},
          {a: 5, b: true},
          {a: 6, b: false},
          {a: 7, b: false},
        ],
        start: {
          row: {a: 6, b: false},
          basis: 'at',
        },
        constraint: {b: false},
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "a": 6,
            "b": false,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 7,
            "b": false,
          },
        },
      ]
    `);
  });

  test('c2 reverse', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 3, b: false},
          {a: 5, b: true},
          {a: 6, b: false},
          {a: 7, b: false},
        ],
        start: {
          row: {a: 6, b: false},
          basis: 'at',
        },
        constraint: {b: false},
        reverse: true,
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "a": 6,
            "b": false,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": false,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": false,
          },
        },
      ]
    `);
  });

  test('c3', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 3, b: false},
          {a: 5, b: true},
          {a: 6, b: false},
          {a: 7, b: false},
          {a: 8, b: true},
          {a: 9, b: false},
        ],
        start: {
          row: {a: 6, b: false},
          basis: 'after',
        },
        constraint: {b: false},
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "a": 7,
            "b": false,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 9,
            "b": false,
          },
        },
      ]
    `);
  });

  test('c3 reverse', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 3, b: false},
          {a: 5, b: true},
          {a: 6, b: false},
          {a: 7, b: false},
          {a: 8, b: true},
          {a: 9, b: false},
        ],
        start: {
          row: {a: 6, b: false},
          basis: 'after',
        },
        constraint: {b: false},
        reverse: true,
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": false,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": false,
          },
        },
      ]
    `);
  });

  test('c4', () => {
    expect(
      t({
        columns: {
          a: {type: 'number'},
          b: {type: 'boolean'},
          c: {type: 'number'},
        },
        startData: [
          {a: 2, b: false, c: 2},
          {a: 3, b: false, c: 1},
          {a: 5, b: true, c: 2},
          {a: 6, b: false, c: 1},
          {a: 7, b: false, c: 2},
          {a: 8, b: true, c: 1},
          {a: 9, b: false, c: 2},
        ],
        start: {
          row: {a: 6, b: false, c: 1},
          basis: 'at',
        },
        constraint: {b: false, c: 1},
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "a": 6,
            "b": false,
            "c": 1,
          },
        },
      ]
    `);
  });

  test('c4 reverse', () => {
    expect(
      t({
        columns: {
          a: {type: 'number'},
          b: {type: 'boolean'},
          c: {type: 'number'},
        },
        startData: [
          {a: 2, b: false, c: 2},
          {a: 3, b: false, c: 1},
          {a: 5, b: true, c: 2},
          {a: 6, b: false, c: 1},
          {a: 7, b: false, c: 2},
          {a: 8, b: true, c: 1},
          {a: 9, b: false, c: 2},
        ],
        start: {
          row: {a: 6, b: false, c: 1},
          basis: 'at',
        },
        constraint: {b: false, c: 1},
        reverse: true,
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "a": 6,
            "b": false,
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": false,
            "c": 1,
          },
        },
      ]
    `);
  });
});

test('push', () => {
  const sort = [['a', 'asc']] as const;
  const s = createSource(lc, testLogConfig, 'table', {a: {type: 'number'}}, [
    'a',
  ]);
  const out = new Catch(s.connect(sort));

  expect(out.pushes).toEqual([]);

  consume(s.push({type: 'add', row: {a: 2}}));
  expect(out.pushes).toEqual(asChanges([{type: 'add', row: {a: 2}}]));

  consume(s.push({type: 'add', row: {a: 1}}));
  expect(out.pushes).toEqual(
    asChanges([
      {type: 'add', row: {a: 2}},
      {type: 'add', row: {a: 1}},
    ]),
  );

  consume(s.push({type: 'remove', row: {a: 1}}));
  consume(s.push({type: 'remove', row: {a: 2}}));
  expect(out.pushes).toEqual(
    asChanges([
      {type: 'add', row: {a: 2}},
      {type: 'add', row: {a: 1}},
      {type: 'remove', row: {a: 1}},
      {type: 'remove', row: {a: 2}},
    ]),
  );

  // Remove row that isn't there
  out.reset();
  expect(() => consume(s.push({type: 'remove', row: {a: 1}}))).toThrow(
    'Row not found',
  );
  expect(out.pushes).toEqual(asChanges([]));

  // Add row twice
  consume(s.push({type: 'add', row: {a: 1}}));
  expect(out.pushes).toEqual(asChanges([{type: 'add', row: {a: 1}}]));
  expect(() => consume(s.push({type: 'add', row: {a: 1}}))).toThrow(
    'Row already exists',
  );
  expect(out.pushes).toEqual(asChanges([{type: 'add', row: {a: 1}}]));
});

test('overlay-source-isolation', () => {
  // Ok this is a little tricky. We are trying to show that overlays
  // only show up for one output at a time. But because calling outputs
  // is synchronous with push, it's a little tough to catch (especially
  // without involving joins, which is the reason we care about this).
  // To do so, we arrange for each output to call fetch when any of the
  // *other* outputs are pushed to. Then we can observe that the overlay
  // only shows up in the cases it is supposed to.

  const sort = [['a', 'asc']] as const;
  const s = createSource(lc, testLogConfig, 'table', {a: {type: 'number'}}, [
    'a',
  ]);
  const o1 = new OverlaySpy(s.connect(sort));
  const o2 = new OverlaySpy(s.connect(sort));
  const o3 = new OverlaySpy(s.connect(sort));

  function fetchAll() {
    o1.fetch({});
    o2.fetch({});
    o3.fetch({});
  }

  o1.onPush = fetchAll;
  o2.onPush = fetchAll;
  o3.onPush = fetchAll;

  consume(s.push({type: 'add', row: {a: 2}}));
  expect(o1.fetches).toEqual([
    asNodes([{a: 2}]),
    asNodes([{a: 2}]),
    asNodes([{a: 2}]),
  ]);
  expect(o2.fetches).toEqual([[], asNodes([{a: 2}]), asNodes([{a: 2}])]);
  expect(o3.fetches).toEqual([[], [], asNodes([{a: 2}])]);
});

test('overlay-source-isolation with split edit', () => {
  // Ok this is a little tricky. We are trying to show that overlays
  // only show up for one output at a time. But because calling outputs
  // is synchronous with push, it's a little tough to catch (especially
  // without involving joins, which is the reason we care about this).
  // To do so, we arrange for each output to call fetch when any of the
  // *other* outputs are pushed to. Then we can observe that the overlay
  // only shows up in the cases it is supposed to.

  const sort = [['a', 'asc']] as const;
  const s = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}, c: {type: 'number'}},
    ['a'],
  );
  const o1 = new OverlaySpy(s.connect(sort));
  const o2 = new OverlaySpy(s.connect(sort, undefined, new Set(['b', 'c'])));
  const o3 = new OverlaySpy(s.connect(sort, undefined, new Set(['c'])));

  function fetchAll() {
    o1.fetch({});
    o2.fetch({});
    o3.fetch({});
  }

  function clearAll() {
    o1.fetches.length = 0;
    o2.fetches.length = 0;
    o3.fetches.length = 0;
  }

  o1.onPush = fetchAll;
  o2.onPush = fetchAll;
  o3.onPush = fetchAll;

  consume(s.push({type: 'add', row: {a: 2, b: 'foo', c: 1}}));
  consume(s.push({type: 'add', row: {a: 3, b: 'bar', c: 1}}));

  clearAll();

  consume(
    s.push({
      type: 'edit',
      oldRow: {a: 2, b: 'foo', c: 1},
      row: {a: 2, b: 'foo2', c: 1},
    }),
  );

  // o2 needs edit split, edit is split for all connections,
  // so 2 pushes each
  expect(o1.fetches.length).toEqual(6);
  expect(o2.fetches.length).toEqual(6);
  expect(o3.fetches.length).toEqual(6);

  expect(o1.fetches).toMatchInlineSnapshot(`
    [
      [
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
    ]
  `);

  expect(o2.fetches).toMatchInlineSnapshot(`
    [
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
    ]
  `);

  expect(o3.fetches).toMatchInlineSnapshot(`
    [
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
    ]
  `);

  clearAll();

  consume(
    s.push({
      type: 'edit',
      oldRow: {a: 3, b: 'bar', c: 1},
      row: {a: 3, b: 'bar', c: 2},
    }),
  );

  // o2 and o3 need edit split, edit is split for all connections,
  // so 2 pushes each
  expect(o1.fetches.length).toEqual(6);
  expect(o2.fetches.length).toEqual(6);
  expect(o3.fetches.length).toEqual(6);

  expect(o1.fetches).toMatchInlineSnapshot(`
    [
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 2,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 2,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 2,
          },
        },
      ],
    ]
  `);
  expect(o2.fetches).toMatchInlineSnapshot(`
    [
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 2,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 2,
          },
        },
      ],
    ]
  `);
  expect(o3.fetches).toMatchInlineSnapshot(`
    [
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
      ],
      [
        {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo2",
            "c": 1,
          },
        },
        {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "bar",
            "c": 2,
          },
        },
      ],
    ]
  `);
});

suite('overlay-vs-fetch-start', () => {
  function t(c: {
    startData: Row[];
    start: Start;
    reverse?: boolean | undefined;
    change: SourceChange;
  }) {
    const sort = [['a', 'asc']] as const;
    const s = createSource(lc, testLogConfig, 'table', {a: {type: 'number'}}, [
      'a',
    ]);
    for (const row of c.startData) {
      consume(s.push({type: 'add', row}));
    }
    const out = new OverlaySpy(s.connect(sort));
    out.onPush = () =>
      out.fetch({
        start: c.start,
        reverse: c.reverse,
      });
    try {
      consume(s.push(c.change));
    } catch (e) {
      return {
        e: (e as Error).message,
      };
    }
    return out.fetches;
  }

  test('c9', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'at',
        },
        change: {type: 'add', row: {a: 1}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 2,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 4,
            },
          },
        ],
      ]
    `);
  });

  test('c9 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'at',
        },
        reverse: true,
        change: {type: 'add', row: {a: 1}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 2,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 1,
            },
          },
        ],
      ]
    `);
  });

  test('c10', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'at',
        },
        change: {type: 'add', row: {a: 3}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 2,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 3,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 4,
            },
          },
        ],
      ]
    `);
  });

  test('c10 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'at',
        },
        reverse: true,
        change: {type: 'add', row: {a: 3}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 2,
            },
          },
        ],
      ]
    `);
  });

  test('c11', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'at',
        },
        change: {type: 'add', row: {a: 5}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 2,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 4,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 5,
            },
          },
        ],
      ]
    `);
  });

  test('c11 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'at',
        },
        reverse: true,
        change: {type: 'add', row: {a: 5}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 2,
            },
          },
        ],
      ]
    `);
  });

  test('c12', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'after',
        },
        change: {type: 'add', row: {a: 1}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 4,
            },
          },
        ],
      ]
    `);
  });

  test('c12 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'after',
        },
        reverse: true,
        change: {type: 'add', row: {a: 1}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 1,
            },
          },
        ],
      ]
    `);
  });

  test('c13', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'after',
        },
        change: {type: 'add', row: {a: 3}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 3,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 4,
            },
          },
        ],
      ]
    `);
  });

  test('c13 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'after',
        },
        reverse: true,
        change: {type: 'add', row: {a: 3}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [],
      ]
    `);
  });

  test('c14', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'after',
        },
        change: {type: 'add', row: {a: 5}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 4,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 5,
            },
          },
        ],
      ]
    `);
  });

  test('c14 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'after',
        },
        reverse: true,
        change: {type: 'add', row: {a: 5}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [],
      ]
    `);
  });

  test('c15', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 4},
          basis: 'after',
        },
        change: {type: 'add', row: {a: 3}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [],
      ]
    `);
  });

  test('c15 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 4},
          basis: 'after',
        },
        reverse: true,
        change: {type: 'add', row: {a: 3}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 3,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 2,
            },
          },
        ],
      ]
    `);
  });

  test('c16', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 4},
          basis: 'after',
        },
        change: {type: 'add', row: {a: 5}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 5,
            },
          },
        ],
      ]
    `);
  });

  test('c16 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 4},
          basis: 'after',
        },
        reverse: true,
        change: {type: 'add', row: {a: 5}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 2,
            },
          },
        ],
      ]
    `);
  });

  test('c23', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'at',
        },
        change: {type: 'remove', row: {a: 2}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 4,
            },
          },
        ],
      ]
    `);
  });

  test('c23 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'at',
        },
        reverse: true,
        change: {type: 'remove', row: {a: 2}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [],
      ]
    `);
  });

  test('c24', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'at',
        },
        change: {type: 'remove', row: {a: 4}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 2,
            },
          },
        ],
      ]
    `);
  });

  test('c24 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'at',
        },
        reverse: true,
        change: {type: 'remove', row: {a: 4}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 2,
            },
          },
        ],
      ]
    `);
  });

  test('c25', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 4},
          basis: 'at',
        },
        change: {type: 'remove', row: {a: 2}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 4,
            },
          },
        ],
      ]
    `);
  });

  test('c25 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 4},
          basis: 'at',
        },
        reverse: true,
        change: {type: 'remove', row: {a: 2}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 4,
            },
          },
        ],
      ]
    `);
  });

  test('c26', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 4},
          basis: 'at',
        },
        change: {type: 'remove', row: {a: 4}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [],
      ]
    `);
  });

  test('c26 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 4},
          basis: 'at',
        },
        reverse: true,
        change: {type: 'remove', row: {a: 4}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 2,
            },
          },
        ],
      ]
    `);
  });

  test('c27', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'after',
        },
        change: {type: 'remove', row: {a: 2}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 4,
            },
          },
        ],
      ]
    `);
  });

  test('c27 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'after',
        },
        reverse: true,
        change: {type: 'remove', row: {a: 2}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [],
      ]
    `);
  });

  test('c28', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 2},
          basis: 'after',
        },
        change: {type: 'remove', row: {a: 4}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [],
      ]
    `);
  });

  test('c29', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 4},
          basis: 'after',
        },
        change: {type: 'remove', row: {a: 2}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [],
      ]
    `);
  });

  test('c29 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 4},
          basis: 'after',
        },
        change: {type: 'remove', row: {a: 2}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [],
      ]
    `);
  });

  test('c30', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 4},
          basis: 'after',
        },
        change: {type: 'remove', row: {a: 4}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [],
      ]
    `);
  });

  test('c30 reverse', () => {
    expect(
      t({
        startData: [{a: 2}, {a: 4}],
        start: {
          row: {a: 4},
          basis: 'after',
        },
        reverse: true,
        change: {type: 'remove', row: {a: 4}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 2,
            },
          },
        ],
      ]
    `);
  });
});

suite('overlay-vs-constraint', () => {
  function t(c: {
    startData: Row[];
    constraint: Constraint;
    change: SourceChange;
  }) {
    const sort = [['a', 'asc']] as const;
    const s = createSource(
      lc,
      testLogConfig,
      'table',
      {
        a: {type: 'number'},
        b: {type: 'boolean'},
      },
      ['a'],
    );
    for (const row of c.startData) {
      consume(s.push({type: 'add', row}));
    }
    const out = new OverlaySpy(s.connect(sort));
    out.onPush = () =>
      out.fetch({
        constraint: c.constraint,
      });
    try {
      consume(s.push(c.change));
    } catch (e) {
      return {
        e: (e as Error).message,
      };
    }
    return out.fetches;
  }

  test('c1', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 4, b: true},
        ],
        constraint: {b: true},
        change: {type: 'add', row: {a: 1, b: true}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 1,
              "b": true,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 4,
              "b": true,
            },
          },
        ],
      ]
    `);
  });

  test('c2', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 4, b: true},
        ],
        constraint: {b: true},
        change: {type: 'add', row: {a: 1, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 4,
              "b": true,
            },
          },
        ],
      ]
    `);
  });

  test('c3', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 4, b: true},
          {a: 5, b: true},
        ],
        constraint: {b: true},
        change: {type: 'edit', oldRow: {a: 4, b: true}, row: {a: 4, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 5,
              "b": true,
            },
          },
        ],
      ]
    `);
  });

  test('c4', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 4, b: true},
          {a: 5, b: true},
        ],
        constraint: {b: false},
        change: {type: 'edit', oldRow: {a: 4, b: true}, row: {a: 4, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 2,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 4,
              "b": false,
            },
          },
        ],
      ]
    `);
  });

  test('c5', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 4, b: true},
          {a: 5, b: true},
        ],
        constraint: {a: 4, b: false},
        change: {type: 'edit', oldRow: {a: 4, b: true}, row: {a: 4, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 4,
              "b": false,
            },
          },
        ],
      ]
    `);
  });
});

suite('overlay-vs-filter', () => {
  function t(c: {startData: Row[]; filter: Condition; change: SourceChange}) {
    const sort = [
      ['a', 'asc'],
      ['b', 'asc'],
    ] as const;
    const s = createSource(
      lc,
      testLogConfig,
      'table',
      {
        a: {type: 'number'},
        b: {type: 'boolean'},
      },
      ['a', 'b'],
    );
    for (const row of c.startData) {
      consume(s.push({type: 'add', row}));
    }
    const sourceInput = s.connect(sort, c.filter);
    const out = new OverlaySpy(sourceInput);
    out.onPush = () => out.fetch({});
    try {
      consume(s.push(c.change));
    } catch (e) {
      return {
        e: (e as Error).message,
        fullyAppliedFilters: sourceInput.fullyAppliedFilters,
      };
    }
    return {
      fetches: out.fetches,
      fullyAppliedFilters: sourceInput.fullyAppliedFilters,
    };
  }

  test('c1', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 4, b: true},
        ],
        filter: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'b'},
          right: {type: 'literal', value: true},
        },
        change: {type: 'add', row: {a: 1, b: true}},
      }),
    ).toMatchInlineSnapshot(`
      {
        "fetches": [
          [
            {
              "relationships": {},
              "row": {
                "a": 1,
                "b": true,
              },
            },
            {
              "relationships": {},
              "row": {
                "a": 4,
                "b": true,
              },
            },
          ],
        ],
        "fullyAppliedFilters": true,
      }
    `);
  });

  test('c2', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 4, b: true},
        ],
        filter: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'b'},
          right: {type: 'literal', value: true},
        },
        change: {type: 'add', row: {a: 1, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      {
        "fetches": [],
        "fullyAppliedFilters": true,
      }
    `);
  });

  test('c3', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 4, b: true},
          {a: 5, b: true},
        ],
        filter: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'b'},
          right: {type: 'literal', value: true},
        },
        change: {type: 'edit', oldRow: {a: 4, b: true}, row: {a: 4, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      {
        "fetches": [
          [
            {
              "relationships": {},
              "row": {
                "a": 5,
                "b": true,
              },
            },
          ],
        ],
        "fullyAppliedFilters": true,
      }
    `);
  });

  test('c4', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 4, b: true},
          {a: 5, b: true},
        ],
        filter: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'b'},
          right: {type: 'literal', value: false},
        },
        change: {type: 'edit', oldRow: {a: 4, b: true}, row: {a: 4, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      {
        "fetches": [
          [
            {
              "relationships": {},
              "row": {
                "a": 2,
                "b": false,
              },
            },
            {
              "relationships": {},
              "row": {
                "a": 4,
                "b": false,
              },
            },
          ],
        ],
        "fullyAppliedFilters": true,
      }
    `);
  });

  test('c5', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 4, b: true},
        ],
        filter: {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'a'},
              right: {type: 'literal', value: 4},
            },
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'b'},
              right: {type: 'literal', value: false},
            },
          ],
        },
        change: {type: 'add', row: {a: 1, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      {
        "fetches": [
          [
            {
              "relationships": {},
              "row": {
                "a": 1,
                "b": false,
              },
            },
            {
              "relationships": {},
              "row": {
                "a": 2,
                "b": false,
              },
            },
            {
              "relationships": {},
              "row": {
                "a": 4,
                "b": true,
              },
            },
          ],
        ],
        "fullyAppliedFilters": true,
      }
    `);
  });

  test('c6', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 4, b: true},
        ],
        filter: {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'a'},
              right: {type: 'literal', value: 4},
            },
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'b'},
              right: {type: 'literal', value: false},
            },
          ],
        },
        change: {type: 'add', row: {a: 1, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      {
        "fetches": [
          [
            {
              "relationships": {},
              "row": {
                "a": 1,
                "b": false,
              },
            },
            {
              "relationships": {},
              "row": {
                "a": 2,
                "b": false,
              },
            },
            {
              "relationships": {},
              "row": {
                "a": 4,
                "b": true,
              },
            },
          ],
        ],
        "fullyAppliedFilters": true,
      }
    `);
  });
  test('c7', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 4, b: true},
        ],
        filter: {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'a'},
              right: {type: 'literal', value: 4},
            },
            {
              type: 'correlatedSubquery',
              related: {
                system: 'client',
                correlation: {
                  parentField: ['a'],
                  childField: ['b'],
                },
                subquery: {
                  table: 't',
                  alias: 'zsubq_ts',
                  orderBy: [['id', 'asc']],
                },
              },
              op: 'EXISTS',
            },
          ],
        },
        change: {type: 'add', row: {a: 1, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      {
        "fetches": [
          [
            {
              "relationships": {},
              "row": {
                "a": 1,
                "b": false,
              },
            },
            {
              "relationships": {},
              "row": {
                "a": 2,
                "b": false,
              },
            },
            {
              "relationships": {},
              "row": {
                "a": 4,
                "b": true,
              },
            },
          ],
        ],
        "fullyAppliedFilters": false,
      }
    `);
  });

  test('c8', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 4, b: true},
        ],
        filter: {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'a'},
              right: {type: 'literal', value: 4},
            },
            {
              type: 'correlatedSubquery',
              related: {
                system: 'client',
                correlation: {
                  parentField: ['a'],
                  childField: ['b'],
                },
                subquery: {
                  table: 't',
                  alias: 'zsubq_ts',
                  orderBy: [['id', 'asc']],
                },
              },
              op: 'EXISTS',
            },
          ],
        },
        change: {type: 'add', row: {a: 4, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      {
        "fetches": [
          [
            {
              "relationships": {},
              "row": {
                "a": 4,
                "b": false,
              },
            },
            {
              "relationships": {},
              "row": {
                "a": 4,
                "b": true,
              },
            },
          ],
        ],
        "fullyAppliedFilters": false,
      }
    `);
  });
});

suite('overlay-vs-constraint-and-start', () => {
  function t(c: {
    startData: Row[];
    columns?: Record<string, SchemaValue> | undefined;
    start: Start;
    reverse?: boolean | undefined;
    constraint: Constraint;
    change: SourceChange;
  }) {
    const sort = [['a', 'asc']] as const;
    const s = createSource(
      lc,
      testLogConfig,
      'table',
      c.columns ?? {
        a: {type: 'number'},
        b: {type: 'boolean'},
      },
      ['a'],
    );
    for (const row of c.startData) {
      consume(s.push({type: 'add', row}));
    }
    const out = new OverlaySpy(s.connect(sort));
    out.onPush = () =>
      out.fetch({
        start: c.start,
        constraint: c.constraint,
      });
    try {
      consume(s.push(c.change));
    } catch (e) {
      return {
        e: (e as Error).message,
      };
    }
    return out.fetches;
  }

  test('c3', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 3, b: false},
          {a: 5, b: true},
          {a: 6, b: false},
          {a: 7, b: false},
        ],
        start: {
          row: {a: 5.5, b: false},
          basis: 'at',
        },
        constraint: {b: false},
        change: {type: 'add', row: {a: 5.75, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 5.75,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 6,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 7,
              "b": false,
            },
          },
        ],
      ]
    `);
  });

  test('c3 reverse', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 3, b: false},
          {a: 5, b: true},
          {a: 6, b: false},
          {a: 7, b: false},
        ],
        start: {
          row: {a: 5.5, b: false},
          basis: 'at',
        },
        reverse: true,
        constraint: {b: false},
        change: {type: 'add', row: {a: 5.75, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 5.75,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 6,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 7,
              "b": false,
            },
          },
        ],
      ]
    `);
  });

  test('c4', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 3, b: false},
          {a: 5, b: true},
          {a: 6, b: false},
          {a: 7, b: false},
        ],
        start: {
          row: {a: 5.5, b: false},
          basis: 'at',
        },
        constraint: {b: false},
        change: {type: 'add', row: {a: 4, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 6,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 7,
              "b": false,
            },
          },
        ],
      ]
    `);
  });

  test('c4 reverse', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 3, b: false},
          {a: 5, b: true},
          {a: 6, b: false},
          {a: 7, b: false},
        ],
        start: {
          row: {a: 5.5, b: false},
          basis: 'at',
        },
        reverse: true,
        constraint: {b: false},
        change: {type: 'add', row: {a: 4, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 6,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 7,
              "b": false,
            },
          },
        ],
      ]
    `);
  });

  test('c5', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 3, b: false},
          {a: 5, b: true},
          {a: 6, b: false},
          {a: 7, b: false},
        ],
        start: {
          row: {a: 5.5, b: false},
          basis: 'at',
        },
        constraint: {b: false},
        change: {type: 'add', row: {a: 8, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 6,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 7,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 8,
              "b": false,
            },
          },
        ],
      ]
    `);
  });

  test('c5 reverse', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 3, b: false},
          {a: 5, b: true},
          {a: 6, b: false},
          {a: 7, b: false},
        ],
        start: {
          row: {a: 5.5, b: false},
          basis: 'at',
        },
        reverse: true,
        constraint: {b: false},
        change: {type: 'add', row: {a: 8, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 6,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 7,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 8,
              "b": false,
            },
          },
        ],
      ]
    `);
  });

  test('c6', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 3, b: false},
          {a: 5, b: true},
          {a: 6, b: false},
          {a: 7, b: false},
        ],
        start: {
          row: {a: 5.5, b: false},
          basis: 'after',
        },
        constraint: {b: false},
        change: {type: 'add', row: {a: 6.5, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 6,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 6.5,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 7,
              "b": false,
            },
          },
        ],
      ]
    `);
  });

  test('c6 reverse', () => {
    expect(
      t({
        startData: [
          {a: 2, b: false},
          {a: 3, b: false},
          {a: 5, b: true},
          {a: 6, b: false},
          {a: 7, b: false},
        ],
        start: {
          row: {a: 5.5, b: false},
          basis: 'after',
        },
        reverse: true,
        constraint: {b: false},
        change: {type: 'add', row: {a: 6.5, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 6,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 6.5,
              "b": false,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 7,
              "b": false,
            },
          },
        ],
      ]
    `);
  });

  test('c7', () => {
    expect(
      t({
        columns: {
          a: {type: 'number'},
          b: {type: 'boolean'},
          c: {type: 'number'},
        },
        startData: [
          {a: 2, b: false, c: 1},
          {a: 3, b: false, c: 1},
          {a: 5, b: true, c: 1},
          {a: 6, b: true, c: 2},
          {a: 7, b: false, c: 2},
        ],
        start: {
          row: {a: 5, b: true, c: 1},
          basis: 'at',
        },
        constraint: {b: true, c: 1},
        change: {type: 'add', row: {a: 5.5, b: true, c: 1}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 5,
              "b": true,
              "c": 1,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 5.5,
              "b": true,
              "c": 1,
            },
          },
        ],
      ]
    `);
  });

  test('c7 reverse', () => {
    expect(
      t({
        columns: {
          a: {type: 'number'},
          b: {type: 'boolean'},
          c: {type: 'number'},
        },
        startData: [
          {a: 2, b: false, c: 1},
          {a: 3, b: false, c: 1},
          {a: 5, b: true, c: 1},
          {a: 6, b: true, c: 2},
          {a: 7, b: false, c: 2},
        ],
        start: {
          row: {a: 5, b: true, c: 1},
          basis: 'at',
        },
        reverse: true,
        constraint: {b: true, c: 1},
        change: {type: 'add', row: {a: 5.5, b: true, c: 1}},
      }),
    ).toMatchInlineSnapshot(`
      [
        [
          {
            "relationships": {},
            "row": {
              "a": 5,
              "b": true,
              "c": 1,
            },
          },
          {
            "relationships": {},
            "row": {
              "a": 5.5,
              "b": true,
              "c": 1,
            },
          },
        ],
      ]
    `);
  });
});

test('per-output-sorts', () => {
  const sort1 = [['a', 'asc']] as const;
  const sort2 = [
    ['b', 'asc'],
    ['a', 'asc'],
  ] as const;
  const s = createSource(
    lc,
    testLogConfig,
    'table',
    {
      a: {type: 'number'},
      b: {type: 'number'},
    },
    ['a'],
  );
  const out1 = new Catch(s.connect(sort1));
  const out2 = new Catch(s.connect(sort2));

  consume(s.push({type: 'add', row: {a: 2, b: 3}}));
  consume(s.push({type: 'add', row: {a: 1, b: 2}}));
  consume(s.push({type: 'add', row: {a: 3, b: 1}}));

  expect(out1.fetch({})).toEqual(
    asNodes([
      {a: 1, b: 2},
      {a: 2, b: 3},
      {a: 3, b: 1},
    ]),
  );
  expect(out2.fetch({})).toEqual(
    asNodes([
      {a: 3, b: 1},
      {a: 1, b: 2},
      {a: 2, b: 3},
    ]),
  );
});

test('streams-are-one-time-only', () => {
  // It is very important that streas are one-time only. This is because on
  // the server, they are backed by cursors over streaming SQL queries which
  // can't be rewound or branched. This test ensures that streas from all
  // sources behave this way for consistency.
  const source = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}},
    ['a'],
  );
  consume(source.push({type: 'add', row: {a: 1}}));
  consume(source.push({type: 'add', row: {a: 2}}));
  consume(source.push({type: 'add', row: {a: 3}}));

  const conn = source.connect([['a', 'asc']]);
  const stream = conn.fetch({});
  const it1 = stream[Symbol.iterator]();
  const it2 = stream[Symbol.iterator]();
  expect(it1.next()).toEqual({
    done: false,
    value: {row: {a: 1}, relationships: {}},
  });
  expect(it2.next()).toEqual({
    done: false,
    value: {row: {a: 2}, relationships: {}},
  });
  expect(it1.next()).toEqual({
    done: false,
    value: {row: {a: 3}, relationships: {}},
  });
  expect(it2.next()).toEqual({done: true, value: undefined});
  expect(it1.next()).toEqual({done: true, value: undefined});

  const it3 = stream[Symbol.iterator]();
  expect(it3.next()).toEqual({done: true, value: undefined});
});

test('json is a valid type to read and write to/from a source', () => {
  const source = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, j: {type: 'json'}},
    ['a'],
  );

  consume(source.push({type: 'add', row: {a: 1, j: {foo: 'bar'}}}));
  consume(source.push({type: 'add', row: {a: 2, j: {baz: 'qux'}}}));
  consume(source.push({type: 'add', row: {a: 3, j: {foo: 'foo'}}}));

  const out = new Catch(source.connect([['a', 'asc']]));
  expect(out.fetch({})).toEqual(
    asNodes([
      {a: 1, j: {foo: 'bar'}},
      {a: 2, j: {baz: 'qux'}},
      {a: 3, j: {foo: 'foo'}},
    ]),
  );

  consume(source.push({type: 'add', row: {a: 4, j: {foo: 'foo'}}}));
  consume(source.push({type: 'add', row: {a: 5, j: {baz: 'qux'}}}));
  consume(source.push({type: 'add', row: {a: 6, j: {foo: 'bar'}}}));
  expect(out.pushes).toEqual([
    {
      type: 'add',
      node: {relationships: {}, row: {a: 4, j: {foo: 'foo'}}},
    },
    {
      type: 'add',
      node: {relationships: {}, row: {a: 5, j: {baz: 'qux'}}},
    },
    {
      type: 'add',
      node: {relationships: {}, row: {a: 6, j: {foo: 'bar'}}},
    },
  ]);

  // check edit and remove too
  out.reset();
  consume(
    source.push({
      type: 'edit',
      oldRow: {a: 5, j: {baz: 'qux'}},
      row: {a: 5, j: {baz: 'qux2'}},
    }),
  );
  consume(source.push({type: 'remove', row: {a: 5, j: {baz: 'qux'}}}));
  expect(out.pushes).toMatchInlineSnapshot(`
    [
      {
        "oldRow": {
          "a": 5,
          "j": {
            "baz": "qux",
          },
        },
        "row": {
          "a": 5,
          "j": {
            "baz": "qux2",
          },
        },
        "type": "edit",
      },
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 5,
            "j": {
              "baz": "qux",
            },
          },
        },
        "type": "remove",
      },
    ]
  `);
  expect(out.fetch({})).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": 1,
          "j": {
            "foo": "bar",
          },
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 2,
          "j": {
            "baz": "qux",
          },
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 3,
          "j": {
            "foo": "foo",
          },
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 4,
          "j": {
            "foo": "foo",
          },
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 6,
          "j": {
            "foo": "bar",
          },
        },
      },
    ]
  `);
});

test('IS and IS NOT comparisons against null', () => {
  const source = createSource(
    lc,
    testLogConfig,
    'table',
    {
      a: {type: 'number'},
      s: {type: 'string', optional: true},
    },
    ['a'],
  );

  consume(source.push({type: 'add', row: {a: 1, s: 'foo'}}));
  consume(source.push({type: 'add', row: {a: 2, s: 'bar'}}));
  consume(source.push({type: 'add', row: {a: 3, s: null}}));

  let out = new Catch(
    source.connect([['a', 'asc']], {
      type: 'simple',
      left: {
        type: 'column',
        name: 's',
      },
      op: 'IS',
      right: {
        type: 'literal',
        value: null,
      },
    }),
  );
  expect(out.fetch({})).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": 3,
          "s": null,
        },
      },
    ]
  `);

  // nothing `=` null
  out = new Catch(
    source.connect([['a', 'asc']], {
      type: 'simple',
      left: {
        type: 'column',
        name: 's',
      },
      op: '=',
      right: {
        type: 'literal',
        value: null,
      },
    }),
  );
  expect(out.fetch({})).toEqual([]);

  // nothing `!=` null
  out = new Catch(
    source.connect([['a', 'asc']], {
      type: 'simple',
      left: {
        type: 'column',
        name: 's',
      },
      op: '!=',
      right: {
        type: 'literal',
        value: null,
      },
    }),
  );
  expect(out.fetch({})).toEqual([]);

  // all non-nulls match `IS NOT NULL`
  out = new Catch(
    source.connect([['a', 'asc']], {
      type: 'simple',
      left: {
        type: 'column',
        name: 's',
      },
      op: 'IS NOT',
      right: {
        type: 'literal',
        value: null,
      },
    }),
  );
  expect(out.fetch({})).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": 1,
          "s": "foo",
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 2,
          "s": "bar",
        },
      },
    ]
  `);
});

// eslint-disable-next-line eslint-plugin-jest/expect-expect -- assertions in check() helper
test('constant/literal expression', () => {
  const source = createSource(
    lc,
    testLogConfig,
    'table',
    {n: {type: 'number'}, b: {type: 'boolean'}, s: {type: 'string'}},
    ['n'],
  );

  consume(source.push({type: 'add', row: {n: 1, b: true, s: 'foo'}}));
  consume(source.push({type: 'add', row: {n: 2, b: false, s: 'bar'}}));
  const allData = asNodes([
    {n: 1, b: true, s: 'foo'},
    {n: 2, b: false, s: 'bar'},
  ]);

  function check(
    leftValue: number | string | boolean,
    rightValue: number | string | boolean | number[] | boolean[] | string[],
    expected: ReturnType<typeof asNodes>,
    op: SimpleOperator = '=',
  ) {
    const out = new Catch(
      source.connect([['n', 'asc']], {
        type: 'simple',
        left: {
          type: 'literal',
          value: leftValue,
        },
        right: {
          type: 'literal',
          value: rightValue,
        },
        op,
      }),
    );
    expect(out.fetch({})).toEqual(expected);
  }

  check(1, 1, allData);
  check(1, 2, []);
  check(true, true, allData);
  check(true, false, []);
  check('foo', 'foo', allData);
  check('foo', 'bar', []);
  check(1, [1, 2, 3], allData, 'IN');
  check(1, [2, 4, 6], [], 'IN');
});

// Tests that verify the epoch-based overlay approach is semantically equivalent
// to the old index-based approach. The key invariant:
// When pushing to connection[i], connections[0..i] should see the overlay,
// while connections[i+1..n] should NOT see the overlay.
suite('epoch-based overlay semantic equivalence', () => {
  test('add: overlay visibility follows connection push order', () => {
    const sort = [['a', 'asc']] as const;
    const s = createSource(lc, testLogConfig, 'table', {a: {type: 'number'}}, [
      'a',
    ]);

    // Create 4 connections
    const spies = [
      new OverlaySpy(s.connect(sort)),
      new OverlaySpy(s.connect(sort)),
      new OverlaySpy(s.connect(sort)),
      new OverlaySpy(s.connect(sort)),
    ];

    // Track which rows each connection sees during each push phase
    const observations: [Row[][], Row[][], Row[][], Row[][]] = [[], [], [], []];

    // Each connection records what it sees when ANY connection is pushed to
    const recordAll = () => {
      for (let i = 0; i < observations.length; i++) {
        // Fetch from each spy and then figure out what each spy saw
        spies[i].fetch({});
        observations[i].push(
          spies[i].fetches[spies[i].fetches.length - 1].map(n =>
            n !== 'yield' ? n.row : {},
          ),
        );
      }
    };

    spies.forEach(spy => {
      spy.onPush = recordAll;
    });

    // Push an add - this will trigger 4 push phases (one per connection)
    consume(s.push({type: 'add', row: {a: 1}}));

    // Verify the invariant: connection sees overlay iff its index <= current push target
    // Phase 0: pushing to conn[0], only conn[0] should see overlay
    // Phase 1: pushing to conn[1], conn[0,1] should see overlay
    // Phase 2: pushing to conn[2], conn[0,1,2] should see overlay
    // Phase 3: pushing to conn[3], conn[0,1,2,3] should see overlay
    for (let pushPhase = 0; pushPhase < 4; pushPhase++) {
      for (let connIdx = 0; connIdx < 4; connIdx++) {
        const sawRow = observations[connIdx][pushPhase].some(r => r.a === 1);
        const shouldSeeOverlay = connIdx <= pushPhase;
        expect(sawRow).toBe(shouldSeeOverlay);
      }
    }
  });

  test('split edit: each sub-operation has correct overlay visibility', () => {
    const sort = [['a', 'asc']] as const;
    const s = createSource(
      lc,
      testLogConfig,
      'table',
      {a: {type: 'number'}, b: {type: 'string'}},
      ['a'],
    );

    // Add initial data
    consume(s.push({type: 'add', row: {a: 1, b: 'old'}}));

    // Create 3 connections - middle one has splitEditKeys on 'b'
    const spies = [
      new OverlaySpy(s.connect(sort)),
      new OverlaySpy(s.connect(sort, undefined, new Set(['b']))), // triggers split
      new OverlaySpy(s.connect(sort)),
    ];

    const observations: Row[][][] = [[], [], []];

    const recordAll = () => {
      for (let i = 0; i < 3; i++) {
        spies[i].fetch({});
        observations[i].push(
          spies[i].fetches[spies[i].fetches.length - 1].map(n =>
            n !== 'yield' ? n.row : {},
          ),
        );
      }
    };

    spies.forEach(spy => {
      spy.onPush = recordAll;
    });

    // Push an edit that will be split into remove + add
    consume(
      s.push({type: 'edit', oldRow: {a: 1, b: 'old'}, row: {a: 1, b: 'new'}}),
    );

    // Split edit creates 6 push phases: 3 for remove, 3 for add
    expect(observations[0].length).toBe(6);

    // Phases 0-2: remove operation
    // Phase 0: pushing remove to conn[0], only conn[0] sees row removed
    // Phase 1: pushing remove to conn[1], conn[0,1] see row removed
    // Phase 2: pushing remove to conn[2], all see row removed
    for (let phase = 0; phase < 3; phase++) {
      for (let connIdx = 0; connIdx < 3; connIdx++) {
        const sawOld = observations[connIdx][phase].some(r => r.b === 'old');
        const sawNew = observations[connIdx][phase].some(r => r.b === 'new');
        const seesRemoveOverlay = connIdx <= phase;
        expect(sawOld).toBe(!seesRemoveOverlay);
        expect(sawNew).toBe(false);
      }
    }

    // Phases 3-5: add operation (after remove committed)
    // Phase 3: pushing add to conn[0], only conn[0] sees new row
    // Phase 4: pushing add to conn[1], conn[0,1] see new row
    // Phase 5: pushing add to conn[2], all see new row
    for (let phase = 3; phase < 6; phase++) {
      for (let connIdx = 0; connIdx < 3; connIdx++) {
        const sawOld = observations[connIdx][phase].some(r => r.b === 'old');
        const sawNew = observations[connIdx][phase].some(r => r.b === 'new');
        const seesAddOverlay = connIdx <= phase - 3;
        expect(sawOld).toBe(false);
        expect(sawNew).toBe(seesAddOverlay);
      }
    }
  });
});
