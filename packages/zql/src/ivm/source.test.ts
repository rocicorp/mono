import {expect, test} from 'vitest';
import type {ReadonlyJSONValue} from '../../../shared/src/json.js';
import type {
  Condition,
  SimpleOperator,
} from '../../../zero-protocol/src/ast.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import {Catch, expandNode} from './catch.js';
import type {Constraint} from './constraint.js';
import type {Node} from './data.js';
import type {FetchRequest, Input, Output, Start} from './operator.js';
import type {SourceChange} from './source.js';
import {createSource} from './test/source-factory.js';

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
  fetches: Node[][] = [];

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
  }
}

test('simple-fetch', () => {
  const sort = [['a', 'asc']] as const;
  const s = createSource('table', {a: {type: 'number'}}, ['a']);
  const out = new Catch(s.connect(sort));
  expect(out.fetch()).toEqual([]);

  s.push({type: 'add', row: {a: 3}});
  expect(out.fetch()).toEqual(asNodes([{a: 3}]));

  s.push({type: 'add', row: {a: 1}});
  s.push({type: 'add', row: {a: 2}});
  expect(out.fetch()).toEqual(asNodes([{a: 1}, {a: 2}, {a: 3}]));

  s.push({type: 'remove', row: {a: 1}});
  expect(out.fetch()).toEqual(asNodes([{a: 2}, {a: 3}]));

  s.push({type: 'remove', row: {a: 2}});
  s.push({type: 'remove', row: {a: 3}});
  expect(out.fetch()).toEqual([]);
});

test('fetch-with-constraint', () => {
  const sort = [['a', 'asc']] as const;
  const s = createSource(
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
  s.push({type: 'add', row: {a: 3, b: true, c: 1, d: null}});
  s.push({type: 'add', row: {a: 1, b: true, c: 2, d: null}});
  s.push({type: 'add', row: {a: 2, b: false, c: null, d: null}});

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
    'table',
    {
      a: {type: 'number'},
    },
    ['a'],
  );
  const out = new Catch(s.connect(sort));

  expect(out.fetch({start: {row: {a: 2}, basis: 'before'}})).toEqual(
    asNodes([]),
  );
  expect(out.fetch({start: {row: {a: 2}, basis: 'at'}})).toEqual(asNodes([]));
  expect(out.fetch({start: {row: {a: 2}, basis: 'after'}})).toEqual(
    asNodes([]),
  );

  s.push({type: 'add', row: {a: 2}});
  s.push({type: 'add', row: {a: 3}});
  expect(out.fetch({start: {row: {a: 2}, basis: 'before'}})).toEqual(
    asNodes([{a: 2}, {a: 3}]),
  );
  expect(out.fetch({start: {row: {a: 2}, basis: 'at'}})).toEqual(
    asNodes([{a: 2}, {a: 3}]),
  );
  expect(out.fetch({start: {row: {a: 2}, basis: 'after'}})).toEqual(
    asNodes([{a: 3}]),
  );

  expect(out.fetch({start: {row: {a: 3}, basis: 'before'}})).toEqual(
    asNodes([{a: 2}, {a: 3}]),
  );
  expect(out.fetch({start: {row: {a: 3}, basis: 'at'}})).toEqual(
    asNodes([{a: 3}]),
  );
  expect(out.fetch({start: {row: {a: 3}, basis: 'after'}})).toEqual(
    asNodes([]),
  );

  s.push({type: 'remove', row: {a: 3}});
  expect(out.fetch({start: {row: {a: 2}, basis: 'before'}})).toEqual(
    asNodes([{a: 2}]),
  );
  expect(out.fetch({start: {row: {a: 2}, basis: 'at'}})).toEqual(
    asNodes([{a: 2}]),
  );
  expect(out.fetch({start: {row: {a: 2}, basis: 'after'}})).toEqual(
    asNodes([]),
  );
});

test('fetch-with-constraint-and-start', () => {
  const cases: {
    startData: Row[];
    start: Start;
    constraint: Constraint;
    expected: Row[];
  }[] = [
    {
      startData: [
        {a: 2, b: false},
        {a: 3, b: false},
        {a: 5, b: true},
        {a: 6, b: false},
        {a: 7, b: false},
      ],
      start: {
        row: {a: 6, b: false},
        basis: 'before',
      },
      constraint: {b: false},
      expected: [
        {a: 3, b: false},
        {a: 6, b: false},
        {a: 7, b: false},
      ],
    },
    {
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
      expected: [
        {a: 6, b: false},
        {a: 7, b: false},
      ],
    },
    {
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
      expected: [
        {a: 7, b: false},
        {a: 9, b: false},
      ],
    },
  ];

  for (const c of cases) {
    const sort = [['a', 'asc']] as const;
    const s = createSource(
      'table',
      {
        a: {type: 'number'},
        b: {type: 'boolean'},
      },
      ['a'],
    );
    for (const row of c.startData) {
      s.push({type: 'add', row});
    }
    const out = new Catch(s.connect(sort));
    expect(
      out.fetch({constraint: c.constraint, start: c.start}),
      JSON.stringify(c),
    ).toEqual(asNodes(c.expected));
  }
});

test('push', () => {
  const sort = [['a', 'asc']] as const;
  const s = createSource('table', {a: {type: 'number'}}, ['a']);
  const out = new Catch(s.connect(sort));

  expect(out.pushes).toEqual([]);

  s.push({type: 'add', row: {a: 2}});
  expect(out.pushes).toEqual(asChanges([{type: 'add', row: {a: 2}}]));

  s.push({type: 'add', row: {a: 1}});
  expect(out.pushes).toEqual(
    asChanges([
      {type: 'add', row: {a: 2}},
      {type: 'add', row: {a: 1}},
    ]),
  );

  s.push({type: 'remove', row: {a: 1}});
  s.push({type: 'remove', row: {a: 2}});
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
  expect(() => s.push({type: 'remove', row: {a: 1}})).toThrow('Row not found');
  expect(out.pushes).toEqual(asChanges([]));

  // Add row twice
  s.push({type: 'add', row: {a: 1}});
  expect(out.pushes).toEqual(asChanges([{type: 'add', row: {a: 1}}]));
  expect(() => s.push({type: 'add', row: {a: 1}})).toThrow(
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
  const s = createSource('table', {a: {type: 'number'}}, ['a']);
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

  s.push({type: 'add', row: {a: 2}});
  expect(o1.fetches).toEqual([
    asNodes([{a: 2}]),
    asNodes([{a: 2}]),
    asNodes([{a: 2}]),
  ]);
  expect(o2.fetches).toEqual([[], asNodes([{a: 2}]), asNodes([{a: 2}])]);
  expect(o3.fetches).toEqual([[], [], asNodes([{a: 2}])]);
});

test('overlay-vs-fetch-start', () => {
  const cases: {
    startData: Row[];
    start: Start;
    change: SourceChange;
    expected: Row[] | string;
  }[] = [
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 1},
        basis: 'before',
      },
      change: {type: 'add', row: {a: 1}},
      expected: [{a: 1}, {a: 2}, {a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'before',
      },
      change: {type: 'add', row: {a: 1}},
      expected: [{a: 1}, {a: 2}, {a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'before',
      },
      change: {type: 'add', row: {a: 2}},
      expected: 'Row already exists',
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'before',
      },
      change: {type: 'add', row: {a: 3}},
      expected: [{a: 2}, {a: 3}, {a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'before',
      },
      change: {type: 'add', row: {a: 5}},
      expected: [{a: 2}, {a: 4}, {a: 5}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 4},
        basis: 'before',
      },
      change: {type: 'add', row: {a: 0}},
      expected: [{a: 2}, {a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 4},
        basis: 'before',
      },
      change: {type: 'add', row: {a: 3}},
      expected: [{a: 3}, {a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 4},
        basis: 'before',
      },
      change: {type: 'add', row: {a: 5}},
      expected: [{a: 2}, {a: 4}, {a: 5}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'at',
      },
      change: {type: 'add', row: {a: 1}},
      expected: [{a: 2}, {a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'at',
      },
      change: {type: 'add', row: {a: 3}},
      expected: [{a: 2}, {a: 3}, {a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'at',
      },
      change: {type: 'add', row: {a: 5}},
      expected: [{a: 2}, {a: 4}, {a: 5}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'after',
      },
      change: {type: 'add', row: {a: 1}},
      expected: [{a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'after',
      },
      change: {type: 'add', row: {a: 3}},
      expected: [{a: 3}, {a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'after',
      },
      change: {type: 'add', row: {a: 5}},
      expected: [{a: 4}, {a: 5}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 4},
        basis: 'after',
      },
      change: {type: 'add', row: {a: 3}},
      expected: [],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 4},
        basis: 'after',
      },
      change: {type: 'add', row: {a: 5}},
      expected: [{a: 5}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'before',
      },
      change: {type: 'remove', row: {a: 1}},
      expected: 'Row not found',
    },
    {
      startData: [{a: 2}],
      start: {
        row: {a: 2},
        basis: 'before',
      },
      change: {type: 'remove', row: {a: 2}},
      expected: [],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'before',
      },
      change: {type: 'remove', row: {a: 2}},
      expected: [{a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'before',
      },
      change: {type: 'remove', row: {a: 4}},
      expected: [{a: 2}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 4},
        basis: 'before',
      },
      change: {type: 'remove', row: {a: 2}},
      expected: [{a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 4},
        basis: 'before',
      },
      change: {type: 'remove', row: {a: 4}},
      expected: [{a: 2}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'at',
      },
      change: {type: 'remove', row: {a: 2}},
      expected: [{a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'at',
      },
      change: {type: 'remove', row: {a: 4}},
      expected: [{a: 2}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 4},
        basis: 'at',
      },
      change: {type: 'remove', row: {a: 2}},
      expected: [{a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 4},
        basis: 'at',
      },
      change: {type: 'remove', row: {a: 4}},
      expected: [],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'after',
      },
      change: {type: 'remove', row: {a: 2}},
      expected: [{a: 4}],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 2},
        basis: 'after',
      },
      change: {type: 'remove', row: {a: 4}},
      expected: [],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 4},
        basis: 'after',
      },
      change: {type: 'remove', row: {a: 2}},
      expected: [],
    },
    {
      startData: [{a: 2}, {a: 4}],
      start: {
        row: {a: 4},
        basis: 'after',
      },
      change: {type: 'remove', row: {a: 4}},
      expected: [],
    },
  ];

  for (const c of cases) {
    const sort = [['a', 'asc']] as const;
    const s = createSource('table', {a: {type: 'number'}}, ['a']);
    for (const row of c.startData) {
      s.push({type: 'add', row});
    }
    const out = new OverlaySpy(s.connect(sort));
    out.onPush = () =>
      out.fetch({
        start: c.start,
      });
    if (typeof c.expected === 'string') {
      expect(() => s.push(c.change), JSON.stringify(c)).toThrow(c.expected);
    } else {
      s.push(c.change);
      expect(out.fetches, JSON.stringify(c)).toEqual([asNodes(c.expected)]);
    }
  }
});

test('overlay-vs-constraint', () => {
  const cases: {
    startData: Row[];
    constraint: Constraint;
    change: SourceChange;
    expected: Row[] | string;
  }[] = [
    {
      startData: [
        {a: 2, b: false},
        {a: 4, b: true},
      ],
      constraint: {b: true},
      change: {type: 'add', row: {a: 1, b: true}},
      expected: [
        {a: 1, b: true},
        {a: 4, b: true},
      ],
    },
    {
      startData: [
        {a: 2, b: false},
        {a: 4, b: true},
      ],
      constraint: {b: true},
      change: {type: 'add', row: {a: 1, b: false}},
      expected: [{a: 4, b: true}],
    },
    {
      startData: [
        {a: 2, b: false},
        {a: 4, b: true},
        {a: 5, b: true},
      ],
      constraint: {b: true},
      change: {type: 'edit', oldRow: {a: 4, b: true}, row: {a: 4, b: false}},
      expected: [{a: 5, b: true}],
    },
    {
      startData: [
        {a: 2, b: false},
        {a: 4, b: true},
        {a: 5, b: true},
      ],
      constraint: {b: false},
      change: {type: 'edit', oldRow: {a: 4, b: true}, row: {a: 4, b: false}},
      expected: [
        {a: 2, b: false},
        {a: 4, b: false},
      ],
    },
  ];

  for (const c of cases) {
    const sort = [['a', 'asc']] as const;
    const s = createSource(
      'table',
      {
        a: {type: 'number'},
        b: {type: 'boolean'},
      },
      ['a'],
    );
    for (const row of c.startData) {
      s.push({type: 'add', row});
    }
    const out = new OverlaySpy(s.connect(sort));
    out.onPush = () =>
      out.fetch({
        constraint: c.constraint,
      });
    if (typeof c.expected === 'string') {
      expect(() => s.push(c.change), JSON.stringify(c)).toThrow(c.expected);
    } else {
      s.push(c.change);
      expect(out.fetches, JSON.stringify(c)).toEqual([asNodes(c.expected)]);
    }
  }
});

test('overlay-vs-filter', () => {
  const cases: {
    startData: Row[];
    filter: Condition;
    change: SourceChange;
    expected: Row[] | string;
    appliedFilters?: false;
  }[] = [
    {
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
      expected: [
        {a: 1, b: true},
        {a: 4, b: true},
      ],
    },
    {
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
      expected: [{a: 4, b: true}],
    },
    {
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
      expected: [{a: 5, b: true}],
    },
    {
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
      expected: [
        {a: 2, b: false},
        {a: 4, b: false},
      ],
    },
    {
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
      change: {type: 'add', row: {a: 1, b: true}},
      expected: [
        {a: 2, b: false},
        {a: 4, b: true},
      ],
    },
    {
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
      expected: [
        {a: 1, b: false},
        {a: 2, b: false},
        {a: 4, b: true},
      ],
    },
    {
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
              correlations: [
                {
                  parentField: 'a',
                  op: '=',
                  childField: 'b',
                },
              ],
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
      expected: [
        {a: 1, b: false},
        {a: 2, b: false},
        {a: 4, b: true},
      ],
      appliedFilters: false,
    },
    {
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
              correlations: [
                {
                  parentField: 'a',
                  op: '=',
                  childField: 'b',
                },
              ],
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
      expected: [{a: 4, b: true}],
      appliedFilters: false,
    },
  ];

  for (const c of cases) {
    const sort = [['a', 'asc']] as const;
    const s = createSource(
      'table',
      {
        a: {type: 'number'},
        b: {type: 'boolean'},
      },
      ['a'],
    );
    for (const row of c.startData) {
      s.push({type: 'add', row});
    }
    const sourceInput = s.connect(sort, c.filter);
    expect(sourceInput.appliedFilters).toEqual(c.appliedFilters !== false);
    const out = new OverlaySpy(sourceInput);
    out.onPush = () => out.fetch({});
    if (typeof c.expected === 'string') {
      expect(() => s.push(c.change), JSON.stringify(c)).toThrow(c.expected);
    } else {
      s.push(c.change);
      expect(out.fetches, JSON.stringify(c)).toEqual([asNodes(c.expected)]);
    }
  }
});

test('overlay-vs-constraint-and-start', () => {
  const cases: {
    startData: Row[];
    start: Start;
    constraint: Constraint;
    change: SourceChange;
    expected: Row[] | string;
  }[] = [
    {
      startData: [
        {a: 2, b: false},
        {a: 3, b: false},
        {a: 5, b: true},
        {a: 6, b: false},
        {a: 7, b: false},
      ],
      start: {
        row: {a: 6, b: false},
        basis: 'before',
      },
      constraint: {b: false},
      change: {type: 'add', row: {a: 4, b: false}},
      expected: [
        {a: 4, b: false},
        {a: 6, b: false},
        {a: 7, b: false},
      ],
    },
    {
      startData: [
        {a: 2, b: false},
        {a: 3, b: false},
        {a: 5, b: true},
        {a: 6, b: false},
        {a: 7, b: false},
      ],
      start: {
        row: {a: 6, b: false},
        basis: 'before',
      },
      constraint: {b: false},
      change: {type: 'add', row: {a: 2.5, b: false}},
      expected: [
        {a: 3, b: false},
        {a: 6, b: false},
        {a: 7, b: false},
      ],
    },
    {
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
      expected: [
        {a: 5.75, b: false},
        {a: 6, b: false},
        {a: 7, b: false},
      ],
    },
    {
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
      expected: [
        {a: 6, b: false},
        {a: 7, b: false},
      ],
    },
    {
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
      expected: [
        {a: 6, b: false},
        {a: 7, b: false},
        {a: 8, b: false},
      ],
    },
    {
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
      expected: [
        {a: 6, b: false},
        {a: 6.5, b: false},
        {a: 7, b: false},
      ],
    },
  ];

  for (const c of cases) {
    const sort = [['a', 'asc']] as const;
    const s = createSource(
      'table',
      {
        a: {type: 'number'},
        b: {type: 'boolean'},
      },
      ['a'],
    );
    for (const row of c.startData) {
      s.push({type: 'add', row});
    }
    const out = new OverlaySpy(s.connect(sort));
    out.onPush = () =>
      out.fetch({
        start: c.start,
        constraint: c.constraint,
      });
    if (typeof c.expected === 'string') {
      expect(() => s.push(c.change), JSON.stringify(c)).toThrow(c.expected);
    } else {
      s.push(c.change);
      expect(out.fetches, JSON.stringify(c)).toEqual([asNodes(c.expected)]);
    }
  }
});

test('per-output-sorts', () => {
  const sort1 = [['a', 'asc']] as const;
  const sort2 = [
    ['b', 'asc'],
    ['a', 'asc'],
  ] as const;
  const s = createSource(
    'table',
    {
      a: {type: 'number'},
      b: {type: 'number'},
    },
    ['a'],
  );
  const out1 = new Catch(s.connect(sort1));
  const out2 = new Catch(s.connect(sort2));

  s.push({type: 'add', row: {a: 2, b: 3}});
  s.push({type: 'add', row: {a: 1, b: 2}});
  s.push({type: 'add', row: {a: 3, b: 1}});

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

test('streas-are-one-time-only', () => {
  // It is very important that streas are one-time only. This is because on
  // the server, they are backed by cursors over streaming SQL queries which
  // can't be rewound or branched. This test ensures that streas from all
  // sources behave this way for consistency.
  const source = createSource('table', {a: {type: 'number'}}, ['a']);
  source.push({type: 'add', row: {a: 1}});
  source.push({type: 'add', row: {a: 2}});
  source.push({type: 'add', row: {a: 3}});

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
    'table',
    {a: {type: 'number'}, j: {type: 'json'}},
    ['a'],
  );

  // This is certainly odd.
  // See commentary in `TableSource.push` about why
  // the `push` json is converted to a string for table-source.
  const isTableSource =
    (source as unknown as Record<string, unknown>)['setDB'] !== undefined;
  function jForPush(j: ReadonlyJSONValue): ReadonlyJSONValue {
    return isTableSource ? JSON.stringify(j) : j;
  }
  source.push({type: 'add', row: {a: 1, j: jForPush({foo: 'bar'})}});
  source.push({type: 'add', row: {a: 2, j: jForPush({baz: 'qux'})}});
  source.push({type: 'add', row: {a: 3, j: jForPush({foo: 'foo'})}});

  const out = new Catch(source.connect([['a', 'asc']]));
  expect(out.fetch({})).toEqual(
    asNodes([
      {a: 1, j: {foo: 'bar'}},
      {a: 2, j: {baz: 'qux'}},
      {a: 3, j: {foo: 'foo'}},
    ]),
  );

  source.push({type: 'add', row: {a: 4, j: jForPush({foo: 'foo'})}});
  source.push({type: 'add', row: {a: 5, j: jForPush({baz: 'qux'})}});
  source.push({type: 'add', row: {a: 6, j: jForPush({foo: 'bar'})}});
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
  source.push({
    type: 'edit',
    oldRow: {a: 4, j: jForPush({foo: 'foo'})},
    row: {a: 4, j: jForPush({foo: 'bar'})},
  });
  source.push({type: 'remove', row: {a: 5, j: jForPush({baz: 'qux'})}});
  expect(out.pushes).toEqual([
    {
      type: 'edit',
      oldRow: {a: 4, j: {foo: 'foo'}},
      row: {a: 4, j: {foo: 'bar'}},
    },
    {
      type: 'remove',
      node: {relationships: {}, row: {a: 5, j: {baz: 'qux'}}},
    },
  ]);
  expect(out.fetch({})).toEqual(
    asNodes([
      {a: 1, j: {foo: 'bar'}},
      {a: 2, j: {baz: 'qux'}},
      {a: 3, j: {foo: 'foo'}},
      {a: 4, j: {foo: 'bar'}},
      {a: 6, j: {foo: 'bar'}},
    ]),
  );
});

test('IS and IS NOT comparisons against null', () => {
  const source = createSource(
    'table',
    {
      a: {type: 'number'},
      s: {type: 'string', optional: true},
    },
    ['a'],
  );

  source.push({type: 'add', row: {a: 1, s: 'foo'}});
  source.push({type: 'add', row: {a: 2, s: 'bar'}});
  source.push({type: 'add', row: {a: 3, s: null}});

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
  expect(out.fetch({})).toEqual([
    {
      relationships: {},
      row: {
        a: 3,
        s: null,
      },
    },
  ]);

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
  expect(out.fetch({})).toEqual([
    {
      relationships: {},
      row: {
        a: 1,
        s: 'foo',
      },
    },
    {
      relationships: {},
      row: {
        a: 2,
        s: 'bar',
      },
    },
  ]);
});

test('constant/literal expression', () => {
  const source = createSource(
    'table',
    {n: {type: 'number'}, b: {type: 'boolean'}, s: {type: 'string'}},
    ['n'],
  );

  source.push({type: 'add', row: {n: 1, b: true, s: 'foo'}});
  source.push({type: 'add', row: {n: 2, b: false, s: 'bar'}});
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
