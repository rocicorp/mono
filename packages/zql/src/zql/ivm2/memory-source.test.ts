import {expect, test} from 'vitest';
import {compareRowsTest} from './data.test.js';
import type {Ordering} from '../ast2/ast.js';
import {MemorySource, SourceChange} from './memory-source.js';
import {Snarf} from './snarf.js';
import type {Row, Node, Value} from './data.js';
import type {FetchRequest, Input, Output, Start} from './operator.js';

test('schema', () => {
  compareRowsTest((order: Ordering) => {
    const ms = new MemorySource(order);
    return ms.schema.compareRows;
  });
});

function asNodes(rows: Row[]): Node[] {
  return rows.map(row => ({
    row,
    relationships: new Map(),
  }));
}

test('simple-pull', () => {
  // works the same for hydrate and fetch.
  for (const m of ['hydrate', 'fetch'] as const) {
    const ms = new MemorySource([['a', 'asc']]);
    const out = new Snarf();
    expect([...ms[m]({}, out)]).toEqual([]);

    ms.push({type: 'add', row: {a: 3}});
    expect([...ms[m]({}, out)]).toEqual(asNodes([{a: 3}]));

    ms.push({type: 'add', row: {a: 1}});
    ms.push({type: 'add', row: {a: 2}});
    expect([...ms[m]({}, out)]).toEqual(asNodes([{a: 1}, {a: 2}, {a: 3}]));

    ms.push({type: 'remove', row: {a: 1}});
    expect([...ms[m]({}, out)]).toEqual(asNodes([{a: 2}, {a: 3}]));

    ms.push({type: 'remove', row: {a: 2}});
    ms.push({type: 'remove', row: {a: 3}});
    expect([...ms[m]({}, out)]).toEqual([]);
  }
});

test('pull-with-constraint', () => {
  // works the same for hydrate and fetch.
  for (const m of ['hydrate', 'fetch'] as const) {
    const ms = new MemorySource([['a', 'asc']]);
    const out = new Snarf();
    ms.addOutput(out);
    ms.push({type: 'add', row: {a: 3, b: true, c: 1}});
    ms.push({type: 'add', row: {a: 1, b: true, c: 2}});
    ms.push({type: 'add', row: {a: 2, b: false, c: null}});

    expect([...ms[m]({constraint: {key: 'b', value: true}}, out)]).toEqual(
      asNodes([
        {a: 1, b: true, c: 2},
        {a: 3, b: true, c: 1},
      ]),
    );

    expect([...ms[m]({constraint: {key: 'b', value: false}}, out)]).toEqual(
      asNodes([{a: 2, b: false, c: null}]),
    );

    expect([...ms[m]({constraint: {key: 'c', value: 1}}, out)]).toEqual(
      asNodes([{a: 3, b: true, c: 1}]),
    );

    expect([...ms[m]({constraint: {key: 'c', value: 0}}, out)]).toEqual(
      asNodes([]),
    );

    // Constraints are used to implement joins and so should use join
    // semantics for equality. null !== null.
    expect([...ms[m]({constraint: {key: 'c', value: null}}, out)]).toEqual(
      asNodes([]),
    );
    expect([...ms[m]({constraint: {key: 'c', value: undefined}}, out)]).toEqual(
      asNodes([]),
    );

    // Not really a feature, but because of loose typing of joins and how we
    // accept undefined we can't really tell when constraining on a field that
    // doesn't exist.
    expect([...ms[m]({constraint: {key: 'd', value: null}}, out)]).toEqual(
      asNodes([]),
    );
    expect([...ms[m]({constraint: {key: 'd', value: undefined}}, out)]).toEqual(
      asNodes([]),
    );
  }
});

test('fetch-start', () => {
  const ms = new MemorySource([['a', 'asc']]);
  const out = new Snarf();
  ms.addOutput(out);

  expect([...ms.fetch({start: {row: {a: 2}, basis: 'before'}}, out)]).toEqual(
    asNodes([]),
  );
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'at'}}, out)]).toEqual(
    asNodes([]),
  );
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'after'}}, out)]).toEqual(
    asNodes([]),
  );

  ms.push({type: 'add', row: {a: 2}});
  ms.push({type: 'add', row: {a: 3}});
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'before'}}, out)]).toEqual(
    asNodes([{a: 2}, {a: 3}]),
  );
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'at'}}, out)]).toEqual(
    asNodes([{a: 2}, {a: 3}]),
  );
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'after'}}, out)]).toEqual(
    asNodes([{a: 3}]),
  );

  expect([...ms.fetch({start: {row: {a: 3}, basis: 'before'}}, out)]).toEqual(
    asNodes([{a: 2}, {a: 3}]),
  );
  expect([...ms.fetch({start: {row: {a: 3}, basis: 'at'}}, out)]).toEqual(
    asNodes([{a: 3}]),
  );
  expect([...ms.fetch({start: {row: {a: 3}, basis: 'after'}}, out)]).toEqual(
    asNodes([]),
  );

  ms.push({type: 'remove', row: {a: 3}});
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'before'}}, out)]).toEqual(
    asNodes([{a: 2}]),
  );
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'at'}}, out)]).toEqual(
    asNodes([{a: 2}]),
  );
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'after'}}, out)]).toEqual(
    asNodes([]),
  );
});

function asChanges(sc: SourceChange[]) {
  return sc.map(c => ({
    type: c.type,
    node: {
      row: c.row,
      relationships: {},
    },
  }));
}

test('push', () => {
  const ms = new MemorySource([['a', 'asc']]);
  const out = new Snarf();
  ms.addOutput(out);

  expect(out.changes).toEqual([]);

  ms.push({type: 'add', row: {a: 2}});
  expect(out.changes).toEqual(asChanges([{type: 'add', row: {a: 2}}]));

  ms.push({type: 'add', row: {a: 1}});
  expect(out.changes).toEqual(
    asChanges([
      {type: 'add', row: {a: 2}},
      {type: 'add', row: {a: 1}},
    ]),
  );

  ms.push({type: 'remove', row: {a: 1}});
  ms.push({type: 'remove', row: {a: 2}});
  expect(out.changes).toEqual(
    asChanges([
      {type: 'add', row: {a: 2}},
      {type: 'add', row: {a: 1}},
      {type: 'remove', row: {a: 1}},
      {type: 'remove', row: {a: 2}},
    ]),
  );

  // Remove row that isn't there
  out.reset();
  expect(() => ms.push({type: 'remove', row: {a: 1}})).toThrow('Row not found');
  expect(out.changes).toEqual(asChanges([]));

  // Add row twice
  ms.push({type: 'add', row: {a: 1}});
  expect(out.changes).toEqual(asChanges([{type: 'add', row: {a: 1}}]));
  expect(() => ms.push({type: 'add', row: {a: 1}})).toThrow(
    'Row already exists',
  );
  expect(out.changes).toEqual(asChanges([{type: 'add', row: {a: 1}}]));
});

class OverlaySpy implements Output {
  #input: Input;
  fetches: Node[][] = [];

  onPush = () => {};

  constructor(input: Input) {
    this.#input = input;
  }

  fetch(req: FetchRequest) {
    this.fetches.push([...this.#input.fetch(req, this)]);
  }

  push() {
    this.onPush();
  }
}

test('overlay-source-isolation', () => {
  // Ok this is a little tricky. We are trying to show that overlays
  // only show up for one output at a time. But because calling outputs
  // is synchronous with push, it's a little tough to catch (especially
  // without involving joins, which is the reason we care about this).
  // To do so, we arrange for each output to call fetch when any of the
  // *other* outputs are pushed to. Then we can observe that the overlay
  // only shows up in the cases it is supposed to.

  const ms = new MemorySource([['a', 'asc']]);
  const o1 = new OverlaySpy(ms);
  const o2 = new OverlaySpy(ms);
  const o3 = new OverlaySpy(ms);
  ms.addOutput(o1);
  ms.addOutput(o2);
  ms.addOutput(o3);

  function fetchAll() {
    o1.fetch({});
    o2.fetch({});
    o3.fetch({});
  }

  o1.onPush = fetchAll;
  o2.onPush = fetchAll;
  o3.onPush = fetchAll;

  ms.push({type: 'add', row: {a: 2}});
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
    const ms = new MemorySource([['a', 'asc']]);
    for (const row of c.startData) {
      ms.push({type: 'add', row});
    }
    const out = new OverlaySpy(ms);
    ms.addOutput(out);
    out.onPush = () =>
      out.fetch({
        start: c.start,
      });
    if (typeof c.expected === 'string') {
      expect(() => ms.push(c.change), JSON.stringify(c)).toThrow(c.expected);
    } else {
      ms.push(c.change);
      expect(out.fetches, JSON.stringify(c)).toEqual([asNodes(c.expected)]);
    }
  }
});

test('overlay-vs-constraint', () => {
  const cases: {
    startData: Row[];
    constraint: {key: string; value: Value};
    change: SourceChange;
    expected: Row[] | string;
  }[] = [
    {
      startData: [
        {a: 2, b: false},
        {a: 4, b: true},
      ],
      constraint: {key: 'b', value: true},
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
      constraint: {key: 'b', value: true},
      change: {type: 'add', row: {a: 1, b: false}},
      expected: [{a: 4, b: true}],
    },
  ];

  for (const c of cases) {
    const ms = new MemorySource([['a', 'asc']]);
    for (const row of c.startData) {
      ms.push({type: 'add', row});
    }
    const out = new OverlaySpy(ms);
    ms.addOutput(out);
    out.onPush = () =>
      out.fetch({
        constraint: c.constraint,
      });
    if (typeof c.expected === 'string') {
      expect(() => ms.push(c.change), JSON.stringify(c)).toThrow(c.expected);
    } else {
      ms.push(c.change);
      expect(out.fetches, JSON.stringify(c)).toEqual([asNodes(c.expected)]);
    }
  }
});
