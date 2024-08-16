import {expect, test} from 'vitest';
import {Ordering} from '../../ast2/ast.js';
import {Node, Row, Value} from '../data.js';
import {FetchRequest, Input, Output, Start} from '../operator.js';
import {Catch, expandNode} from '../catch.js';
import {Source, SourceChange} from '../source.js';
import {ValueType} from '../schema.js';

type SourceFactory = (
  name: string,
  columns: Record<string, ValueType>,
  order: Ordering,
  primaryKeys: readonly [string, ...string[]],
) => Source;

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
  }

  fetch(req: FetchRequest) {
    this.fetches.push([...this.#input.fetch(req, this)].map(expandNode));
  }

  push() {
    this.onPush();
  }
}

const cases = {
  'simple-pull': (createSource: SourceFactory) => {
    // works the same for hydrate and fetch.
    for (const m of ['hydrate', 'fetch'] as const) {
      const ms = createSource('table', {a: 'number'}, [['a', 'asc']], ['a']);
      const out = new Catch(ms);
      ms.addOutput(out);
      expect(out[m]()).toEqual([]);

      ms.push({type: 'add', row: {a: 3}});
      expect(out[m]()).toEqual(asNodes([{a: 3}]));

      ms.push({type: 'add', row: {a: 1}});
      ms.push({type: 'add', row: {a: 2}});
      expect(out[m]()).toEqual(asNodes([{a: 1}, {a: 2}, {a: 3}]));

      ms.push({type: 'remove', row: {a: 1}});
      expect(out[m]()).toEqual(asNodes([{a: 2}, {a: 3}]));

      ms.push({type: 'remove', row: {a: 2}});
      ms.push({type: 'remove', row: {a: 3}});
      expect(out[m]()).toEqual([]);
    }
  },

  'pull-with-constraint': (createSource: SourceFactory) => {
    // works the same for hydrate and fetch.
    for (const m of ['hydrate', 'fetch'] as const) {
      const ms = createSource(
        'table',
        {
          a: 'number',
          b: 'boolean',
          c: 'number',
          d: 'string',
        },
        [['a', 'asc']],
        ['a'],
      );
      const out = new Catch(ms);
      ms.addOutput(out);
      ms.push({type: 'add', row: {a: 3, b: true, c: 1, d: null}});
      ms.push({type: 'add', row: {a: 1, b: true, c: 2, d: null}});
      ms.push({type: 'add', row: {a: 2, b: false, c: null, d: null}});

      expect(out[m]({constraint: {key: 'b', value: true}})).toEqual(
        asNodes([
          {a: 1, b: true, c: 2, d: null},
          {a: 3, b: true, c: 1, d: null},
        ]),
      );

      expect(out[m]({constraint: {key: 'b', value: false}})).toEqual(
        asNodes([{a: 2, b: false, c: null, d: null}]),
      );

      expect(out[m]({constraint: {key: 'c', value: 1}})).toEqual(
        asNodes([{a: 3, b: true, c: 1, d: null}]),
      );

      expect(out[m]({constraint: {key: 'c', value: 0}})).toEqual(asNodes([]));

      // Constraints are used to implement joins and so should use join
      // semantics for equality. null !== null.
      expect(out[m]({constraint: {key: 'c', value: null}})).toEqual(
        asNodes([]),
      );
      expect(out[m]({constraint: {key: 'c', value: undefined}})).toEqual(
        asNodes([]),
      );

      // Not really a feature, but because of loose typing of joins and how we
      // accept undefined we can't really tell when constraining on a field that
      // doesn't exist.
      expect(out[m]({constraint: {key: 'd', value: null}})).toEqual(
        asNodes([]),
      );
      expect(out[m]({constraint: {key: 'd', value: undefined}})).toEqual(
        asNodes([]),
      );
    }
  },

  'fetch-start': (createSource: SourceFactory) => {
    const ms = createSource(
      'table',
      {
        a: 'number',
      },
      [['a', 'asc']],
      ['a'],
    );
    const out = new Catch(ms);
    ms.addOutput(out);

    expect(out.fetch({start: {row: {a: 2}, basis: 'before'}})).toEqual(
      asNodes([]),
    );
    expect(out.fetch({start: {row: {a: 2}, basis: 'at'}})).toEqual(asNodes([]));
    expect(out.fetch({start: {row: {a: 2}, basis: 'after'}})).toEqual(
      asNodes([]),
    );

    ms.push({type: 'add', row: {a: 2}});
    ms.push({type: 'add', row: {a: 3}});
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

    ms.push({type: 'remove', row: {a: 3}});
    expect(out.fetch({start: {row: {a: 2}, basis: 'before'}})).toEqual(
      asNodes([{a: 2}]),
    );
    expect(out.fetch({start: {row: {a: 2}, basis: 'at'}})).toEqual(
      asNodes([{a: 2}]),
    );
    expect(out.fetch({start: {row: {a: 2}, basis: 'after'}})).toEqual(
      asNodes([]),
    );
  },

  'push': (createSource: SourceFactory) => {
    const ms = createSource('table', {a: 'number'}, [['a', 'asc']], ['a']);
    const out = new Catch(ms);
    ms.addOutput(out);

    expect(out.pushes).toEqual([]);

    ms.push({type: 'add', row: {a: 2}});
    expect(out.pushes).toEqual(asChanges([{type: 'add', row: {a: 2}}]));

    ms.push({type: 'add', row: {a: 1}});
    expect(out.pushes).toEqual(
      asChanges([
        {type: 'add', row: {a: 2}},
        {type: 'add', row: {a: 1}},
      ]),
    );

    ms.push({type: 'remove', row: {a: 1}});
    ms.push({type: 'remove', row: {a: 2}});
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
    expect(() => ms.push({type: 'remove', row: {a: 1}})).toThrow(
      'Row not found',
    );
    expect(out.pushes).toEqual(asChanges([]));

    // Add row twice
    ms.push({type: 'add', row: {a: 1}});
    expect(out.pushes).toEqual(asChanges([{type: 'add', row: {a: 1}}]));
    expect(() => ms.push({type: 'add', row: {a: 1}})).toThrow(
      'Row already exists',
    );
    expect(out.pushes).toEqual(asChanges([{type: 'add', row: {a: 1}}]));
  },

  'overlay-source-isolation': (createSource: SourceFactory) => {
    // Ok this is a little tricky. We are trying to show that overlays
    // only show up for one output at a time. But because calling outputs
    // is synchronous with push, it's a little tough to catch (especially
    // without involving joins, which is the reason we care about this).
    // To do so, we arrange for each output to call fetch when any of the
    // *other* outputs are pushed to. Then we can observe that the overlay
    // only shows up in the cases it is supposed to.

    const ms = createSource('table', {a: 'number'}, [['a', 'asc']], ['a']);
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
  },

  'overlay-vs-fetch-start': (createSource: SourceFactory) => {
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
      const ms = createSource('table', {a: 'number'}, [['a', 'asc']], ['a']);
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
  },

  'overlay-vs-constraint': (createSource: SourceFactory) => {
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
      const ms = createSource(
        'table',
        {
          a: 'number',
          b: 'boolean',
        },
        [['a', 'asc']],
        ['a'],
      );
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
  },
};

/**
 * All sources should uphold the same contract so we can run the same tests
 * against all source implementations.
 */
export function runCases(
  createSource: SourceFactory,
  omit: Set<string> = new Set(),
  only: Set<string> = new Set(),
) {
  for (const [name, fn] of Object.entries(cases)) {
    if (omit.has(name)) {
      continue;
    }
    if (only.size > 0 && !only.has(name)) {
      continue;
    }
    test(name, () => fn(createSource));
  }
}
