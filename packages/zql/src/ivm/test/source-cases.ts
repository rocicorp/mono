import {expect, test} from 'vitest';
import type {Row, Value} from '../../../../zero-protocol/src/data.js';
import {Catch, expandNode} from '../catch.js';
import type {Node} from '../data.js';
import type {FetchRequest, Input, Output, Start} from '../operator.js';
import type {SchemaValue} from '../schema.js';
import type {Source, SourceChange} from '../source.js';
import type {ReadonlyJSONValue} from '../../../../shared/src/json.js';

type SourceFactory = (
  name: string,
  columns: Record<string, SchemaValue>,
  primaryKey: readonly [string, ...string[]],
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
    this.#input.setOutput(this);
  }

  fetch(req: FetchRequest) {
    this.fetches.push([...this.#input.fetch(req)].map(expandNode));
  }

  push() {
    this.onPush();
  }
}

const cases = {
  'simple-fetch': (createSource: SourceFactory) => {
    const sort = [['a', 'asc']] as const;
    const ms = createSource('table', {a: {type: 'number'}}, ['a']);
    const out = new Catch(ms.connect(sort));
    expect(out.fetch()).toEqual([]);

    ms.push({type: 'add', row: {a: 3}});
    expect(out.fetch()).toEqual(asNodes([{a: 3}]));

    ms.push({type: 'add', row: {a: 1}});
    ms.push({type: 'add', row: {a: 2}});
    expect(out.fetch()).toEqual(asNodes([{a: 1}, {a: 2}, {a: 3}]));

    ms.push({type: 'remove', row: {a: 1}});
    expect(out.fetch()).toEqual(asNodes([{a: 2}, {a: 3}]));

    ms.push({type: 'remove', row: {a: 2}});
    ms.push({type: 'remove', row: {a: 3}});
    expect(out.fetch()).toEqual([]);
  },

  'fetch-with-constraint': (createSource: SourceFactory) => {
    const sort = [['a', 'asc']] as const;
    const ms = createSource(
      'table',
      {
        a: {type: 'number'},
        b: {type: 'boolean'},
        c: {type: 'number'},
        d: {type: 'string'},
      },
      ['a'],
    );
    const out = new Catch(ms.connect(sort));
    ms.push({type: 'add', row: {a: 3, b: true, c: 1, d: null}});
    ms.push({type: 'add', row: {a: 1, b: true, c: 2, d: null}});
    ms.push({type: 'add', row: {a: 2, b: false, c: null, d: null}});

    expect(out.fetch({constraint: {key: 'b', value: true}})).toEqual(
      asNodes([
        {a: 1, b: true, c: 2, d: null},
        {a: 3, b: true, c: 1, d: null},
      ]),
    );

    expect(out.fetch({constraint: {key: 'b', value: false}})).toEqual(
      asNodes([{a: 2, b: false, c: null, d: null}]),
    );

    expect(out.fetch({constraint: {key: 'c', value: 1}})).toEqual(
      asNodes([{a: 3, b: true, c: 1, d: null}]),
    );

    expect(out.fetch({constraint: {key: 'c', value: 0}})).toEqual(asNodes([]));

    // Constraints are used to implement joins and so should use join
    // semantics for equality. null !== null.
    expect(out.fetch({constraint: {key: 'c', value: null}})).toEqual(
      asNodes([]),
    );
    expect(out.fetch({constraint: {key: 'c', value: undefined}})).toEqual(
      asNodes([]),
    );

    // Not really a feature, but because of loose typing of joins and how we
    // accept undefined we can't really tell when constraining on a field that
    // doesn't exist.
    expect(out.fetch({constraint: {key: 'd', value: null}})).toEqual(
      asNodes([]),
    );
    expect(out.fetch({constraint: {key: 'd', value: undefined}})).toEqual(
      asNodes([]),
    );
  },

  'fetch-start': (createSource: SourceFactory) => {
    const sort = [['a', 'asc']] as const;
    const ms = createSource(
      'table',
      {
        a: {type: 'number'},
      },
      ['a'],
    );
    const out = new Catch(ms.connect(sort));

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

  'fetch-with-constraint-and-start': (createSource: SourceFactory) => {
    const cases: {
      startData: Row[];
      start: Start;
      constraint: {key: string; value: Value};
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
        constraint: {key: 'b', value: false},
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
        constraint: {key: 'b', value: false},
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
        constraint: {key: 'b', value: false},
        expected: [
          {a: 7, b: false},
          {a: 9, b: false},
        ],
      },
    ];

    for (const c of cases) {
      const sort = [['a', 'asc']] as const;
      const ms = createSource(
        'table',
        {
          a: {type: 'number'},
          b: {type: 'boolean'},
        },
        ['a'],
      );
      for (const row of c.startData) {
        ms.push({type: 'add', row});
      }
      const out = new Catch(ms.connect(sort));
      expect(
        out.fetch({constraint: c.constraint, start: c.start}),
        JSON.stringify(c),
      ).toEqual(asNodes(c.expected));
    }
  },

  'push': (createSource: SourceFactory) => {
    const sort = [['a', 'asc']] as const;
    const ms = createSource('table', {a: {type: 'number'}}, ['a']);
    const out = new Catch(ms.connect(sort));

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

    const sort = [['a', 'asc']] as const;
    const ms = createSource('table', {a: {type: 'number'}}, ['a']);
    const o1 = new OverlaySpy(ms.connect(sort));
    const o2 = new OverlaySpy(ms.connect(sort));
    const o3 = new OverlaySpy(ms.connect(sort));

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
      const sort = [['a', 'asc']] as const;
      const ms = createSource('table', {a: {type: 'number'}}, ['a']);
      for (const row of c.startData) {
        ms.push({type: 'add', row});
      }
      const out = new OverlaySpy(ms.connect(sort));
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
      const sort = [['a', 'asc']] as const;
      const ms = createSource(
        'table',
        {
          a: {type: 'number'},
          b: {type: 'boolean'},
        },
        ['a'],
      );
      for (const row of c.startData) {
        ms.push({type: 'add', row});
      }
      const out = new OverlaySpy(ms.connect(sort));
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

  'overlay-vs-constraint-and-start': (createSource: SourceFactory) => {
    const cases: {
      startData: Row[];
      start: Start;
      constraint: {key: string; value: Value};
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
        constraint: {key: 'b', value: false},
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
        constraint: {key: 'b', value: false},
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
        constraint: {key: 'b', value: false},
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
        constraint: {key: 'b', value: false},
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
        constraint: {key: 'b', value: false},
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
        constraint: {key: 'b', value: false},
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
      const ms = createSource(
        'table',
        {
          a: {type: 'number'},
          b: {type: 'boolean'},
        },
        ['a'],
      );
      for (const row of c.startData) {
        ms.push({type: 'add', row});
      }
      const out = new OverlaySpy(ms.connect(sort));
      out.onPush = () =>
        out.fetch({
          start: c.start,
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

  'per-output-sorts': (createSource: SourceFactory) => {
    const sort1 = [['a', 'asc']] as const;
    const sort2 = [
      ['b', 'asc'],
      ['a', 'asc'],
    ] as const;
    const ms = createSource(
      'table',
      {
        a: {type: 'number'},
        b: {type: 'number'},
      },
      ['a'],
    );
    const out1 = new Catch(ms.connect(sort1));
    const out2 = new Catch(ms.connect(sort2));

    ms.push({type: 'add', row: {a: 2, b: 3}});
    ms.push({type: 'add', row: {a: 1, b: 2}});
    ms.push({type: 'add', row: {a: 3, b: 1}});

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
  },

  'streams-are-one-time-only': (createSource: SourceFactory) => {
    // It is very important that streams are one-time only. This is because on
    // the server, they are backed by cursors over streaming SQL queries which
    // can't be rewound or branched. This test ensures that streams from all
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
  },

  'json is a valid type to read and write to/from a source': (
    createSource: SourceFactory,
  ) => {
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
