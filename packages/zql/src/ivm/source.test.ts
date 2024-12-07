import {expect, suite, test} from 'vitest';
import type {ReadonlyJSONValue} from '../../../shared/src/json.js';
import type {
  Condition,
  SimpleOperator,
} from '../../../zero-protocol/src/ast.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';
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

  expect(out.fetch({start: {row: {a: 2}, basis: 'at'}})).toEqual(asNodes([]));
  expect(out.fetch({start: {row: {a: 2}, basis: 'after'}})).toEqual(
    asNodes([]),
  );

  s.push({type: 'add', row: {a: 2}});
  s.push({type: 'add', row: {a: 3}});
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

  s.push({type: 'remove', row: {a: 3}});
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

  s.push({type: 'add', row: {a: 2}});
  s.push({type: 'add', row: {a: 3}});
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

  s.push({type: 'remove', row: {a: 3}});
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
      'table',
      c.columns ?? {
        a: {type: 'number'},
        b: {type: 'boolean'},
      },
      ['a'],
    );
    for (const row of c.startData) {
      s.push({type: 'add', row});
    }
    const out = new Catch(s.connect(sort));
    return out.fetch({
      constraint: c.constraint,
      start: c.start,
      reverse: c.reverse,
    });
  }

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

suite('overlay-vs-fetch-start', () => {
  function t(c: {
    startData: Row[];
    start: Start;
    reverse?: boolean | undefined;
    change: SourceChange;
  }) {
    const sort = [['a', 'asc']] as const;
    const s = createSource('table', {a: {type: 'number'}}, ['a']);
    for (const row of c.startData) {
      s.push({type: 'add', row});
    }
    const out = new OverlaySpy(s.connect(sort));
    out.onPush = () =>
      out.fetch({
        start: c.start,
        reverse: c.reverse,
      });
    try {
      s.push(c.change);
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
    try {
      s.push(c.change);
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
    const out = new OverlaySpy(sourceInput);
    out.onPush = () => out.fetch({});
    try {
      s.push(c.change);
    } catch (e) {
      return {
        e: (e as Error).message,
        appliedFilters: sourceInput.appliedFilters,
      };
    }
    return {
      fetches: out.fetches,
      appliedFilters: sourceInput.appliedFilters,
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
        "appliedFilters": true,
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
        "appliedFilters": true,
        "fetches": [
          [
            {
              "relationships": {},
              "row": {
                "a": 4,
                "b": true,
              },
            },
          ],
        ],
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
        "appliedFilters": true,
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
        "appliedFilters": true,
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
        change: {type: 'add', row: {a: 1, b: true}},
      }),
    ).toMatchInlineSnapshot(`
      {
        "appliedFilters": true,
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
                "b": true,
              },
            },
          ],
        ],
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
        "appliedFilters": true,
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
        "appliedFilters": false,
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
        change: {type: 'add', row: {a: 1, b: false}},
      }),
    ).toMatchInlineSnapshot(`
      {
        "appliedFilters": false,
        "fetches": [
          [
            {
              "relationships": {},
              "row": {
                "a": 4,
                "b": true,
              },
            },
          ],
        ],
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
      'table',
      c.columns ?? {
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
    try {
      s.push(c.change);
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

test('streams-are-one-time-only', () => {
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
