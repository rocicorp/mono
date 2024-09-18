import {unreachable} from 'shared/src/asserts.js';
import {stringCompare} from 'shared/src/string-compare.js';
import {expect, test} from 'vitest';
import {ArrayView} from './array-view.js';
import {Change} from './change.js';
import {Node} from './data.js';
import {Join} from './join.js';
import {MemorySource} from './memory-source.js';
import {MemoryStorage} from './memory-storage.js';
import {Input} from './operator.js';
import {Schema} from './schema.js';
import {Stream} from './stream.js';

test('basics', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  const view = new ArrayView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
  );

  let callCount = 0;
  let data: unknown[] = [];
  const unlisten = view.addListener(entries => {
    ++callCount;
    data = [...entries];
  });

  view.hydrate();
  expect(data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
  ]);

  expect(callCount).toBe(1);
  expect(() => view.hydrate()).toThrow("Can't hydrate twice");

  ms.push({row: {a: 3, b: 'c'}, type: 'add'});

  // We don't get called until flush.
  expect(callCount).toBe(1);

  view.flush();
  expect(callCount).toBe(2);
  expect(data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
    {a: 3, b: 'c'},
  ]);

  ms.push({row: {a: 2, b: 'b'}, type: 'remove'});
  expect(callCount).toBe(2);
  ms.push({row: {a: 1, b: 'a'}, type: 'remove'});
  expect(callCount).toBe(2);

  view.flush();
  expect(callCount).toBe(3);
  expect(data).toEqual([{a: 3, b: 'c'}]);

  unlisten();
  ms.push({row: {a: 3, b: 'c'}, type: 'remove'});
  expect(callCount).toBe(3);

  view.flush();
  expect(callCount).toBe(3);
  expect(data).toEqual([{a: 3, b: 'c'}]);
});

test('hydrate-empty', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );

  const view = new ArrayView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
  );

  let callCount = 0;
  let data: unknown[] = [];
  view.addListener(entries => {
    ++callCount;
    data = [...entries];
  });

  view.hydrate();
  expect(data).toEqual([]);
  expect(callCount).toBe(1);
});

test('tree', () => {
  const ms = new MemorySource(
    'table',
    {id: {type: 'number'}, name: {type: 'string'}},
    ['id'],
  );
  ms.push({
    type: 'add',
    row: {id: 1, name: 'foo', childID: 2},
  });
  ms.push({
    type: 'add',
    row: {id: 2, name: 'foobar', childID: null},
  });
  ms.push({
    type: 'add',
    row: {id: 3, name: 'mon', childID: 4},
  });
  ms.push({
    type: 'add',
    row: {id: 4, name: 'monkey', childID: null},
  });

  const join = new Join({
    parent: ms.connect([
      ['name', 'asc'],
      ['id', 'asc'],
    ]),
    child: ms.connect([
      ['name', 'desc'],
      ['id', 'desc'],
    ]),
    storage: new MemoryStorage(),
    parentKey: 'childID',
    childKey: 'id',
    relationshipName: 'children',
    hidden: false,
  });

  const view = new ArrayView(join);
  let data: unknown[] = [];
  view.addListener(entries => {
    data = [...entries];
  });

  view.hydrate();
  expect(data).toEqual([
    {
      id: 1,
      name: 'foo',
      childID: 2,
      children: [
        {
          id: 2,
          name: 'foobar',
          childID: null,
        },
      ],
    },
    {
      id: 2,
      name: 'foobar',
      childID: null,
      children: [],
    },
    {
      id: 3,
      name: 'mon',
      childID: 4,
      children: [
        {
          id: 4,
          name: 'monkey',
          childID: null,
        },
      ],
    },
    {
      id: 4,
      name: 'monkey',
      childID: null,
      children: [],
    },
  ]);

  // add parent with child
  ms.push({type: 'add', row: {id: 5, name: 'chocolate', childID: 2}});
  view.flush();
  expect(data).toEqual([
    {
      id: 5,
      name: 'chocolate',
      childID: 2,
      children: [
        {
          id: 2,
          name: 'foobar',
          childID: null,
        },
      ],
    },
    {
      id: 1,
      name: 'foo',
      childID: 2,
      children: [
        {
          id: 2,
          name: 'foobar',
          childID: null,
        },
      ],
    },
    {
      id: 2,
      name: 'foobar',
      childID: null,
      children: [],
    },
    {
      id: 3,
      name: 'mon',
      childID: 4,
      children: [
        {
          id: 4,
          name: 'monkey',
          childID: null,
        },
      ],
    },
    {
      id: 4,
      name: 'monkey',
      childID: null,
      children: [],
    },
  ]);

  // remove parent with child
  ms.push({type: 'remove', row: {id: 5, name: 'chocolate', childID: 2}});
  view.flush();
  expect(data).toEqual([
    {
      id: 1,
      name: 'foo',
      childID: 2,
      children: [
        {
          id: 2,
          name: 'foobar',
          childID: null,
        },
      ],
    },
    {
      id: 2,
      name: 'foobar',
      childID: null,
      children: [],
    },
    {
      id: 3,
      name: 'mon',
      childID: 4,
      children: [
        {
          id: 4,
          name: 'monkey',
          childID: null,
        },
      ],
    },
    {
      id: 4,
      name: 'monkey',
      childID: null,
      children: [],
    },
  ]);

  // remove just child
  ms.push({
    type: 'remove',
    row: {
      id: 2,
      name: 'foobar',
      childID: null,
    },
  });
  view.flush();
  expect(data).toEqual([
    {
      id: 1,
      name: 'foo',
      childID: 2,
      children: [],
    },
    {
      id: 3,
      name: 'mon',
      childID: 4,
      children: [
        {
          id: 4,
          name: 'monkey',
          childID: null,
        },
      ],
    },
    {
      id: 4,
      name: 'monkey',
      childID: null,
      children: [],
    },
  ]);

  // add child
  ms.push({
    type: 'add',
    row: {
      id: 2,
      name: 'foobaz',
      childID: null,
    },
  });
  view.flush();
  expect(data).toEqual([
    {
      id: 1,
      name: 'foo',
      childID: 2,
      children: [
        {
          id: 2,
          name: 'foobaz',
          childID: null,
        },
      ],
    },
    {
      id: 2,
      name: 'foobaz',
      childID: null,
      children: [],
    },
    {
      id: 3,
      name: 'mon',
      childID: 4,
      children: [
        {
          id: 4,
          name: 'monkey',
          childID: null,
        },
      ],
    },
    {
      id: 4,
      name: 'monkey',
      childID: null,
      children: [],
    },
  ]);
});

test('collapse hidden relationships', () => {
  const schema: Schema = {
    tableName: 'issue',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      name: {type: 'string'},
    },
    sort: [['id', 'asc']],
    isHidden: false,
    compareRows: (r1, r2) => (r1.id as number) - (r2.id as number),
    relationships: {
      labels: {
        tableName: 'issueLabel',
        primaryKey: ['id'],
        sort: [['id', 'asc']],
        columns: {
          id: {type: 'number'},
          issueId: {type: 'number'},
          labelId: {type: 'number'},
        },
        isHidden: true,
        compareRows: (r1, r2) => (r1.id as number) - (r2.id as number),
        relationships: {
          labels: {
            tableName: 'label',
            primaryKey: ['id'],
            columns: {
              id: {type: 'number'},
              name: {type: 'string'},
            },
            isHidden: false,
            sort: [['id', 'asc']],
            compareRows: (r1, r2) => (r1.id as number) - (r2.id as number),
            relationships: {},
          },
        },
      },
    },
  };

  const input = {
    cleanup() {
      throw new Error('not implemented');
    },
    fetch() {
      throw new Error('not implemented');
    },
    destroy() {},
    getSchema() {
      return schema;
    },
    setOutput() {},
    push(change: Change) {
      view.push(change);
    },
  };

  const view = new ArrayView(input);
  let data: unknown[] = [];
  view.addListener(entries => {
    data = [...entries];
  });

  const changeSansType = {
    node: {
      row: {
        id: 1,
        name: 'issue',
      },
      relationships: {
        labels: [
          {
            row: {
              id: 1,
              issueId: 1,
              labelId: 1,
            },
            relationships: {
              labels: [
                {
                  row: {
                    id: 1,
                    name: 'label',
                  },
                  relationships: {},
                },
              ],
            },
          },
        ],
      },
    },
  } as const;
  view.push({
    type: 'add',
    ...changeSansType,
  });
  view.flush();

  expect(data).toEqual([
    {
      id: 1,
      labels: [
        {
          id: 1,
          name: 'label',
        },
      ],
      name: 'issue',
    },
  ]);

  view.push({
    type: 'remove',
    ...changeSansType,
  });
  view.flush();

  expect(data).toEqual([]);

  view.push({
    type: 'add',
    ...changeSansType,
  });
  // no commit
  expect(data).toEqual([]);

  view.push({
    type: 'child',
    row: {
      id: 1,
      name: 'issue',
    },
    child: {
      relationshipName: 'labels',
      change: {
        type: 'add',
        node: {
          row: {
            id: 2,
            issueId: 1,
            labelId: 2,
          },
          relationships: {
            labels: [
              {
                row: {
                  id: 2,
                  name: 'label2',
                },
                relationships: {},
              },
            ],
          },
        },
      },
    },
  });
  view.flush();

  expect(data).toEqual([
    {
      id: 1,
      labels: [
        {
          id: 1,
          name: 'label',
        },
        {
          id: 2,
          name: 'label2',
        },
      ],
      name: 'issue',
    },
  ]);
});

test('basic with edit pushes', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  const view = new ArrayView(ms.connect([['a', 'asc']]));

  let callCount = 0;
  let data: unknown[] = [];
  const unlisten = view.addListener(entries => {
    ++callCount;
    data = [...entries];
  });

  view.hydrate();
  expect(data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
  ]);

  expect(callCount).toBe(1);

  ms.push({type: 'edit', row: {a: 2, b: 'b2'}, oldRow: {a: 2, b: 'b'}});

  // We don't get called until flush.
  expect(callCount).toBe(1);

  view.flush();
  expect(callCount).toBe(2);
  expect(data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b2'},
  ]);

  ms.push({type: 'edit', row: {a: 3, b: 'b3'}, oldRow: {a: 2, b: 'b2'}});

  view.flush();
  expect(callCount).toBe(3);
  expect(data).toEqual([
    {a: 1, b: 'a'},
    {a: 3, b: 'b3'},
  ]);

  unlisten();
});

test('tree edit', () => {
  const ms = new MemorySource(
    'table',
    {id: {type: 'number'}, name: {type: 'string'}, data: {type: 'string'}},
    ['id'],
  );
  for (const row of [
    {id: 1, name: 'foo', data: 'a', childID: 2},
    {id: 2, name: 'foobar', data: 'b', childID: null},
    {id: 3, name: 'mon', data: 'c', childID: 4},
    {id: 4, name: 'monkey', data: 'd', childID: null},
  ] as const) {
    ms.push({type: 'add', row});
  }

  const join = new Join({
    parent: ms.connect([
      ['name', 'asc'],
      ['id', 'asc'],
    ]),
    child: ms.connect([
      ['name', 'desc'],
      ['id', 'desc'],
    ]),
    storage: new MemoryStorage(),
    parentKey: 'childID',
    childKey: 'id',
    relationshipName: 'children',
    hidden: false,
  });

  const view = new ArrayView(join);
  let data: unknown[] = [];
  view.addListener(entries => {
    data = [...entries];
  });

  view.hydrate();
  expect(data).toEqual([
    {
      id: 1,
      name: 'foo',
      data: 'a',
      childID: 2,
      children: [
        {
          id: 2,
          name: 'foobar',
          data: 'b',
          childID: null,
        },
      ],
    },
    {
      id: 2,
      name: 'foobar',
      data: 'b',
      childID: null,
      children: [],
    },
    {
      id: 3,
      name: 'mon',
      data: 'c',
      childID: 4,
      children: [
        {
          id: 4,
          name: 'monkey',
          data: 'd',
          childID: null,
        },
      ],
    },
    {
      id: 4,
      name: 'monkey',
      data: 'd',
      childID: null,
      children: [],
    },
  ]);

  // Edit root
  ms.push({
    type: 'edit',
    oldRow: {id: 1, name: 'foo', data: 'a', childID: 2},
    row: {id: 1, name: 'foo', data: 'a2', childID: 2},
  });
  view.flush();
  expect(data).toEqual([
    {
      id: 1,
      name: 'foo',
      data: 'a2',
      childID: 2,
      children: [
        {
          id: 2,
          name: 'foobar',
          data: 'b',
          childID: null,
        },
      ],
    },
    {
      id: 2,
      name: 'foobar',
      data: 'b',
      childID: null,
      children: [],
    },
    {
      id: 3,
      name: 'mon',
      data: 'c',
      childID: 4,
      children: [
        {
          id: 4,
          name: 'monkey',
          data: 'd',
          childID: null,
        },
      ],
    },
    {
      id: 4,
      name: 'monkey',
      data: 'd',
      childID: null,
      children: [],
    },
  ]);
});

test('edit to change the order', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  for (const row of [
    {a: 10, b: 'a'},
    {a: 20, b: 'b'},
    {a: 30, b: 'c'},
  ] as const) {
    ms.push({row, type: 'add'});
  }

  const view = new ArrayView(ms.connect([['a', 'asc']]));
  let data: unknown[] = [];
  view.addListener(entries => {
    data = [...entries];
  });
  view.hydrate();

  expect(data).toEqual([
    {a: 10, b: 'a'},
    {a: 20, b: 'b'},
    {a: 30, b: 'c'},
  ]);

  ms.push({
    type: 'edit',
    oldRow: {a: 20, b: 'b'},
    row: {a: 5, b: 'b2'},
  });
  view.flush();
  expect(data).toEqual([
    {a: 5, b: 'b2'},
    {a: 10, b: 'a'},
    {a: 30, b: 'c'},
  ]);

  ms.push({
    type: 'edit',
    oldRow: {a: 5, b: 'b2'},
    row: {a: 4, b: 'b3'},
  });

  view.flush();
  expect(data).toEqual([
    {a: 4, b: 'b3'},
    {a: 10, b: 'a'},
    {a: 30, b: 'c'},
  ]);

  ms.push({
    type: 'edit',
    oldRow: {a: 4, b: 'b3'},
    row: {a: 20, b: 'b4'},
  });
  view.flush();
  expect(data).toEqual([
    {a: 10, b: 'a'},
    {a: 20, b: 'b4'},
    {a: 30, b: 'c'},
  ]);
});

test('edit to preserve relationships', () => {
  const labelSchema: Schema = {
    tableName: 'label',
    primaryKey: ['id'],
    columns: {id: {type: 'number'}, name: {type: 'string'}},
    sort: [['name', 'asc']],
    isHidden: false,
    compareRows: (r1, r2) =>
      stringCompare(r1.name as string, r2.name as string),
    relationships: {},
  };

  class DummyInput implements Input {
    getSchema(): Schema {
      return {
        tableName: 'issue',
        primaryKey: ['id'],
        columns: {id: {type: 'number'}, title: {type: 'string'}},
        sort: [['id', 'asc']],
        isHidden: false,
        compareRows: (r1, r2) => (r1.id as number) - (r2.id as number),
        relationships: {
          labels: labelSchema,
        },
      };
    }
    fetch(): Stream<Node> {
      unreachable();
    }
    cleanup(): Stream<Node> {
      unreachable();
    }
    setOutput(): void {}
    destroy(): void {
      unreachable();
    }
  }

  const input = new DummyInput();
  const view = new ArrayView(input);
  view.push({
    type: 'add',
    node: {
      row: {id: 1, title: 'issue1'},
      relationships: {
        labels: [
          {
            row: {id: 1, name: 'label1'},
            relationships: {},
          },
        ],
      },
    },
  });
  view.push({
    type: 'add',
    node: {
      row: {id: 2, title: 'issue2'},
      relationships: {
        labels: [
          {
            row: {id: 2, name: 'label2'},
            relationships: {},
          },
        ],
      },
    },
  });
  let data: unknown[] = [];
  view.addListener(entries => {
    data = [...entries];
  });
  view.flush();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "id": 1,
        "labels": [
          {
            "id": 1,
            "name": "label1",
          },
        ],
        "title": "issue1",
      },
      {
        "id": 2,
        "labels": [
          {
            "id": 2,
            "name": "label2",
          },
        ],
        "title": "issue2",
      },
    ]
  `);

  view.push({
    type: 'edit',
    oldRow: {id: 1, title: 'issue1'},
    row: {id: 1, title: 'issue1 changed'},
  });
  view.flush();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "id": 1,
        "labels": [
          {
            "id": 1,
            "name": "label1",
          },
        ],
        "title": "issue1 changed",
      },
      {
        "id": 2,
        "labels": [
          {
            "id": 2,
            "name": "label2",
          },
        ],
        "title": "issue2",
      },
    ]
  `);

  // And now edit to change order
  view.push({
    type: 'edit',
    oldRow: {id: 1, title: 'issue1 changed'},
    row: {id: 3, title: 'issue1 is now issue3'},
  });
  view.flush();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "id": 2,
        "labels": [
          {
            "id": 2,
            "name": "label2",
          },
        ],
        "title": "issue2",
      },
      {
        "id": 3,
        "labels": [
          {
            "id": 1,
            "name": "label1",
          },
        ],
        "title": "issue1 is now issue3",
      },
    ]
  `);
});
