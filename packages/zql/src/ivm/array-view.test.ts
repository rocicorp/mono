import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {assertArray, unreachable} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {stringCompare} from '../../../shared/src/string-compare.ts';
import {ArrayView} from './array-view.ts';
import type {Change} from './change.ts';
import {Join} from './join.ts';
import {MemoryStorage} from './memory-storage.ts';
import type {Input} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {Take} from './take.ts';
import {createSource} from './test/source-factory.ts';
import {idSymbol, refCountSymbol} from './view-apply-change.ts';

const lc = createSilentLogContext();

test('basics', () => {
  const ms = createSource(
    lc,
    testLogConfig,
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
    {singular: false, relationships: {}},
    true,
    () => {},
  );

  let callCount = 0;
  let data: ReadonlyJSONValue[] = [];
  const unlisten = view.addListener(entries => {
    ++callCount;
    assertArray(entries);
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore - stuck with `infinite depth` errors
    data = [...entries] as ReadonlyJSONValue[];
  });

  expect(data).toEqual([
    {
      a: 1,
      b: 'a',
      [refCountSymbol]: 1,
      [idSymbol]: '',
    },
    {
      a: 2,
      b: 'b',
      [refCountSymbol]: 1,
      [idSymbol]: '',
    },
  ]);

  expect(callCount).toBe(1);

  ms.push({row: {a: 3, b: 'c'}, type: 'add'});

  // We don't get called until flush.
  expect(callCount).toBe(1);

  view.flush();
  expect(callCount).toBe(2);
  expect(data).toEqual([
    {
      a: 1,
      b: 'a',
      [refCountSymbol]: 1,
      [idSymbol]: '',
    },
    {
      a: 2,
      b: 'b',
      [refCountSymbol]: 1,
      [idSymbol]: '',
    },
    {
      a: 3,
      b: 'c',
      [refCountSymbol]: 1,
      [idSymbol]: '',
    },
  ]);

  ms.push({row: {a: 2, b: 'b'}, type: 'remove'});
  expect(callCount).toBe(2);
  ms.push({row: {a: 1, b: 'a'}, type: 'remove'});
  expect(callCount).toBe(2);

  view.flush();
  expect(callCount).toBe(3);
  expect(data).toEqual([
    {
      a: 3,
      b: 'c',
      [refCountSymbol]: 1,
      [idSymbol]: '',
    },
  ]);

  unlisten();
  ms.push({row: {a: 3, b: 'c'}, type: 'remove'});
  expect(callCount).toBe(3);

  view.flush();
  expect(callCount).toBe(3);
  expect(view.data).toEqual([]);
  // The data remains but the rc gets updated.
  expect(data).toEqual([
    {
      a: 3,
      b: 'c',
      [refCountSymbol]: 0,
      [idSymbol]: '',
    },
  ]);
});

test('single-format', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});

  const view = new ArrayView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    {singular: true, relationships: {}},
    true,
    () => {},
  );

  let callCount = 0;
  let data: unknown;
  const unlisten = view.addListener(d => {
    ++callCount;
    data = structuredClone(d);
  });

  expect(data).toEqual({a: 1, b: 'a'});
  expect(callCount).toBe(1);

  // trying to add another element should be an error
  // pipeline should have been configured with a limit of one
  expect(() => ms.push({row: {a: 2, b: 'b'}, type: 'add'})).toThrow(
    "Singular relationship '' should not have multiple rows. You may need to declare this relationship with the `many` helper instead of the `one` helper in your schema.",
  );

  // Adding the same element is not an error in the ArrayView but it is an error
  // in the Source. This case is tested in view-apply-change.ts.

  ms.push({row: {a: 1, b: 'a'}, type: 'remove'});

  // no call until flush
  expect(data).toEqual({a: 1, b: 'a'});
  expect(callCount).toBe(1);
  view.flush();

  expect(data).toEqual(undefined);
  expect(callCount).toBe(2);

  unlisten();
});

test('hydrate-empty', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );

  const view = new ArrayView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    {singular: false, relationships: {}},
    true,
    () => {},
  );

  let callCount = 0;
  let data: unknown[] = [];
  view.addListener(entries => {
    ++callCount;
    assertArray(entries);
    data = [...entries];
  });

  expect(data).toEqual([]);
  expect(callCount).toBe(1);
});

test('tree', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {id: {type: 'number'}, name: {type: 'string'}, childID: {type: 'number'}},
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
    parentKey: ['childID'],
    childKey: ['id'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });

  const view = new ArrayView(
    join,
    {
      singular: false,
      relationships: {children: {singular: false, relationships: {}}},
    },
    true,
    () => {},
  );
  let data: unknown[] = [];
  view.addListener(entries => {
    assertArray(entries);
    data = [...entries];
  });

  expect(data).toMatchInlineSnapshot(`
    [
      {
        "childID": 2,
        "children": [
          {
            "childID": null,
            "id": 2,
            "name": "foobar",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": null,
        "children": [],
        "id": 2,
        "name": "foobar",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": 4,
        "children": [
          {
            "childID": null,
            "id": 4,
            "name": "monkey",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": null,
        "children": [],
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  // add parent with child
  ms.push({type: 'add', row: {id: 5, name: 'chocolate', childID: 2}});
  view.flush();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "childID": 2,
        "children": [
          {
            "childID": null,
            "id": 2,
            "name": "foobar",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "id": 5,
        "name": "chocolate",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": 2,
        "children": [
          {
            "childID": null,
            "id": 2,
            "name": "foobar",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": null,
        "children": [],
        "id": 2,
        "name": "foobar",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": 4,
        "children": [
          {
            "childID": null,
            "id": 4,
            "name": "monkey",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": null,
        "children": [],
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  // remove parent with child
  ms.push({type: 'remove', row: {id: 5, name: 'chocolate', childID: 2}});
  view.flush();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "childID": 2,
        "children": [
          {
            "childID": null,
            "id": 2,
            "name": "foobar",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": null,
        "children": [],
        "id": 2,
        "name": "foobar",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": 4,
        "children": [
          {
            "childID": null,
            "id": 4,
            "name": "monkey",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": null,
        "children": [],
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

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
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "childID": 2,
        "children": [],
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": 4,
        "children": [
          {
            "childID": null,
            "id": 4,
            "name": "monkey",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": null,
        "children": [],
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

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
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "childID": 2,
        "children": [
          {
            "childID": null,
            "id": 2,
            "name": "foobaz",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": null,
        "children": [],
        "id": 2,
        "name": "foobaz",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": 4,
        "children": [
          {
            "childID": null,
            "id": 4,
            "name": "monkey",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": null,
        "children": [],
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);
});

test('tree-single', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {id: {type: 'number'}, name: {type: 'string'}, childID: {type: 'number'}},
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

  const take = new Take(
    ms.connect([
      ['name', 'asc'],
      ['id', 'asc'],
    ]),
    new MemoryStorage(),
    1,
  );

  const join = new Join({
    parent: take,
    child: ms.connect([
      ['name', 'desc'],
      ['id', 'desc'],
    ]),
    storage: new MemoryStorage(),
    parentKey: ['childID'],
    childKey: ['id'],
    relationshipName: 'child',
    hidden: false,
    system: 'client',
  });

  const view = new ArrayView(
    join,
    {
      singular: true,
      relationships: {child: {singular: true, relationships: {}}},
    },
    true,
    () => {},
  );
  let data: unknown;
  view.addListener(d => {
    data = structuredClone(d);
  });

  expect(data).toEqual({
    id: 1,
    name: 'foo',
    childID: 2,
    child: {
      id: 2,
      name: 'foobar',
      childID: null,
    },
  });

  // remove the child
  ms.push({
    type: 'remove',
    row: {id: 2, name: 'foobar', childID: null},
  });
  view.flush();

  expect(data).toEqual({
    id: 1,
    name: 'foo',
    childID: 2,
    child: undefined,
  });

  // remove the parent
  ms.push({
    type: 'remove',
    row: {id: 1, name: 'foo', childID: 2},
  });
  view.flush();
  expect(data).toEqual(undefined);
});

test('collapse', () => {
  const schema: SourceSchema = {
    tableName: 'issue',
    primaryKey: ['id'],
    system: 'client',
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
        system: 'client',
        columns: {
          id: {type: 'number'},
          issueId: {type: 'number'},
          labelId: {type: 'number'},
          extra: {type: 'string'},
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
            system: 'client',
            compareRows: (r1, r2) => (r1.id as number) - (r2.id as number),
            relationships: {},
          },
        },
      },
    },
  };

  const input: Input = {
    cleanup() {
      return [];
    },
    fetch() {
      return [];
    },
    destroy() {},
    getSchema() {
      return schema;
    },
    setOutput() {},
  };

  const view = new ArrayView(
    input,
    {
      singular: false,
      relationships: {labels: {singular: false, relationships: {}}},
    },
    true,
    () => {},
  );
  let data: unknown[] = [];
  view.addListener(entries => {
    assertArray(entries);
    data = [...entries];
  });

  const changeSansType = {
    node: {
      row: {
        id: 1,
        name: 'issue',
      },
      relationships: {
        labels: () => [
          {
            row: {
              id: 1,
              issueId: 1,
              labelId: 1,
              extra: 'a',
            },
            relationships: {
              labels: () => [
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

  expect(data).toMatchInlineSnapshot(`
    [
      {
        "id": 1,
        "labels": [
          {
            "id": 1,
            "name": "label",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "name": "issue",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  view.push({
    type: 'remove',
    ...changeSansType,
  });
  view.flush();

  expect(data).toMatchInlineSnapshot(`[]`);

  view.push({
    type: 'add',
    ...changeSansType,
  });
  // no commit
  expect(data).toMatchInlineSnapshot(`[]`);

  view.push({
    type: 'child',
    node: {
      row: {
        id: 1,
        name: 'issue',
      },
      relationships: {
        labels: () => [
          {
            row: {
              id: 1,
              issueId: 1,
              labelId: 1,
              extra: 'a',
            },
            relationships: {
              labels: () => [
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
          {
            row: {
              id: 2,
              issueId: 1,
              labelId: 2,
              extra: 'b',
            },
            relationships: {
              labels: () => [
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
        ],
      },
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
            extra: 'b',
          },
          relationships: {
            labels: () => [
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

  expect(data).toMatchInlineSnapshot(`
    [
      {
        "id": 1,
        "labels": [
          {
            "id": 1,
            "name": "label",
            Symbol(rc): 1,
            Symbol(id): "",
          },
          {
            "id": 2,
            "name": "label2",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "name": "issue",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  // edit the hidden row
  view.push({
    type: 'child',
    node: {
      row: {
        id: 1,
        name: 'issue',
      },
      relationships: {
        labels: () => [
          {
            row: {
              id: 1,
              issueId: 1,
              labelId: 1,
              extra: 'a',
            },
            relationships: {
              labels: () => [
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
          {
            row: {
              id: 2,
              issueId: 1,
              labelId: 2,
              extra: 'b2',
            },
            relationships: {
              labels: () => [
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
        ],
      },
    },
    child: {
      relationshipName: 'labels',
      change: {
        type: 'edit',
        oldNode: {
          row: {
            id: 2,
            issueId: 1,
            labelId: 2,
            extra: 'b',
          },
          relationships: {
            labels: () => [
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
        node: {
          row: {
            id: 2,
            issueId: 1,
            labelId: 2,
            extra: 'b2',
          },
          relationships: {
            labels: () => [
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

  expect(data).toMatchInlineSnapshot(`
    [
      {
        "id": 1,
        "labels": [
          {
            "id": 1,
            "name": "label",
            Symbol(rc): 1,
            Symbol(id): "",
          },
          {
            "id": 2,
            "name": "label2",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "name": "issue",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  // edit the leaf
  view.push({
    type: 'child',
    node: {
      row: {
        id: 1,
        name: 'issue',
      },
      relationships: {
        labels: () => [
          {
            row: {
              id: 1,
              issueId: 1,
              labelId: 1,
              extra: 'a',
            },
            relationships: {
              labels: () => [
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
          {
            row: {
              id: 2,
              issueId: 1,
              labelId: 2,
              extra: 'b2',
            },
            relationships: {
              labels: () => [
                {
                  row: {
                    id: 2,
                    name: 'label2x',
                  },
                  relationships: {},
                },
              ],
            },
          },
        ],
      },
    },
    child: {
      relationshipName: 'labels',
      change: {
        type: 'child',
        node: {
          row: {
            id: 2,
            issueId: 1,
            labelId: 2,
            extra: 'b2',
          },
          relationships: {
            labels: () => [
              {
                row: {
                  id: 2,
                  name: 'label2x',
                },
                relationships: {},
              },
            ],
          },
        },
        child: {
          relationshipName: 'labels',
          change: {
            type: 'edit',
            oldNode: {
              row: {
                id: 2,
                name: 'label2',
              },
              relationships: {},
            },
            node: {
              row: {
                id: 2,
                name: 'label2x',
              },
              relationships: {},
            },
          },
        },
      },
    },
  });
  view.flush();

  expect(data).toMatchInlineSnapshot(`
    [
      {
        "id": 1,
        "labels": [
          {
            "id": 1,
            "name": "label",
            Symbol(rc): 1,
            Symbol(id): "",
          },
          {
            "id": 2,
            "name": "label2x",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "name": "issue",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);
});

test('collapse-single', () => {
  const schema: SourceSchema = {
    tableName: 'issue',
    primaryKey: ['id'],
    system: 'client',
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
        system: 'client',
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
            system: 'client',
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
      return [];
    },
    fetch() {
      return [];
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

  const view = new ArrayView(
    input,
    {
      singular: false,
      relationships: {labels: {singular: true, relationships: {}}},
    },
    true,
    () => {},
  );
  let data: unknown;
  view.addListener(d => {
    data = structuredClone(d);
  });

  const changeSansType = {
    node: {
      row: {
        id: 1,
        name: 'issue',
      },
      relationships: {
        labels: () => [
          {
            row: {
              id: 1,
              issueId: 1,
              labelId: 1,
            },
            relationships: {
              labels: () => [
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
      labels: {
        id: 1,
        name: 'label',
      },
      name: 'issue',
    },
  ]);
});

test('basic with edit pushes', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  const view = new ArrayView(
    ms.connect([['a', 'asc']]),
    {singular: false, relationships: {}},
    true,
    () => {},
  );

  let callCount = 0;
  let data: unknown[] = [];
  const unlisten = view.addListener(entries => {
    ++callCount;
    assertArray(entries);
    data = [...entries];
  });

  expect(data).toMatchInlineSnapshot(`
    [
      {
        "a": 1,
        "b": "a",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "a": 2,
        "b": "b",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  expect(callCount).toBe(1);

  ms.push({type: 'edit', row: {a: 2, b: 'b2'}, oldRow: {a: 2, b: 'b'}});

  // We don't get called until flush.
  expect(callCount).toBe(1);

  view.flush();
  expect(callCount).toBe(2);
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "a": 1,
        "b": "a",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "a": 2,
        "b": "b2",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  ms.push({type: 'edit', row: {a: 3, b: 'b3'}, oldRow: {a: 2, b: 'b2'}});

  view.flush();
  expect(callCount).toBe(3);
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "a": 1,
        "b": "a",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "a": 3,
        "b": "b3",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  unlisten();
});

test('tree edit', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {
      id: {type: 'number'},
      name: {type: 'string'},
      data: {type: 'string'},
      childID: {type: 'number'},
    },
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
    parentKey: ['childID'],
    childKey: ['id'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });

  const view = new ArrayView(
    join,
    {
      singular: false,
      relationships: {children: {singular: false, relationships: {}}},
    },
    true,
    () => {},
  );
  let data: unknown[] = [];
  view.addListener(entries => {
    assertArray(entries);
    data = [...entries];
  });

  expect(data).toMatchInlineSnapshot(`
    [
      {
        "childID": 2,
        "children": [
          {
            "childID": null,
            "data": "b",
            "id": 2,
            "name": "foobar",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "data": "a",
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": null,
        "children": [],
        "data": "b",
        "id": 2,
        "name": "foobar",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": 4,
        "children": [
          {
            "childID": null,
            "data": "d",
            "id": 4,
            "name": "monkey",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "data": "c",
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": null,
        "children": [],
        "data": "d",
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  // Edit root
  ms.push({
    type: 'edit',
    oldRow: {id: 1, name: 'foo', data: 'a', childID: 2},
    row: {id: 1, name: 'foo', data: 'a2', childID: 2},
  });
  view.flush();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "childID": 2,
        "children": [
          {
            "childID": null,
            "data": "b",
            "id": 2,
            "name": "foobar",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "data": "a2",
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": null,
        "children": [],
        "data": "b",
        "id": 2,
        "name": "foobar",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": 4,
        "children": [
          {
            "childID": null,
            "data": "d",
            "id": 4,
            "name": "monkey",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "data": "c",
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "childID": null,
        "children": [],
        "data": "d",
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);
});

test('edit to change the order', () => {
  const ms = createSource(
    lc,
    testLogConfig,
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

  const view = new ArrayView(
    ms.connect([['a', 'asc']]),
    {singular: false, relationships: {}},
    true,
    () => {},
  );
  let data: unknown[] = [];
  view.addListener(entries => {
    assertArray(entries);
    data = [...entries];
  });

  expect(data).toMatchInlineSnapshot(`
    [
      {
        "a": 10,
        "b": "a",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "a": 20,
        "b": "b",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "a": 30,
        "b": "c",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  ms.push({
    type: 'edit',
    oldRow: {a: 20, b: 'b'},
    row: {a: 5, b: 'b2'},
  });
  view.flush();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "a": 5,
        "b": "b2",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "a": 10,
        "b": "a",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "a": 30,
        "b": "c",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  ms.push({
    type: 'edit',
    oldRow: {a: 5, b: 'b2'},
    row: {a: 4, b: 'b3'},
  });

  view.flush();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "a": 4,
        "b": "b3",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "a": 10,
        "b": "a",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "a": 30,
        "b": "c",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  ms.push({
    type: 'edit',
    oldRow: {a: 4, b: 'b3'},
    row: {a: 20, b: 'b4'},
  });
  view.flush();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "a": 10,
        "b": "a",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "a": 20,
        "b": "b4",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "a": 30,
        "b": "c",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);
});

test('edit to preserve relationships', () => {
  const schema: SourceSchema = {
    tableName: 'issue',
    primaryKey: ['id'],
    system: 'client',
    columns: {id: {type: 'number'}, title: {type: 'string'}},
    sort: [['id', 'asc']],
    isHidden: false,
    compareRows: (r1, r2) => (r1.id as number) - (r2.id as number),
    relationships: {
      labels: {
        tableName: 'label',
        primaryKey: ['id'],
        system: 'client',
        columns: {id: {type: 'number'}, name: {type: 'string'}},
        sort: [['name', 'asc']],
        isHidden: false,
        compareRows: (r1, r2) =>
          stringCompare(r1.name as string, r2.name as string),
        relationships: {},
      },
    },
  };

  const input: Input = {
    getSchema() {
      return schema;
    },
    fetch() {
      return [];
    },
    cleanup() {
      return [];
    },
    setOutput() {},
    destroy() {
      unreachable();
    },
  };

  const view = new ArrayView(
    input,
    {
      singular: false,
      relationships: {labels: {singular: false, relationships: {}}},
    },
    true,
    () => void 0,
  );
  view.push({
    type: 'add',
    node: {
      row: {id: 1, title: 'issue1'},
      relationships: {
        labels: () => [
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
        labels: () => [
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
    assertArray(entries);
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
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "title": "issue1",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "id": 2,
        "labels": [
          {
            "id": 2,
            "name": "label2",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "title": "issue2",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  view.push({
    type: 'edit',
    oldNode: {
      row: {id: 1, title: 'issue1'},
      relationships: {},
    },
    node: {row: {id: 1, title: 'issue1 changed'}, relationships: {}},
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
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "title": "issue1 changed",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "id": 2,
        "labels": [
          {
            "id": 2,
            "name": "label2",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "title": "issue2",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);

  // And now edit to change order
  view.push({
    type: 'edit',
    oldNode: {row: {id: 1, title: 'issue1 changed'}, relationships: {}},
    node: {row: {id: 3, title: 'issue1 is now issue3'}, relationships: {}},
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
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "title": "issue2",
        Symbol(rc): 1,
        Symbol(id): "",
      },
      {
        "id": 3,
        "labels": [
          {
            "id": 1,
            "name": "label1",
            Symbol(rc): 1,
            Symbol(id): "",
          },
        ],
        "title": "issue1 is now issue3",
        Symbol(rc): 1,
        Symbol(id): "",
      },
    ]
  `);
});
