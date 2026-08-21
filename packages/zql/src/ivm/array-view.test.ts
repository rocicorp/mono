import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {assertArray, unreachable} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {stringCompare} from '../../../shared/src/string-compare.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import {buildPipeline} from '../builder/builder.ts';
import {TestBuilderDelegate} from '../builder/test-builder-delegate.ts';
import type {ResultType} from '../query/typed-view.ts';
import {ArrayView} from './array-view.ts';
import type {Change} from './change.ts';
import {
  makeAddChange,
  makeChildChange,
  makeEditChange,
  makeRemoveChange,
} from './change.ts';
import {Join} from './join.ts';
import {MemoryStorage} from './memory-storage.ts';
import type {Input} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from './source.ts';
import {consume} from './stream.ts';
import {Take} from './take.ts';
import {createSource} from './test/source-factory.ts';
import {refCountSymbol} from './view-apply-change.ts';

const lc = createSilentLogContext();

test('basics', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  consume(ms.push(makeSourceChangeAdd({a: 1, b: 'a'})));
  consume(ms.push(makeSourceChangeAdd({a: 2, b: 'b'})));

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
    // @ts-ignore - stuck with `infinite depth` errors
    data = [...entries] as ReadonlyJSONValue[];
  });

  expect(data).toEqual([
    {
      a: 1,
      b: 'a',
      [refCountSymbol]: 1,
    },
    {
      a: 2,
      b: 'b',
      [refCountSymbol]: 1,
    },
  ]);

  expect(callCount).toBe(1);

  consume(ms.push(makeSourceChangeAdd({a: 3, b: 'c'})));

  // We don't get called until flush.
  expect(callCount).toBe(1);

  view.flush();
  expect(callCount).toBe(2);
  expect(data).toEqual([
    {
      a: 1,
      b: 'a',
      [refCountSymbol]: 1,
    },
    {
      a: 2,
      b: 'b',
      [refCountSymbol]: 1,
    },
    {
      a: 3,
      b: 'c',
      [refCountSymbol]: 1,
    },
  ]);

  consume(ms.push(makeSourceChangeRemove({a: 2, b: 'b'})));
  expect(callCount).toBe(2);
  consume(ms.push(makeSourceChangeRemove({a: 1, b: 'a'})));
  expect(callCount).toBe(2);

  view.flush();
  expect(callCount).toBe(3);
  expect(data).toEqual([
    {
      a: 3,
      b: 'c',
      [refCountSymbol]: 1,
    },
  ]);

  unlisten();
  consume(ms.push(makeSourceChangeRemove({a: 3, b: 'c'})));
  expect(callCount).toBe(3);

  view.flush();
  expect(callCount).toBe(3);
  expect(view.data).toEqual([]);
  // applyChange is immutable: the snapshot captured by the (now removed)
  // listener is frozen and is not mutated by the later remove, so it keeps the
  // entry with its original refCount rather than seeing the in-place decrement.
  expect(data).toEqual([
    {
      a: 3,
      b: 'c',
      [refCountSymbol]: 1,
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
  consume(ms.push(makeSourceChangeAdd({a: 1, b: 'a'})));

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
  expect(() => consume(ms.push(makeSourceChangeAdd({a: 2, b: 'b'})))).toThrow(
    "Singular relationship '' should not have multiple rows. You may need to declare this relationship with the `many` helper instead of the `one` helper in your schema.",
  );

  // Adding the same element is not an error in the ArrayView but it is an error
  // in the Source. This case is tested in view-apply-change.ts.

  consume(ms.push(makeSourceChangeRemove({a: 1, b: 'a'})));

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
  consume(ms.push(makeSourceChangeAdd({id: 1, name: 'foo', childID: 2})));
  consume(ms.push(makeSourceChangeAdd({id: 2, name: 'foobar', childID: null})));
  consume(ms.push(makeSourceChangeAdd({id: 3, name: 'mon', childID: 4})));
  consume(ms.push(makeSourceChangeAdd({id: 4, name: 'monkey', childID: null})));

  const join = new Join({
    parent: ms.connect([
      ['name', 'asc'],
      ['id', 'asc'],
    ]),
    child: ms.connect([
      ['name', 'desc'],
      ['id', 'desc'],
    ]),
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
          },
        ],
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
      },
      {
        "childID": null,
        "children": [],
        "id": 2,
        "name": "foobar",
        Symbol(rc): 1,
      },
      {
        "childID": 4,
        "children": [
          {
            "childID": null,
            "id": 4,
            "name": "monkey",
            Symbol(rc): 1,
          },
        ],
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
      },
      {
        "childID": null,
        "children": [],
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
      },
    ]
  `);

  // add parent with child
  consume(ms.push(makeSourceChangeAdd({id: 5, name: 'chocolate', childID: 2})));
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
          },
        ],
        "id": 5,
        "name": "chocolate",
        Symbol(rc): 1,
      },
      {
        "childID": 2,
        "children": [
          {
            "childID": null,
            "id": 2,
            "name": "foobar",
            Symbol(rc): 1,
          },
        ],
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
      },
      {
        "childID": null,
        "children": [],
        "id": 2,
        "name": "foobar",
        Symbol(rc): 1,
      },
      {
        "childID": 4,
        "children": [
          {
            "childID": null,
            "id": 4,
            "name": "monkey",
            Symbol(rc): 1,
          },
        ],
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
      },
      {
        "childID": null,
        "children": [],
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
      },
    ]
  `);

  // remove parent with child
  consume(
    ms.push(makeSourceChangeRemove({id: 5, name: 'chocolate', childID: 2})),
  );
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
          },
        ],
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
      },
      {
        "childID": null,
        "children": [],
        "id": 2,
        "name": "foobar",
        Symbol(rc): 1,
      },
      {
        "childID": 4,
        "children": [
          {
            "childID": null,
            "id": 4,
            "name": "monkey",
            Symbol(rc): 1,
          },
        ],
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
      },
      {
        "childID": null,
        "children": [],
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
      },
    ]
  `);

  // remove just child
  consume(
    ms.push(
      makeSourceChangeRemove({
        id: 2,
        name: 'foobar',
        childID: null,
      }),
    ),
  );
  view.flush();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "childID": 2,
        "children": [],
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
      },
      {
        "childID": 4,
        "children": [
          {
            "childID": null,
            "id": 4,
            "name": "monkey",
            Symbol(rc): 1,
          },
        ],
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
      },
      {
        "childID": null,
        "children": [],
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
      },
    ]
  `);

  // add child
  consume(
    ms.push(
      makeSourceChangeAdd({
        id: 2,
        name: 'foobaz',
        childID: null,
      }),
    ),
  );
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
          },
        ],
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
      },
      {
        "childID": null,
        "children": [],
        "id": 2,
        "name": "foobaz",
        Symbol(rc): 1,
      },
      {
        "childID": 4,
        "children": [
          {
            "childID": null,
            "id": 4,
            "name": "monkey",
            Symbol(rc): 1,
          },
        ],
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
      },
      {
        "childID": null,
        "children": [],
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
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
  consume(ms.push(makeSourceChangeAdd({id: 1, name: 'foo', childID: 2})));
  consume(ms.push(makeSourceChangeAdd({id: 2, name: 'foobar', childID: null})));

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
  consume(
    ms.push(makeSourceChangeRemove({id: 2, name: 'foobar', childID: null})),
  );
  view.flush();

  expect(data).toEqual({
    id: 1,
    name: 'foo',
    childID: 2,
    child: undefined,
  });

  // remove the parent
  consume(ms.push(makeSourceChangeRemove({id: 1, name: 'foo', childID: 2})));
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
  consume(view.push(makeAddChange(changeSansType.node)));
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
          },
        ],
        "name": "issue",
        Symbol(rc): 1,
      },
    ]
  `);

  consume(view.push(makeRemoveChange(changeSansType.node)));
  view.flush();

  expect(data).toMatchInlineSnapshot(`[]`);

  consume(view.push(makeAddChange(changeSansType.node)));
  // no commit
  expect(data).toMatchInlineSnapshot(`[]`);

  consume(
    view.push(
      makeChildChange(
        {
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
        {
          relationshipName: 'labels',
          change: makeAddChange({
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
          }),
        },
      ),
    ),
  );
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
          },
          {
            "id": 2,
            "name": "label2",
            Symbol(rc): 1,
          },
        ],
        "name": "issue",
        Symbol(rc): 1,
      },
    ]
  `);

  // edit the hidden row
  consume(
    view.push(
      makeChildChange(
        {
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
        {
          relationshipName: 'labels',
          change: makeEditChange(
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
          ),
        },
      ),
    ),
  );
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
          },
          {
            "id": 2,
            "name": "label2",
            Symbol(rc): 1,
          },
        ],
        "name": "issue",
        Symbol(rc): 1,
      },
    ]
  `);

  // edit the leaf
  consume(
    view.push(
      makeChildChange(
        {
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
        {
          relationshipName: 'labels',
          change: makeChildChange(
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
            {
              relationshipName: 'labels',
              change: makeEditChange(
                {
                  row: {
                    id: 2,
                    name: 'label2x',
                  },
                  relationships: {},
                },
                {
                  row: {
                    id: 2,
                    name: 'label2',
                  },
                  relationships: {},
                },
              ),
            },
          ),
        },
      ),
    ),
  );
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
          },
          {
            "id": 2,
            "name": "label2x",
            Symbol(rc): 1,
          },
        ],
        "name": "issue",
        Symbol(rc): 1,
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
    fetch() {
      return [];
    },
    destroy() {},
    getSchema() {
      return schema;
    },
    setOutput() {},
    *push(change: Change) {
      yield* view.push(change);
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
  consume(view.push(makeAddChange(changeSansType.node)));
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
  consume(ms.push(makeSourceChangeAdd({a: 1, b: 'a'})));
  consume(ms.push(makeSourceChangeAdd({a: 2, b: 'b'})));

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
      },
      {
        "a": 2,
        "b": "b",
        Symbol(rc): 1,
      },
    ]
  `);

  expect(callCount).toBe(1);

  consume(ms.push(makeSourceChangeEdit({a: 2, b: 'b2'}, {a: 2, b: 'b'})));

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
      },
      {
        "a": 2,
        "b": "b2",
        Symbol(rc): 1,
      },
    ]
  `);

  consume(ms.push(makeSourceChangeEdit({a: 3, b: 'b3'}, {a: 2, b: 'b2'})));

  view.flush();
  expect(callCount).toBe(3);
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "a": 1,
        "b": "a",
        Symbol(rc): 1,
      },
      {
        "a": 3,
        "b": "b3",
        Symbol(rc): 1,
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
    consume(ms.push(makeSourceChangeAdd(row)));
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
          },
        ],
        "data": "a",
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
      },
      {
        "childID": null,
        "children": [],
        "data": "b",
        "id": 2,
        "name": "foobar",
        Symbol(rc): 1,
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
          },
        ],
        "data": "c",
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
      },
      {
        "childID": null,
        "children": [],
        "data": "d",
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
      },
    ]
  `);

  // Edit root
  consume(
    ms.push(
      makeSourceChangeEdit(
        {id: 1, name: 'foo', data: 'a2', childID: 2},
        {id: 1, name: 'foo', data: 'a', childID: 2},
      ),
    ),
  );
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
          },
        ],
        "data": "a2",
        "id": 1,
        "name": "foo",
        Symbol(rc): 1,
      },
      {
        "childID": null,
        "children": [],
        "data": "b",
        "id": 2,
        "name": "foobar",
        Symbol(rc): 1,
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
          },
        ],
        "data": "c",
        "id": 3,
        "name": "mon",
        Symbol(rc): 1,
      },
      {
        "childID": null,
        "children": [],
        "data": "d",
        "id": 4,
        "name": "monkey",
        Symbol(rc): 1,
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
    consume(ms.push(makeSourceChangeAdd(row)));
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
      },
      {
        "a": 20,
        "b": "b",
        Symbol(rc): 1,
      },
      {
        "a": 30,
        "b": "c",
        Symbol(rc): 1,
      },
    ]
  `);

  consume(ms.push(makeSourceChangeEdit({a: 5, b: 'b2'}, {a: 20, b: 'b'})));
  view.flush();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "a": 5,
        "b": "b2",
        Symbol(rc): 1,
      },
      {
        "a": 10,
        "b": "a",
        Symbol(rc): 1,
      },
      {
        "a": 30,
        "b": "c",
        Symbol(rc): 1,
      },
    ]
  `);

  consume(ms.push(makeSourceChangeEdit({a: 4, b: 'b3'}, {a: 5, b: 'b2'})));

  view.flush();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "a": 4,
        "b": "b3",
        Symbol(rc): 1,
      },
      {
        "a": 10,
        "b": "a",
        Symbol(rc): 1,
      },
      {
        "a": 30,
        "b": "c",
        Symbol(rc): 1,
      },
    ]
  `);

  consume(ms.push(makeSourceChangeEdit({a: 20, b: 'b4'}, {a: 4, b: 'b3'})));
  view.flush();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "a": 10,
        "b": "a",
        Symbol(rc): 1,
      },
      {
        "a": 20,
        "b": "b4",
        Symbol(rc): 1,
      },
      {
        "a": 30,
        "b": "c",
        Symbol(rc): 1,
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
  consume(
    view.push(
      makeAddChange({
        row: {id: 1, title: 'issue1'},
        relationships: {
          labels: () => [
            {
              row: {id: 1, name: 'label1'},
              relationships: {},
            },
          ],
        },
      }),
    ),
  );
  consume(
    view.push(
      makeAddChange({
        row: {id: 2, title: 'issue2'},
        relationships: {
          labels: () => [
            {
              row: {id: 2, name: 'label2'},
              relationships: {},
            },
          ],
        },
      }),
    ),
  );
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
          },
        ],
        "title": "issue1",
        Symbol(rc): 1,
      },
      {
        "id": 2,
        "labels": [
          {
            "id": 2,
            "name": "label2",
            Symbol(rc): 1,
          },
        ],
        "title": "issue2",
        Symbol(rc): 1,
      },
    ]
  `);

  consume(
    view.push(
      makeEditChange(
        {row: {id: 1, title: 'issue1 changed'}, relationships: {}},
        {
          row: {id: 1, title: 'issue1'},
          relationships: {},
        },
      ),
    ),
  );
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
          },
        ],
        "title": "issue1 changed",
        Symbol(rc): 1,
      },
      {
        "id": 2,
        "labels": [
          {
            "id": 2,
            "name": "label2",
            Symbol(rc): 1,
          },
        ],
        "title": "issue2",
        Symbol(rc): 1,
      },
    ]
  `);

  // And now edit to change order
  consume(
    view.push(
      makeEditChange(
        {row: {id: 3, title: 'issue1 is now issue3'}, relationships: {}},
        {row: {id: 1, title: 'issue1 changed'}, relationships: {}},
      ),
    ),
  );
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
          },
        ],
        "title": "issue2",
        Symbol(rc): 1,
      },
      {
        "id": 3,
        "labels": [
          {
            "id": 1,
            "name": "label1",
            Symbol(rc): 1,
          },
        ],
        "title": "issue1 is now issue3",
        Symbol(rc): 1,
      },
    ]
  `);
});

test('listeners receive error when queryComplete rejects - plural', async () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  consume(ms.push(makeSourceChangeAdd({a: 1, b: 'a'})));
  consume(ms.push(makeSourceChangeAdd({a: 2, b: 'b'})));

  const testError: ErroredQuery = {
    error: 'app',
    id: 'test-error-1',
    name: 'error-query',
    message: 'Query execution failed',
    details: {reason: 'Test rejection'},
  };

  const queryCompletePromise = Promise.reject(testError);

  const view = new ArrayView(
    ms.connect([['a', 'asc']]),
    {singular: false, relationships: {}},
    queryCompletePromise, // Pass rejecting promise
    () => {},
  );

  let receivedData: unknown;
  let receivedResultType: ResultType | undefined;
  let receivedError: ErroredQuery | undefined;

  view.addListener((data, resultType, error) => {
    receivedData = data;
    receivedResultType = resultType;
    receivedError = error;
  });

  // Initial call should have unknown state with data
  expect(receivedResultType).toBe('unknown');
  expect(receivedData).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1},
    {a: 2, b: 'b', [refCountSymbol]: 1},
  ]);
  expect(receivedError).toBeUndefined();

  // Wait for promise rejection to propagate
  await new Promise(resolve => setTimeout(resolve, 0));

  // After rejection, should have error state
  expect(receivedResultType).toBe('error');
  expect(receivedData).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1},
    {a: 2, b: 'b', [refCountSymbol]: 1},
  ]);
  expect(receivedError).toEqual(testError);
});

test('listeners receive error when queryComplete rejects - singular', async () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  consume(ms.push(makeSourceChangeAdd({a: 1, b: 'a'})));

  const testError: ErroredQuery = {
    error: 'parse',
    id: 'singular-error',
    name: 'error-query-1',
    message: 'Singular query failed',
  };

  const queryCompletePromise = Promise.reject(testError);

  const view = new ArrayView(
    ms.connect([['a', 'asc']]),
    {singular: true, relationships: {}},
    queryCompletePromise,
    () => {},
  );

  let receivedData: unknown;
  let receivedResultType: ResultType | undefined;
  let receivedError: ErroredQuery | undefined;

  view.addListener((data, resultType, error) => {
    receivedData = data;
    receivedResultType = resultType;
    receivedError = error;
  });

  // Initial state
  expect(receivedResultType).toBe('unknown');
  expect(receivedData).toEqual({a: 1, b: 'a', [refCountSymbol]: 1});
  expect(receivedError).toBeUndefined();

  // Wait for rejection
  await new Promise(resolve => setTimeout(resolve, 0));

  // Error state - data preserved
  expect(receivedResultType).toBe('error');
  expect(receivedData).toEqual({a: 1, b: 'a', [refCountSymbol]: 1});
  expect(receivedError).toEqual(testError);
});

test('all listeners receive error when queryComplete rejects', async () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  consume(ms.push(makeSourceChangeAdd({a: 1, b: 'a'})));

  const testError: ErroredQuery = {
    error: 'parse',
    id: 'query-1',
    name: 'error-query-2',
    message: 'Query execution failed',
    details: {reason: 'Test rejection'},
  };

  const queryCompletePromise = Promise.reject(testError);

  const view = new ArrayView(
    ms.connect([['a', 'asc']]),
    {singular: false, relationships: {}},
    queryCompletePromise,
    () => {},
  );

  const listener1Results: ResultType[] = [];
  const listener2Results: ResultType[] = [];
  const listener1Errors: (ErroredQuery | undefined)[] = [];
  const listener2Errors: (ErroredQuery | undefined)[] = [];

  view.addListener((_data, resultType, error) => {
    listener1Results.push(resultType);
    listener1Errors.push(error);
  });

  view.addListener((_data, resultType, error) => {
    listener2Results.push(resultType);
    listener2Errors.push(error);
  });

  // Both get initial unknown state
  expect(listener1Results).toEqual(['unknown']);
  expect(listener2Results).toEqual(['unknown']);

  await new Promise(resolve => setTimeout(resolve, 0));

  // Both get error state
  expect(listener1Results).toEqual(['unknown', 'error']);
  expect(listener2Results).toEqual(['unknown', 'error']);
  expect(listener1Errors[1]).toEqual(testError);
  expect(listener2Errors[1]).toEqual(testError);
});

test('listeners added after error still receive error state', async () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  consume(ms.push(makeSourceChangeAdd({a: 1, b: 'a'})));

  const testError: ErroredQuery = {
    error: 'app',
    id: 'late-listener-error',
    name: 'error-query-3',
    message: 'Error before listener',
  };

  const queryCompletePromise = Promise.reject(testError);

  const view = new ArrayView(
    ms.connect([['a', 'asc']]),
    {singular: false, relationships: {}},
    queryCompletePromise,
    () => {},
  );

  // Wait for error to occur
  await new Promise(resolve => setTimeout(resolve, 0));

  // Add listener after error
  let receivedResultType: ResultType | undefined;
  let receivedError: ErroredQuery | undefined;

  view.addListener((_data, resultType, error) => {
    receivedResultType = resultType;
    receivedError = error;
  });

  // Should immediately receive error state
  expect(receivedResultType).toBe('error');
  expect(receivedError).toEqual(testError);
});

test('error state persists through flush operations', async () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  consume(ms.push(makeSourceChangeAdd({a: 1, b: 'a'})));

  const testError: ErroredQuery = {
    error: 'app',
    id: 'persistent-error',
    name: 'error-query',
    message: 'Persistent error',
  };

  const queryCompletePromise = Promise.reject(testError);

  const view = new ArrayView(
    ms.connect([['a', 'asc']]),
    {singular: false, relationships: {}},
    queryCompletePromise,
    () => {},
  );

  let callCount = 0;
  let lastResultType: ResultType | undefined;

  view.addListener((_data, resultType, _error) => {
    callCount++;
    lastResultType = resultType;
  });

  await new Promise(resolve => setTimeout(resolve, 0));

  expect(lastResultType).toBe('error');
  const callsAfterError = callCount;

  // Add more data and flush
  consume(ms.push(makeSourceChangeAdd({a: 2, b: 'b'})));
  view.flush();

  // Should still be in error state
  expect(lastResultType).toBe('error');
  expect(callCount).toBeGreaterThan(callsAfterError);
});

/**
 * Tests that duplicate relationship aliases are handled via LWW (last writer wins).
 *
 * When the same alias is used for two related subqueries, only the last one
 * is used - earlier ones are dropped. This is consistent with other LWW
 * behaviors like limit(5).limit(10) where the second limit wins.
 */
test('duplicate relationship alias uses last-writer-wins', () => {
  const users = createSource(
    lc,
    testLogConfig,
    'users',
    {id: {type: 'string'}, name: {type: 'string'}},
    ['id'],
  );

  const candidateJobConnections = createSource(
    lc,
    testLogConfig,
    'candidate_job_connections',
    {
      id: {type: 'string'},
      candidate_id: {type: 'string'},
      job_id: {type: 'string'},
    },
    ['id'],
  );

  // Add user AND cjc BEFORE pipeline
  // The cjc has job_id='other-job', which won't match the filter 'target-job'
  consume(users.push(makeSourceChangeAdd({id: 'user-1', name: 'Test User'})));
  consume(
    candidateJobConnections.push(
      makeSourceChangeAdd({
        id: 'cjc-1',
        candidate_id: 'user-1',
        job_id: 'other-job',
      }),
    ),
  );

  const sources = {users, candidate_job_connections: candidateJobConnections};
  const delegate = new TestBuilderDelegate(sources);

  // Build query with DUPLICATE alias "candidate_applications"
  // The second (filtered) one should win via LWW
  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    related: [
      // First: unfiltered - will be DROPPED due to LWW
      {
        system: 'client',
        correlation: {
          parentField: ['id'],
          childField: ['candidate_id'],
        },
        subquery: {
          table: 'candidate_job_connections',
          alias: 'candidate_applications',
          orderBy: [['id', 'asc']],
        },
      },
      // Second: filtered - WINS via LWW
      {
        system: 'client',
        correlation: {
          parentField: ['id'],
          childField: ['candidate_id'],
        },
        subquery: {
          table: 'candidate_job_connections',
          alias: 'candidate_applications',
          orderBy: [['id', 'asc']],
          where: {
            type: 'simple',
            left: {type: 'column', name: 'job_id'},
            right: {type: 'literal', value: 'target-job'},
            op: '=',
          },
        },
      },
    ],
  };

  const pipeline = buildPipeline(ast, delegate, 'test-query');

  const view = new ArrayView(
    pipeline,
    {
      singular: false,
      relationships: {
        candidate_applications: {singular: false, relationships: {}},
      },
    },
    true,
    () => {},
  );

  let data: unknown[] = [];
  view.addListener(entries => {
    assertArray(entries);
    data = [...entries];
  });

  // Verify initial state - user exists with empty candidate_applications
  // (cjc-1 doesn't match the filter, and only the filtered relationship exists)
  expect(data).toHaveLength(1);
  const user = data[0] as {id: string; candidate_applications: unknown[]};
  expect(user.id).toBe('user-1');
  expect(user.candidate_applications).toHaveLength(0);

  // Delete the cjc - should NOT throw because only the filtered Join exists,
  // and the filtered source connection filters out the remove (predicate fails)
  expect(() => {
    consume(
      candidateJobConnections.push(
        makeSourceChangeRemove({
          id: 'cjc-1',
          candidate_id: 'user-1',
          job_id: 'other-job',
        }),
      ),
    );
    view.flush();
  }).not.toThrow();
});

test('unique relationship aliases work correctly', () => {
  const users = createSource(
    lc,
    testLogConfig,
    'users',
    {id: {type: 'string'}, name: {type: 'string'}},
    ['id'],
  );

  const candidateJobConnections = createSource(
    lc,
    testLogConfig,
    'candidate_job_connections',
    {
      id: {type: 'string'},
      candidate_id: {type: 'string'},
      job_id: {type: 'string'},
    },
    ['id'],
  );

  consume(users.push(makeSourceChangeAdd({id: 'user-1', name: 'Test User'})));
  consume(
    candidateJobConnections.push(
      makeSourceChangeAdd({
        id: 'cjc-1',
        candidate_id: 'user-1',
        job_id: 'other-job',
      }),
    ),
  );

  const sources = {users, candidate_job_connections: candidateJobConnections};
  const delegate = new TestBuilderDelegate(sources);

  // Build query with UNIQUE aliases
  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {
          parentField: ['id'],
          childField: ['candidate_id'],
        },
        subquery: {
          table: 'candidate_job_connections',
          alias: 'all_applications',
          orderBy: [['id', 'asc']],
        },
      },
      {
        system: 'client',
        correlation: {
          parentField: ['id'],
          childField: ['candidate_id'],
        },
        subquery: {
          table: 'candidate_job_connections',
          alias: 'this_job_application',
          orderBy: [['id', 'asc']],
          where: {
            type: 'simple',
            left: {type: 'column', name: 'job_id'},
            right: {type: 'literal', value: 'target-job'},
            op: '=',
          },
        },
      },
    ],
  };

  const pipeline = buildPipeline(ast, delegate, 'test-query');

  const view = new ArrayView(
    pipeline,
    {
      singular: false,
      relationships: {
        all_applications: {singular: false, relationships: {}},
        this_job_application: {singular: false, relationships: {}},
      },
    },
    true,
    () => {},
  );

  let data: unknown[] = [];
  view.addListener(entries => {
    assertArray(entries);
    data = [...entries];
  });

  expect(data).toHaveLength(1);
  const user = data[0] as {
    id: string;
    all_applications: unknown[];
    this_job_application: unknown[];
  };
  expect(user.id).toBe('user-1');
  expect(user.all_applications).toHaveLength(1);
  expect(user.this_job_application).toHaveLength(0);

  // Delete should work fine with unique aliases
  expect(() => {
    consume(
      candidateJobConnections.push(
        makeSourceChangeRemove({
          id: 'cjc-1',
          candidate_id: 'user-1',
          job_id: 'other-job',
        }),
      ),
    );
    view.flush();
  }).not.toThrow();

  const userAfter = data[0] as {all_applications: unknown[]};
  expect(userAfter.all_applications).toHaveLength(0);
});

/**
 * Tests that a change flowing through the pipeline that does not alter the
 * view output does not notify listeners.
 *
 * whereExists subqueries (zsubq_*) are present in the pipeline schema but not
 * in the view format. Edits to rows matched by the subquery propagate to the
 * view as child changes that alter nothing; the view must not flag itself
 * dirty for them, otherwise every subscriber re-renders with identical data.
 */
test('whereExists subquery edit does not notify listeners', () => {
  const issues = createSource(
    lc,
    testLogConfig,
    'issues',
    {id: {type: 'number'}, title: {type: 'string'}},
    ['id'],
  );
  const labels = createSource(
    lc,
    testLogConfig,
    'labels',
    {
      id: {type: 'number'},
      issueID: {type: 'number'},
      name: {type: 'string'},
    },
    ['id'],
  );

  consume(issues.push(makeSourceChangeAdd({id: 1, title: 'issue'})));
  consume(labels.push(makeSourceChangeAdd({id: 1, issueID: 1, name: 'bug'})));

  const delegate = new TestBuilderDelegate({issues, labels});

  // issues.whereExists('labels')
  const ast: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['issueID']},
        subquery: {
          table: 'labels',
          alias: 'zsubq_labels',
          orderBy: [
            ['issueID', 'asc'],
            ['id', 'asc'],
          ],
        },
      },
      op: 'EXISTS',
    },
  };

  const view = new ArrayView(
    buildPipeline(ast, delegate, 'test-query'),
    {singular: false, relationships: {}},
    true,
    () => {},
  );

  let callCount = 0;
  let data: unknown;
  view.addListener(entries => {
    ++callCount;
    data = entries;
  });

  expect(callCount).toBe(1);
  expect(data).toEqual([{id: 1, title: 'issue', [refCountSymbol]: 1}]);
  const snapshot = data;

  // Edit a field of the matched label that affects neither the correlation
  // nor the view output. The change reaches the view as a zsubq_labels child
  // change but alters nothing: no notification, same data reference.
  consume(
    labels.push(
      makeSourceChangeEdit(
        {id: 1, issueID: 1, name: 'bug!'},
        {id: 1, issueID: 1, name: 'bug'},
      ),
    ),
  );
  view.flush();
  expect(callCount).toBe(1);
  expect(view.data).toBe(snapshot);

  // A change that does alter the view still notifies.
  consume(
    issues.push(
      makeSourceChangeEdit({id: 1, title: 'issue!'}, {id: 1, title: 'issue'}),
    ),
  );
  view.flush();
  expect(callCount).toBe(2);
  expect(data).not.toBe(snapshot);
  expect(data).toEqual([{id: 1, title: 'issue!', [refCountSymbol]: 1}]);
});

test('no-op push does not dirty the view', () => {
  // A child change for a relationship that is in the pipeline schema but not
  // in the view format is a no-op for the view.
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
      ['zsubq_labels']: {
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
  };

  const input: Input = {
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
    // zsubq_labels is not part of the view format.
    {singular: false, relationships: {}},
    true,
    () => {},
  );

  let callCount = 0;
  let data: unknown;
  view.addListener(entries => {
    ++callCount;
    data = entries;
  });
  expect(callCount).toBe(1);

  const labelNode = {
    row: {id: 1, name: 'label'},
    relationships: {},
  } as const;
  consume(
    view.push(
      makeAddChange({
        row: {id: 1, name: 'issue'},
        relationships: {['zsubq_labels']: () => [labelNode]},
      }),
    ),
  );
  view.flush();
  expect(callCount).toBe(2);
  const snapshot = data;

  const noopChange = makeChildChange(
    {row: {id: 1, name: 'issue'}, relationships: {}},
    {
      relationshipName: 'zsubq_labels',
      change: makeEditChange(
        {row: {id: 1, name: 'label edited'}, relationships: {}},
        labelNode,
      ),
    },
  );
  consume(view.push(noopChange));
  view.flush();

  // No notification, same data reference.
  expect(callCount).toBe(2);
  expect(data).toBe(snapshot);
  expect(view.data).toBe(snapshot);

  // A transaction mixing no-op and real changes still notifies once.
  consume(view.push(noopChange));
  consume(
    view.push(
      makeAddChange({
        row: {id: 2, name: 'issue2'},
        relationships: {['zsubq_labels']: () => []},
      }),
    ),
  );
  consume(view.push(noopChange));
  view.flush();
  expect(callCount).toBe(3);
  expect(data).not.toBe(snapshot);
  expect(data).toEqual([
    {id: 1, name: 'issue', [refCountSymbol]: 1},
    {id: 2, name: 'issue2', [refCountSymbol]: 1},
  ]);
});

test('a listener throwing during flush does not freeze the view', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );

  const view = new ArrayView(
    ms.connect([['a', 'asc']]),
    {singular: false, relationships: {}},
    true,
    () => {},
  );

  let throwOnce = false;
  let callCount = 0;
  let data: unknown;
  view.addListener(entries => {
    ++callCount;
    data = entries;
    if (throwOnce) {
      throwOnce = false;
      throw new Error('listener error');
    }
  });
  expect(callCount).toBe(1);

  consume(ms.push(makeSourceChangeAdd({a: 1, b: 'a'})));
  throwOnce = true;
  // In production the throw is swallowed by ZeroContext#endTransaction.
  expect(() => view.flush()).toThrow('listener error');
  expect(callCount).toBe(2);
  const snapshot = data;

  // The next transaction must still copy-on-write and notify. If the throw
  // left the committed root marked as owned by the flushed transaction,
  // applyChange would mutate it in place and return an identical reference,
  // which push() would read as "no change" — silencing the view forever.
  consume(ms.push(makeSourceChangeAdd({a: 2, b: 'b'})));
  view.flush();
  expect(callCount).toBe(3);
  expect(data).not.toBe(snapshot);
  expect(data).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1},
    {a: 2, b: 'b', [refCountSymbol]: 1},
  ]);
  // The snapshot delivered before the throw was not mutated in place.
  expect(snapshot).toEqual([{a: 1, b: 'a', [refCountSymbol]: 1}]);
});

test('a listener pushing synchronously during flush is not lost', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );

  const view = new ArrayView(
    ms.connect([['a', 'asc']]),
    {singular: false, relationships: {}},
    true,
    () => {},
  );

  let pushOnce = false;
  let callCount = 0;
  let data: unknown;
  view.addListener(entries => {
    ++callCount;
    data = entries;
    if (pushOnce) {
      pushOnce = false;
      consume(ms.push(makeSourceChangeAdd({a: 2, b: 'b'})));
    }
  });
  expect(callCount).toBe(1);

  consume(ms.push(makeSourceChangeAdd({a: 1, b: 'a'})));
  pushOnce = true;
  view.flush();
  expect(callCount).toBe(2);
  const snapshot = data;
  // The re-entrant push must not mutate the just-delivered snapshot in place;
  // it belongs to the next transaction and is delivered by the next flush.
  expect(snapshot).toEqual([{a: 1, b: 'a', [refCountSymbol]: 1}]);

  view.flush();
  expect(callCount).toBe(3);
  expect(data).not.toBe(snapshot);
  expect(data).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1},
    {a: 2, b: 'b', [refCountSymbol]: 1},
  ]);
});

// Schema for the nested no-op tests: comments is in the view format, its
// zsubq_reactions sub-relationship (a whereExists subquery on the related
// query) is not.
function nestedNoopSchema(): SourceSchema {
  return {
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
      comments: {
        tableName: 'comment',
        primaryKey: ['id'],
        columns: {
          id: {type: 'number'},
          issueID: {type: 'number'},
          text: {type: 'string'},
        },
        isHidden: false,
        sort: [['id', 'asc']],
        system: 'client',
        compareRows: (r1, r2) => (r1.id as number) - (r2.id as number),
        relationships: {
          ['zsubq_reactions']: {
            tableName: 'reaction',
            primaryKey: ['id'],
            columns: {
              id: {type: 'number'},
              commentID: {type: 'number'},
              emoji: {type: 'string'},
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
}

function nestedNoopIssueNode() {
  return {
    row: {id: 1, name: 'issue'},
    relationships: {
      comments: () => [
        {
          row: {id: 1, issueID: 1, text: 'comment'},
          relationships: {
            ['zsubq_reactions']: () => [
              {
                row: {id: 1, commentID: 1, emoji: 'up'},
                relationships: {},
              },
            ],
          },
        },
      ],
    },
  };
}

// issue.related('comments', q => q.whereExists('reactions', ...)): an edit of
// a matched reaction arrives as a child change for the format-present
// comments relationship wrapping a zsubq change one level down. Suppressing
// the notification depends on applyChange's identity-propagation checks
// (return parentEntry when the descendant did not change).
function nestedNoopChange() {
  return makeChildChange(
    {row: {id: 1, name: 'issue'}, relationships: {}},
    {
      relationshipName: 'comments',
      change: makeChildChange(
        {row: {id: 1, issueID: 1, text: 'comment'}, relationships: {}},
        {
          relationshipName: 'zsubq_reactions',
          change: makeEditChange(
            {row: {id: 1, commentID: 1, emoji: 'down'}, relationships: {}},
            {row: {id: 1, commentID: 1, emoji: 'up'}, relationships: {}},
          ),
        },
      ),
    },
  );
}

test('nested no-op child change does not notify listeners', () => {
  const schema = nestedNoopSchema();
  const input: Input = {
    fetch() {
      return [nestedNoopIssueNode()];
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
      relationships: {comments: {singular: false, relationships: {}}},
    },
    true,
    () => {},
  );

  let callCount = 0;
  let data: unknown;
  view.addListener(entries => {
    ++callCount;
    data = entries;
  });
  expect(callCount).toBe(1);
  expect(data).toEqual([
    {
      id: 1,
      name: 'issue',
      comments: [{id: 1, issueID: 1, text: 'comment', [refCountSymbol]: 1}],
      [refCountSymbol]: 1,
    },
  ]);
  const snapshot = data;

  consume(view.push(nestedNoopChange()));
  view.flush();
  expect(callCount).toBe(1);
  expect(view.data).toBe(snapshot);
});

test('nested no-op child change does not notify listeners (singular root)', () => {
  const schema = nestedNoopSchema();
  const input: Input = {
    fetch() {
      return [nestedNoopIssueNode()];
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
      singular: true,
      relationships: {comments: {singular: false, relationships: {}}},
    },
    true,
    () => {},
  );

  let callCount = 0;
  let data: unknown;
  view.addListener(d => {
    ++callCount;
    data = d;
  });
  expect(callCount).toBe(1);
  const snapshot = data;

  consume(view.push(nestedNoopChange()));
  view.flush();
  expect(callCount).toBe(1);
  expect(view.data).toBe(snapshot);
});

// Schema for the hidden junction edit tests, matching the 'collapse' tests:
// issue -> issueLabel (hidden junction) -> label.
function hiddenJunctionSchema(): SourceSchema {
  return {
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
}

function hiddenJunctionIssueNode() {
  return {
    row: {id: 1, name: 'issue'},
    relationships: {
      labels: () => [
        {
          row: {id: 1, issueId: 1, labelId: 1, extra: 'a'},
          relationships: {
            labels: () => [
              {
                row: {id: 1, name: 'label'},
                relationships: {},
              },
            ],
          },
        },
      ],
    },
  };
}

// Edit of a non-key column of the hidden junction row. The junction row is
// collapsed out of the view, so this must not notify.
function hiddenJunctionEditChange() {
  return makeChildChange(
    {row: {id: 1, name: 'issue'}, relationships: {}},
    {
      relationshipName: 'labels',
      change: makeEditChange(
        {row: {id: 1, issueId: 1, labelId: 1, extra: 'a2'}, relationships: {}},
        {row: {id: 1, issueId: 1, labelId: 1, extra: 'a'}, relationships: {}},
      ),
    },
  );
}

// Edit of the visible leaf label, which must notify.
function hiddenJunctionLeafEditChange() {
  return makeChildChange(
    {row: {id: 1, name: 'issue'}, relationships: {}},
    {
      relationshipName: 'labels',
      change: makeChildChange(
        {row: {id: 1, issueId: 1, labelId: 1, extra: 'a2'}, relationships: {}},
        {
          relationshipName: 'labels',
          change: makeEditChange(
            {row: {id: 1, name: 'label2'}, relationships: {}},
            {row: {id: 1, name: 'label'}, relationships: {}},
          ),
        },
      ),
    },
  );
}

test('hidden junction row edit does not notify listeners', () => {
  const schema = hiddenJunctionSchema();
  const input: Input = {
    fetch() {
      return [hiddenJunctionIssueNode()];
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

  let callCount = 0;
  let data: unknown;
  view.addListener(entries => {
    ++callCount;
    data = entries;
  });
  expect(callCount).toBe(1);
  expect(data).toEqual([
    {
      id: 1,
      name: 'issue',
      labels: [{id: 1, name: 'label', [refCountSymbol]: 1}],
      [refCountSymbol]: 1,
    },
  ]);
  const snapshot = data;

  consume(view.push(hiddenJunctionEditChange()));
  view.flush();
  expect(callCount).toBe(1);
  expect(view.data).toBe(snapshot);

  // An edit of the visible leaf still notifies with a new reference.
  consume(view.push(hiddenJunctionLeafEditChange()));
  view.flush();
  expect(callCount).toBe(2);
  expect(data).not.toBe(snapshot);
  expect(data).toEqual([
    {
      id: 1,
      name: 'issue',
      labels: [{id: 1, name: 'label2', [refCountSymbol]: 1}],
      [refCountSymbol]: 1,
    },
  ]);
});

test('hidden junction row edit does not notify listeners (singular child format)', () => {
  const schema = hiddenJunctionSchema();
  const input: Input = {
    fetch() {
      return [hiddenJunctionIssueNode()];
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
      relationships: {labels: {singular: true, relationships: {}}},
    },
    true,
    () => {},
  );

  let callCount = 0;
  let data: unknown;
  view.addListener(entries => {
    ++callCount;
    data = entries;
  });
  expect(callCount).toBe(1);
  expect(data).toEqual([
    {
      id: 1,
      name: 'issue',
      labels: {id: 1, name: 'label', [refCountSymbol]: 1},
      [refCountSymbol]: 1,
    },
  ]);
  const snapshot = data;

  consume(view.push(hiddenJunctionEditChange()));
  view.flush();
  expect(callCount).toBe(1);
  expect(view.data).toBe(snapshot);
});
