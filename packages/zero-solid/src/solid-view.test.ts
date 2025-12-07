import {resolver} from '@rocicorp/resolver';
import {createEffect} from 'solid-js';
import {createStore} from 'solid-js/store';
import {expect, test, vi} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {unreachable} from '../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import {stringCompare} from '../../shared/src/string-compare.ts';
import {
  createSchema,
  number,
  string,
  table,
  type Query,
} from '../../zero/src/zero.ts';
import type {Change} from '../../zql/src/ivm/change.ts';
import {Join} from '../../zql/src/ivm/join.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {MemoryStorage} from '../../zql/src/ivm/memory-storage.ts';
import type {Input} from '../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../zql/src/ivm/schema.ts';
import {Take} from '../../zql/src/ivm/take.ts';
import {createSource} from '../../zql/src/ivm/test/source-factory.ts';
import {idSymbol, refCountSymbol} from '../../zql/src/ivm/view-apply-change.ts';
import type {EntryList} from '../../zql/src/ivm/view.ts';
import {SolidView, createSolidViewFactory, type State} from './solid-view.ts';

const lc = createSilentLogContext();

test('basics', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  consume(ms.push({row: {a: 1, b: 'a'}, type: 'add'}));
  consume(ms.push({row: {a: 2, b: 'b'}, type: 'add'}));

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };
  const format = {singular: false, relationships: {}};
  const onDestroy = () => {};
  const queryComplete = true;
  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''];

  new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    onTransactionCommit,
    format,
    onDestroy,
    queryComplete,
    () => {},
    setState,
    () => {},
  );

  const data0 = [
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
    {a: 2, b: 'b', [refCountSymbol]: 1, [idSymbol]: '2'},
  ];
  expect(data()).toEqual(data0);

  expect(state[1]).toEqual({type: 'complete'});

  consume(ms.push({row: {a: 3, b: 'c'}, type: 'add'}));
  expect(data()).toEqual(data0);
  commit();

  const data1 = [
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
    {a: 2, b: 'b', [refCountSymbol]: 1, [idSymbol]: '2'},
    {a: 3, b: 'c', [refCountSymbol]: 1, [idSymbol]: '3'},
  ];
  expect(data()).toEqual(data1);

  consume(ms.push({row: {a: 2, b: 'b'}, type: 'remove'}));
  consume(ms.push({row: {a: 1, b: 'a'}, type: 'remove'}));

  expect(data()).toEqual(data1);
  commit();

  const data2 = [{a: 3, b: 'c', [refCountSymbol]: 1, [idSymbol]: '3'}];
  expect(data()).toEqual(data2);

  consume(ms.push({row: {a: 3, b: 'c'}, type: 'remove'}));

  expect(data()).toEqual(data2);
  commit();

  expect(data()).toEqual([]);
});

test('single-format', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  consume(ms.push({row: {a: 1, b: 'a'}, type: 'add'}));

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''];

  new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    onTransactionCommit,
    {singular: true, relationships: {}},
    () => {},
    true,
    () => {},
    setState,
    () => {},
  );

  const data0 = {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'};
  expect(data()).toEqual(data0);

  // trying to add another element should be an error
  // pipeline should have been configured with a limit of one
  expect(() => {
    consume(ms.push({row: {a: 2, b: 'b'}, type: 'add'}));
    commit();
  }).toThrow(
    "Singular relationship '' should not have multiple rows. You may need to declare this relationship with the `many` helper instead of the `one` helper in your schema.",
  );

  // Adding the same element is not an error in the ArrayView but it is an error
  // in the Source. This case is tested in view-apply-change.ts.

  consume(ms.push({row: {a: 1, b: 'a'}, type: 'remove'}));
  expect(data()).toEqual(data0);
  commit();

  expect(data()).toEqual(undefined);
});

test('hydrate-empty', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );

  const format = {singular: false, relationships: {}};
  const onDestroy = () => {};
  const queryComplete = true;

  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''];

  new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    () => {},
    format,
    onDestroy,
    queryComplete,
    () => {},
    setState,
    () => {},
  );

  expect(data()).toEqual([]);
});

test('tree', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {id: {type: 'number'}, name: {type: 'string'}, childID: {type: 'number'}},
    ['id'],
  );
  consume(
    ms.push({
      type: 'add',
      row: {id: 1, name: 'foo', childID: 2},
    }),
  );
  consume(
    ms.push({
      type: 'add',
      row: {id: 2, name: 'foobar', childID: null},
    }),
  );
  consume(
    ms.push({
      type: 'add',
      row: {id: 3, name: 'mon', childID: 4},
    }),
  );
  consume(
    ms.push({
      type: 'add',
      row: {id: 4, name: 'monkey', childID: null},
    }),
  );

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

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''];

  new SolidView(
    join,
    onTransactionCommit,
    {
      singular: false,
      relationships: {children: {singular: false, relationships: {}}},
    },
    () => {},
    true,
    () => {},
    setState,
    () => {},
  );

  const data0 = [
    {
      id: 1,
      name: 'foo',
      childID: 2,
      children: [
        {
          id: 2,
          name: 'foobar',
          childID: null,
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
    {
      id: 2,
      name: 'foobar',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '2',
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
          [refCountSymbol]: 1,
          [idSymbol]: '4',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '3',
    },
    {
      id: 4,
      name: 'monkey',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '4',
    },
  ];
  expect(data()).toEqual(data0);

  // add parent with child
  consume(ms.push({type: 'add', row: {id: 5, name: 'chocolate', childID: 2}}));
  expect(data()).toEqual(data0);
  commit();
  const data1 = [
    {
      id: 5,
      name: 'chocolate',
      childID: 2,
      children: [
        {
          id: 2,
          name: 'foobar',
          childID: null,
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '5',
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
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
    {
      id: 2,
      name: 'foobar',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '2',
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
          [refCountSymbol]: 1,
          [idSymbol]: '4',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '3',
    },
    {
      id: 4,
      name: 'monkey',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '4',
    },
  ];
  expect(data()).toEqual(data1);

  // remove parent with child
  consume(
    ms.push({type: 'remove', row: {id: 5, name: 'chocolate', childID: 2}}),
  );
  expect(data()).toEqual(data1);
  commit();
  const data2 = [
    {
      id: 1,
      name: 'foo',
      childID: 2,
      children: [
        {
          id: 2,
          name: 'foobar',
          childID: null,
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
    {
      id: 2,
      name: 'foobar',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '2',
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
          [refCountSymbol]: 1,
          [idSymbol]: '4',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '3',
    },
    {
      id: 4,
      name: 'monkey',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '4',
    },
  ];
  expect(data()).toEqual(data2);

  // remove just child
  consume(
    ms.push({
      type: 'remove',
      row: {
        id: 2,
        name: 'foobar',
        childID: null,
      },
    }),
  );
  expect(data()).toEqual(data2);
  commit();
  const data3 = [
    {
      id: 1,
      name: 'foo',
      childID: 2,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '1',
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
          [refCountSymbol]: 1,
          [idSymbol]: '4',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '3',
    },
    {
      id: 4,
      name: 'monkey',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '4',
    },
  ];
  expect(data()).toEqual(data3);

  // add child
  consume(
    ms.push({
      type: 'add',
      row: {
        id: 2,
        name: 'foobaz',
        childID: null,
      },
    }),
  );
  expect(data()).toEqual(data3);
  commit();
  expect(data()).toEqual([
    {
      id: 1,
      name: 'foo',
      childID: 2,
      children: [
        {
          id: 2,
          name: 'foobaz',
          childID: null,
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
    {
      id: 2,
      name: 'foobaz',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '2',
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
          [refCountSymbol]: 1,
          [idSymbol]: '4',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '3',
    },
    {
      id: 4,
      name: 'monkey',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '4',
    },
  ]);
});

test('tree-single', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {id: {type: 'number'}, name: {type: 'string'}, childID: {type: 'number'}},
    ['id'],
  );
  consume(
    ms.push({
      type: 'add',
      row: {id: 1, name: 'foo', childID: 2},
    }),
  );
  consume(
    ms.push({
      type: 'add',
      row: {id: 2, name: 'foobar', childID: null},
    }),
  );

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

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''];

  new SolidView(
    join,
    onTransactionCommit,
    {
      singular: true,
      relationships: {child: {singular: true, relationships: {}}},
    },
    () => {},
    true,
    () => {},
    setState,
    () => {},
  );

  const data0 = {
    id: 1,
    name: 'foo',
    childID: 2,
    child: {
      id: 2,
      name: 'foobar',
      childID: null,
      [refCountSymbol]: 1,
      [idSymbol]: '2',
    },
    [refCountSymbol]: 1,
    [idSymbol]: '1',
  };
  expect(data()).toEqual(data0);

  // remove the child
  consume(
    ms.push({
      type: 'remove',
      row: {id: 2, name: 'foobar', childID: null},
    }),
  );

  expect(data()).toEqual(data0);
  commit();

  const data1 = {
    id: 1,
    name: 'foo',
    childID: 2,
    child: undefined,
    [refCountSymbol]: 1,
    [idSymbol]: '1',
  };
  expect(data()).toEqual(data1);

  // remove the parent
  consume(
    ms.push({
      type: 'remove',
      row: {id: 1, name: 'foo', childID: 2},
    }),
  );

  expect(data()).toEqual(data1);
  commit();
  expect(data()).toEqual(undefined);
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

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''];

  const view = new SolidView(
    input,
    onTransactionCommit,
    {
      singular: false,
      relationships: {labels: {singular: false, relationships: {}}},
    },
    () => {},
    true,
    () => {},
    setState,
    () => {},
  );

  const data0: unknown[] = [];
  expect(data()).toEqual(data0);

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

  consume(
    view.push({
      type: 'add',
      ...changeSansType,
    }),
  );
  expect(data()).toEqual(data0);
  commit();

  const data1 = [
    {
      id: 1,
      labels: [
        {
          id: 1,
          name: 'label',
          [refCountSymbol]: 1,
          [idSymbol]: '1',
        },
      ],
      name: 'issue',
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
  ];
  expect(data()).toEqual(data1);

  consume(
    view.push({
      type: 'remove',
      ...changeSansType,
    }),
  );
  expect(data()).toEqual(data1);
  commit();

  const data2: unknown[] = [];
  expect(data()).toEqual(data2);

  consume(
    view.push({
      type: 'add',
      ...changeSansType,
    }),
  );
  // no commit

  expect(data()).toEqual(data2);

  consume(
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
    }),
  );

  expect(data()).toEqual(data2);
  commit();

  const data3 = [
    {
      id: 1,
      labels: [
        {
          id: 1,
          name: 'label',
          [refCountSymbol]: 1,
          [idSymbol]: '1',
        },
        {
          id: 2,
          name: 'label2',
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      name: 'issue',
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
  ];
  expect(data()).toEqual(data3);

  // edit the hidden row
  consume(
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
    }),
  );
  expect(data()).toEqual(data3);
  commit();

  const state4 = [
    {
      id: 1,
      labels: [
        {
          id: 1,
          name: 'label',
          [refCountSymbol]: 1,
          [idSymbol]: '1',
        },
        {
          id: 2,
          name: 'label2',
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      name: 'issue',
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
  ];
  expect(data()).toEqual(state4);

  // edit the leaf
  consume(
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
    }),
  );
  expect(data()).toEqual(state4);
  commit();

  expect(data()).toEqual([
    {
      id: 1,
      labels: [
        {
          id: 1,
          name: 'label',
          [refCountSymbol]: 1,
          [idSymbol]: '1',
        },
        {
          id: 2,
          name: 'label2x',
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      name: 'issue',
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
  ]);
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
      consume(view.push(change));
    },
  };

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''];

  const view = new SolidView(
    input,
    onTransactionCommit,
    {
      singular: false,
      relationships: {labels: {singular: true, relationships: {}}},
    },
    () => {},
    true,
    () => {},
    setState,
    () => {},
  );

  const data0: unknown[] = [];
  expect(data()).toEqual(data0);

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
  consume(
    view.push({
      type: 'add',
      ...changeSansType,
    }),
  );

  expect(data()).toEqual(data0);
  commit();

  expect(data()).toEqual([
    {
      id: 1,
      labels: {
        id: 1,
        name: 'label',
        [refCountSymbol]: 1,
        [idSymbol]: '1',
      },
      name: 'issue',
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
  ]);
});

test('basic with edit pushes', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {id: {type: 'number'}, b: {type: 'string'}},
    ['id'],
  );
  consume(ms.push({row: {id: 1, b: 'a'}, type: 'add'}));
  consume(ms.push({row: {id: 2, b: 'b'}, type: 'add'}));

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''];

  new SolidView(
    ms.connect([['id', 'asc']]),
    onTransactionCommit,
    {singular: false, relationships: {}},
    () => {},
    true,
    () => {},
    setState,
    () => {},
  );

  const data0 = [
    {id: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
    {id: 2, b: 'b', [refCountSymbol]: 1, [idSymbol]: '2'},
  ];
  expect(data()).toEqual(data0);

  consume(
    ms.push({type: 'edit', row: {id: 2, b: 'b2'}, oldRow: {id: 2, b: 'b'}}),
  );

  expect(data()).toEqual(data0);
  commit();

  const data1 = [
    {id: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
    {id: 2, b: 'b2', [refCountSymbol]: 1, [idSymbol]: '2'},
  ];
  expect(data()).toEqual(data1);

  consume(
    ms.push({type: 'edit', row: {id: 3, b: 'b3'}, oldRow: {id: 2, b: 'b2'}}),
  );

  expect(data()).toEqual(data1);
  commit();
  expect(data()).toEqual([
    {id: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
    {id: 3, b: 'b3', [refCountSymbol]: 1, [idSymbol]: '3'},
  ]);
});

test('edit trigger reactivity at the column level', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  consume(ms.push({row: {a: 1, b: 'a'}, type: 'add'}));
  consume(ms.push({row: {a: 2, b: 'b'}, type: 'add'}));

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''] as EntryList;

  const row0Log: unknown[] = [];
  const row1Log: unknown[] = [];
  const row0ALog: unknown[] = [];
  const row0BLog: unknown[] = [];
  const row1ALog: unknown[] = [];
  const row1BLog: unknown[] = [];

  function clearLog() {
    row0Log.length = 0;
    row1Log.length = 0;
    row0ALog.length = 0;
    row0BLog.length = 0;
    row1ALog.length = 0;
    row1BLog.length = 0;
  }

  new SolidView(
    ms.connect([['a', 'asc']]),
    onTransactionCommit,
    {singular: false, relationships: {}},
    () => {},
    true,
    () => {},
    setState,
    () => {},
  );

  createEffect(() => {
    row0Log.push(data()[0]);
  });
  createEffect(() => {
    row1Log.push(data()[1]);
  });
  createEffect(() => {
    row0ALog.push(data()[0]?.a);
  });
  createEffect(() => {
    row0BLog.push(data()[0]?.b);
  });
  createEffect(() => {
    row1ALog.push(data()[1]?.a);
  });
  createEffect(() => {
    row1BLog.push(data()[1]?.b);
  });

  const data0 = [
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
    {a: 2, b: 'b', [refCountSymbol]: 1, [idSymbol]: '2'},
  ];
  expect(data()).toEqual(data0);
  expect(row0Log).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
  ]);
  expect(row1Log).toEqual([
    {a: 2, b: 'b', [refCountSymbol]: 1, [idSymbol]: '2'},
  ]);
  expect(row0ALog).toEqual([1]);
  expect(row0BLog).toEqual(['a']);
  expect(row1ALog).toEqual([2]);
  expect(row1BLog).toEqual(['b']);

  clearLog();

  consume(
    ms.push({type: 'edit', row: {a: 2, b: 'b2'}, oldRow: {a: 2, b: 'b'}}),
  );

  expect(data()).toEqual(data0);
  commit();

  const data1 = [
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
    {a: 2, b: 'b2', [refCountSymbol]: 1, [idSymbol]: '2'},
  ];
  expect(data()).toEqual(data1);
  expect(row0Log).toEqual([]);
  expect(row1Log).toEqual([]);
  expect(row0ALog).toEqual([]);
  expect(row0BLog).toEqual([]);
  expect(row1ALog).toEqual([]);
  expect(row1BLog).toEqual(['b2']);

  clearLog();

  consume(
    ms.push({type: 'edit', row: {a: 3, b: 'b3'}, oldRow: {a: 2, b: 'b2'}}),
  );

  expect(data()).toEqual(data1);
  commit();
  const data2 = [
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
    {a: 3, b: 'b3', [refCountSymbol]: 1, [idSymbol]: '3'},
  ];
  expect(data()).toEqual(data2);
  expect(row0Log).toEqual([]);
  expect(row1Log).toEqual([]);
  expect(row0ALog).toEqual([]);
  expect(row0BLog).toEqual([]);
  expect(row1ALog).toEqual([3]);
  expect(row1BLog).toEqual(['b3']);

  clearLog();

  consume(
    ms.push({type: 'edit', row: {a: 0, b: 'b3'}, oldRow: {a: 3, b: 'b3'}}),
  );

  expect(data()).toEqual(data2);
  commit();
  const data3 = [
    {a: 0, b: 'b3', [refCountSymbol]: 1, [idSymbol]: '0'},
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
  ];
  expect(data()).toEqual(data3);
  expect(row0Log).toEqual([
    {a: 0, b: 'b3', [refCountSymbol]: 1, [idSymbol]: '0'},
  ]);
  expect(row1Log).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
  ]);
  expect(row0ALog).toEqual([0]);
  expect(row0BLog).toEqual(['b3']);
  expect(row1ALog).toEqual([1]);
  expect(row1BLog).toEqual(['a']);
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
    consume(ms.push({type: 'add', row}));
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

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''];

  new SolidView(
    join,
    onTransactionCommit,
    {
      singular: false,
      relationships: {children: {singular: false, relationships: {}}},
    },
    () => {},
    true,
    () => {},
    setState,
    () => {},
  );

  const data0 = [
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
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
    {
      id: 2,
      name: 'foobar',
      data: 'b',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '2',
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
          [refCountSymbol]: 1,
          [idSymbol]: '4',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '3',
    },
    {
      id: 4,
      name: 'monkey',
      data: 'd',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '4',
    },
  ];
  expect(data()).toEqual(data0);

  // Edit root
  consume(
    ms.push({
      type: 'edit',
      oldRow: {id: 1, name: 'foo', data: 'a', childID: 2},
      row: {id: 1, name: 'foo', data: 'a2', childID: 2},
    }),
  );

  expect(data()).toEqual(data0);
  commit();

  const data1 = [
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
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
    {
      id: 2,
      name: 'foobar',
      data: 'b',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '2',
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
          [refCountSymbol]: 1,
          [idSymbol]: '4',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '3',
    },
    {
      id: 4,
      name: 'monkey',
      data: 'd',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '4',
    },
  ];
  expect(data()).toEqual(data1);

  // Edit leaf
  consume(
    ms.push({
      type: 'edit',
      oldRow: {
        id: 4,
        name: 'monkey',
        data: 'd',
        childID: null,
      },
      row: {
        id: 4,
        name: 'monkey',
        data: 'd2',
        childID: null,
      },
    }),
  );

  expect(data()).toEqual(data1);

  commit();

  const data2 = [
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
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
    {
      id: 2,
      name: 'foobar',
      data: 'b',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '2',
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
          data: 'd2',
          childID: null,
          [refCountSymbol]: 1,
          [idSymbol]: '4',
        },
      ],
      [refCountSymbol]: 1,
      [idSymbol]: '3',
    },
    {
      id: 4,
      name: 'monkey',
      data: 'd2',
      childID: null,
      children: [],
      [refCountSymbol]: 1,
      [idSymbol]: '4',
    },
  ];

  expect(data()).toEqual(data2);
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
    consume(ms.push({row, type: 'add'}));
  }

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''];

  new SolidView(
    ms.connect([['a', 'asc']]),
    onTransactionCommit,
    {singular: false, relationships: {}},
    () => {},
    true,
    () => {},
    setState,
    () => {},
  );

  const data0 = [
    {a: 10, b: 'a', [refCountSymbol]: 1, [idSymbol]: '10'},
    {a: 20, b: 'b', [refCountSymbol]: 1, [idSymbol]: '20'},
    {a: 30, b: 'c', [refCountSymbol]: 1, [idSymbol]: '30'},
  ];
  expect(data()).toEqual(data0);

  consume(
    ms.push({
      type: 'edit',
      oldRow: {a: 20, b: 'b'},
      row: {a: 5, b: 'b2'},
    }),
  );

  expect(data()).toEqual(data0);
  commit();

  const data1 = [
    {a: 5, b: 'b2', [refCountSymbol]: 1, [idSymbol]: '5'},
    {a: 10, b: 'a', [refCountSymbol]: 1, [idSymbol]: '10'},
    {a: 30, b: 'c', [refCountSymbol]: 1, [idSymbol]: '30'},
  ];
  expect(data()).toEqual(data1);

  consume(
    ms.push({
      type: 'edit',
      oldRow: {a: 5, b: 'b2'},
      row: {a: 4, b: 'b3'},
    }),
  );

  expect(data()).toEqual(data1);
  commit();

  const data2 = [
    {a: 4, b: 'b3', [refCountSymbol]: 1, [idSymbol]: '4'},
    {a: 10, b: 'a', [refCountSymbol]: 1, [idSymbol]: '10'},
    {a: 30, b: 'c', [refCountSymbol]: 1, [idSymbol]: '30'},
  ];
  expect(data()).toEqual(data2);

  consume(
    ms.push({
      type: 'edit',
      oldRow: {a: 4, b: 'b3'},
      row: {a: 20, b: 'b4'},
    }),
  );

  expect(data()).toEqual(data2);
  commit();

  expect(data()).toEqual([
    {a: 10, b: 'a', [refCountSymbol]: 1, [idSymbol]: '10'},
    {a: 20, b: 'b4', [refCountSymbol]: 1, [idSymbol]: '20'},
    {a: 30, b: 'c', [refCountSymbol]: 1, [idSymbol]: '30'},
  ]);
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

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''];

  const view = new SolidView(
    input,
    onTransactionCommit,
    {
      singular: false,
      relationships: {labels: {singular: false, relationships: {}}},
    },
    () => {},
    true,
    () => {},
    setState,
    () => {},
  );

  const data0: unknown[] = [];
  expect(data()).toEqual(data0);

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

  expect(data()).toEqual(data0);

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

  expect(data()).toEqual(data0);
  commit();
  const data1 = [
    {
      id: 1,
      labels: [
        {
          id: 1,
          name: 'label1',
          [refCountSymbol]: 1,
          [idSymbol]: '1',
        },
      ],
      title: 'issue1',
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
    {
      id: 2,
      labels: [
        {
          id: 2,
          name: 'label2',
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      title: 'issue2',
      [refCountSymbol]: 1,
      [idSymbol]: '2',
    },
  ];
  expect(data()).toEqual(data1);

  view.push({
    type: 'edit',
    oldNode: {
      row: {id: 1, title: 'issue1'},
      relationships: {},
    },
    node: {row: {id: 1, title: 'issue1 changed'}, relationships: {}},
  });

  expect(data()).toEqual(data1);
  commit();

  const data2 = [
    {
      id: 1,
      labels: [
        {
          id: 1,
          name: 'label1',
          [refCountSymbol]: 1,
          [idSymbol]: '1',
        },
      ],
      title: 'issue1 changed',
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
    {
      id: 2,
      labels: [
        {
          id: 2,
          name: 'label2',
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      title: 'issue2',
      [refCountSymbol]: 1,
      [idSymbol]: '2',
    },
  ];
  expect(data()).toEqual(data2);

  // And now edit to change order
  view.push({
    type: 'edit',
    oldNode: {row: {id: 1, title: 'issue1 changed'}, relationships: {}},
    node: {row: {id: 3, title: 'issue1 is now issue3'}, relationships: {}},
  });

  expect(data()).toEqual(data2);
  commit();

  expect(data()).toEqual([
    {
      id: 2,
      labels: [
        {
          id: 2,
          name: 'label2',
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      title: 'issue2',
      [refCountSymbol]: 1,
      [idSymbol]: '2',
    },
    {
      id: 3,
      labels: [
        {
          id: 1,
          name: 'label1',
          [refCountSymbol]: 1,
          [idSymbol]: '1',
        },
      ],
      title: 'issue1 is now issue3',
      [refCountSymbol]: 1,
      [idSymbol]: '3',
    },
  ]);
});

test('edit leaf', () => {
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
        isHidden: false,
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

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''];

  const view = new SolidView(
    input,
    onTransactionCommit,
    {
      singular: false,
      relationships: {labels: {singular: false, relationships: {}}},
    },
    () => {},
    true,
    () => {},
    setState,
    () => {},
  );

  const data0: unknown[] = [];
  expect(data()).toEqual(data0);

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
            relationships: {},
          },
        ],
      },
    },
  } as const;

  view.push({
    type: 'add',
    ...changeSansType,
  });
  expect(data()).toEqual(data0);
  commit();

  const data1 = [
    {
      id: 1,
      labels: [
        {
          id: 1,
          issueId: 1,
          labelId: 1,
          extra: 'a',
          [refCountSymbol]: 1,
          [idSymbol]: '1',
        },
      ],
      name: 'issue',
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
  ];
  expect(data()).toEqual(data1);

  view.push({
    type: 'remove',
    ...changeSansType,
  });
  expect(data()).toEqual(data1);
  commit();

  const data2: unknown[] = [];
  expect(data()).toEqual(data2);

  view.push({
    type: 'add',
    ...changeSansType,
  });
  // no commit

  expect(data()).toEqual(data2);

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
            relationships: {},
          },
          {
            row: {
              id: 2,
              issueId: 1,
              labelId: 2,
              extra: 'b',
            },
            relationships: {},
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
          relationships: {},
        },
      },
    },
  });

  expect(data()).toEqual(data2);
  commit();

  const data3 = [
    {
      id: 1,
      labels: [
        {
          id: 1,
          issueId: 1,
          labelId: 1,
          extra: 'a',
          [refCountSymbol]: 1,
          [idSymbol]: '1',
        },
        {
          id: 2,
          issueId: 1,
          labelId: 2,
          extra: 'b',
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      name: 'issue',
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
  ];
  expect(data()).toEqual(data3);

  // edit leaf
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
            relationships: {},
          },
          {
            row: {
              id: 2,
              issueId: 1,
              labelId: 2,
              extra: 'b2',
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
            issueId: 1,
            labelId: 2,
            extra: 'b',
          },
          relationships: {},
        },
        node: {
          row: {
            id: 2,
            issueId: 1,
            labelId: 2,
            extra: 'b2',
          },
          relationships: {},
        },
      },
    },
  });
  expect(data()).toEqual(data3);
  commit();

  expect(data()).toEqual([
    {
      id: 1,
      labels: [
        {
          id: 1,
          issueId: 1,
          labelId: 1,
          extra: 'a',
          [refCountSymbol]: 1,
          [idSymbol]: '1',
        },
        {
          id: 2,
          issueId: 1,
          labelId: 2,
          extra: 'b2',
          [refCountSymbol]: 1,
          [idSymbol]: '2',
        },
      ],
      name: 'issue',
      [refCountSymbol]: 1,
      [idSymbol]: '1',
    },
  ]);
});

test('queryComplete promise', async () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  consume(ms.push({row: {a: 1, b: 'a'}, type: 'add'}));
  consume(ms.push({row: {a: 2, b: 'b'}, type: 'add'}));

  const queryCompleteResolver = resolver<true>();

  const onTransactionCommit = () => {};

  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);
  const data = () => state[0][''];
  const resultDetails = () => state[1];

  new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    onTransactionCommit,
    {singular: false, relationships: {}},
    () => {},
    queryCompleteResolver.promise,
    () => {},
    setState,
    () => {},
  );

  expect(data()).toMatchInlineSnapshot(`
    [
      {
        "a": 1,
        "b": "a",
        Symbol(rc): 1,
        Symbol(id): "1",
      },
      {
        "a": 2,
        "b": "b",
        Symbol(rc): 1,
        Symbol(id): "2",
      },
    ]
  `);

  expect(resultDetails()).toEqual({type: 'unknown'});

  queryCompleteResolver.resolve(true);
  await Promise.resolve();
  expect(resultDetails()).toEqual({type: 'complete'});
});

const schema = createSchema({
  tables: [
    table('test')
      .columns({
        a: number(),
        b: string(),
      })
      .primaryKey('a'),
  ],
});

type TestReturn = {
  a: number;
  b: string;
};

test('factory', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  const onDestroy = vi.fn();
  const onTransactionCommit = vi.fn();

  const [_, setState] = createStore<State>([
    {
      '': undefined,
    },
    {type: 'unknown'},
  ]);

  const view: SolidView = createSolidViewFactory(setState)(
    undefined as unknown as Query<'test', typeof schema, TestReturn>,
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    {singular: false, relationships: {}},
    onDestroy,
    onTransactionCommit,
    true,
    () => {},
  );

  expect(onTransactionCommit).toHaveBeenCalledTimes(1);
  expect(view).toBeDefined();
  expect(onDestroy).not.toHaveBeenCalled();
  view.destroy();
  expect(onDestroy).toHaveBeenCalledTimes(1);
});
