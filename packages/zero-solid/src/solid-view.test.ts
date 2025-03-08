import {resolver} from '@rocicorp/resolver';
import {expect, test, vi} from 'vitest';
import type {LogConfig} from '../../otel/src/log-options.ts';
import {unreachable} from '../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {stringCompare} from '../../shared/src/string-compare.ts';
import {createSchema, number, string, table} from '../../zero/src/zero.ts';
import type {Change} from '../../zql/src/ivm/change.ts';
import {Join} from '../../zql/src/ivm/join.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {MemoryStorage} from '../../zql/src/ivm/memory-storage.ts';
import type {Input} from '../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../zql/src/ivm/schema.ts';
import {Take} from '../../zql/src/ivm/take.ts';
import {createSource} from '../../zql/src/ivm/test/source-factory.ts';
import type {HumanReadable, Query} from '../../zql/src/query/query.ts';
import {SolidView, solidViewFactory} from './solid-view.ts';

const lc = createSilentLogContext();
const logConfig: LogConfig = {
  format: 'text',
  level: 'debug',
  ivmSampling: 0,
  slowRowThreshold: 0,
};

test('basics', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };
  const format = {singular: false, relationships: {}};
  const onDestroy = () => {};
  const queryComplete = true;

  const view = new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    onTransactionCommit,
    format,
    onDestroy,
    queryComplete,
  );

  const state0 = [
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
  ];
  expect(view.data).toEqual(state0);

  expect(view.resultDetails).toEqual({type: 'complete'});

  ms.push({row: {a: 3, b: 'c'}, type: 'add'});
  expect(view.data).toEqual(state0);
  commit();

  const state1 = [
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
    {a: 3, b: 'c'},
  ];
  expect(view.data).toEqual(state1);

  ms.push({row: {a: 2, b: 'b'}, type: 'remove'});
  ms.push({row: {a: 1, b: 'a'}, type: 'remove'});

  expect(view.data).toEqual(state1);
  commit();

  const state2 = [{a: 3, b: 'c'}];
  expect(view.data).toEqual(state2);

  ms.push({row: {a: 3, b: 'c'}, type: 'remove'});

  expect(view.data).toEqual(state2);
  commit();

  expect(view.data).toEqual([]);
});

test('single-format', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const view = new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    onTransactionCommit,
    {singular: true, relationships: {}},
    () => {},
    true,
  );

  const state0 = {a: 1, b: 'a'};
  expect(view.data).toEqual(state0);

  // trying to add another element should be an error
  // pipeline should have been configured with a limit of one
  expect(() => {
    ms.push({row: {a: 2, b: 'b'}, type: 'add'});
    commit();
  }).toThrow('single output already exists');

  // Adding the same element is not an error in the ArrayView but it is an error
  // in the Source. This case is tested in view-apply-change.ts.

  ms.push({row: {a: 1, b: 'a'}, type: 'remove'});
  expect(view.data).toEqual(state0);
  commit();

  expect(view.data).toEqual(undefined);
});

test('hydrate-empty', () => {
  const ms = createSource(
    lc,
    logConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );

  const format = {singular: false, relationships: {}};
  const onDestroy = () => {};
  const queryComplete = true;

  const view = new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    () => {},
    format,
    onDestroy,
    queryComplete,
  );

  expect(view.data).toEqual([]);
});

test('tree', () => {
  const ms = createSource(
    lc,
    logConfig,
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

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const view = new SolidView(
    join,
    onTransactionCommit,
    {
      singular: false,
      relationships: {children: {singular: false, relationships: {}}},
    },
    () => {},
    true,
  );

  const state0 = [
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
  ];
  expect(view.data).toEqual(state0);

  // add parent with child
  ms.push({type: 'add', row: {id: 5, name: 'chocolate', childID: 2}});
  expect(view.data).toEqual(state0);
  commit();
  const state1 = [
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
  ];
  expect(view.data).toEqual(state1);

  // remove parent with child
  ms.push({type: 'remove', row: {id: 5, name: 'chocolate', childID: 2}});
  expect(view.data).toEqual(state1);
  commit();
  const state2 = [
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
  ];
  expect(view.data).toEqual(state2);

  // remove just child
  ms.push({
    type: 'remove',
    row: {
      id: 2,
      name: 'foobar',
      childID: null,
    },
  });
  expect(view.data).toEqual(state2);
  commit();
  const state3 = [
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
  ];
  expect(view.data).toEqual(state3);

  // add child
  ms.push({
    type: 'add',
    row: {
      id: 2,
      name: 'foobaz',
      childID: null,
    },
  });
  expect(view.data).toEqual(state3);
  commit();
  expect(view.data).toEqual([
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

test('tree-single', () => {
  const ms = createSource(
    lc,
    logConfig,
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

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const view = new SolidView(
    join,
    onTransactionCommit,
    {
      singular: true,
      relationships: {child: {singular: true, relationships: {}}},
    },
    () => {},
    true,
  );

  const state0 = {
    id: 1,
    name: 'foo',
    childID: 2,
    child: {
      id: 2,
      name: 'foobar',
      childID: null,
    },
  };
  expect(view.data).toEqual(state0);

  // remove the child
  ms.push({
    type: 'remove',
    row: {id: 2, name: 'foobar', childID: null},
  });

  expect(view.data).toEqual(state0);
  commit();

  const state1 = {
    id: 1,
    name: 'foo',
    childID: 2,
    child: undefined,
  };
  expect(view.data).toEqual(state1);

  // remove the parent
  ms.push({
    type: 'remove',
    row: {id: 1, name: 'foo', childID: 2},
  });

  expect(view.data).toEqual(state1);
  commit();
  expect(view.data).toEqual(undefined);
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

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const view = new SolidView(
    input,
    onTransactionCommit,
    {
      singular: false,
      relationships: {labels: {singular: false, relationships: {}}},
    },
    () => {},
    true,
  );

  const state0: unknown[] = [];
  expect(view.data).toEqual(state0);

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
  expect(view.data).toEqual(state0);
  commit();

  const state1 = [
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
  ];
  expect(view.data).toEqual(state1);

  view.push({
    type: 'remove',
    ...changeSansType,
  });
  expect(view.data).toEqual(state1);
  commit();

  const state2: unknown[] = [];
  expect(view.data).toEqual(state2);

  view.push({
    type: 'add',
    ...changeSansType,
  });
  // no commit

  expect(view.data).toEqual(state2);

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

  expect(view.data).toEqual(state2);
  commit();

  const state3 = [
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
  ];
  expect(view.data).toEqual(state3);

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
  expect(view.data).toEqual(state3);
  commit();

  const state4 = [
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
  ];
  expect(view.data).toEqual(state4);

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
  expect(view.data).toEqual(state4);
  commit();

  expect(view.data).toEqual([
    {
      id: 1,
      labels: [
        {
          id: 1,
          name: 'label',
        },
        {
          id: 2,
          name: 'label2x',
        },
      ],
      name: 'issue',
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
      view.push(change);
    },
  };

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const view = new SolidView(
    input,
    onTransactionCommit,
    {
      singular: false,
      relationships: {labels: {singular: true, relationships: {}}},
    },
    () => {},
    true,
  );

  const state0: unknown[] = [];
  expect(view.data).toEqual(state0);

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

  expect(view.data).toEqual(state0);
  commit();

  expect(view.data).toEqual([
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
    logConfig,
    'table',
    {id: {type: 'number'}, b: {type: 'string'}},
    ['id'],
  );
  ms.push({row: {id: 1, b: 'a'}, type: 'add'});
  ms.push({row: {id: 2, b: 'b'}, type: 'add'});

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const view = new SolidView(
    ms.connect([['id', 'asc']]),
    onTransactionCommit,
    {singular: false, relationships: {}},
    () => {},
    true,
  );

  const state0 = [
    {id: 1, b: 'a'},
    {id: 2, b: 'b'},
  ];
  expect(view.data).toEqual(state0);

  ms.push({type: 'edit', row: {id: 2, b: 'b2'}, oldRow: {id: 2, b: 'b'}});

  expect(view.data).toEqual(state0);
  commit();

  const state1 = [
    {id: 1, b: 'a'},
    {id: 2, b: 'b2'},
  ];
  expect(view.data).toEqual(state1);

  ms.push({type: 'edit', row: {id: 3, b: 'b3'}, oldRow: {id: 2, b: 'b2'}});

  expect(view.data).toEqual(state1);
  commit();
  expect(view.data).toEqual([
    {id: 1, b: 'a'},
    {id: 3, b: 'b3'},
  ]);
});

test('tree edit', () => {
  const ms = createSource(
    lc,
    logConfig,
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

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const view = new SolidView(
    join,
    onTransactionCommit,
    {
      singular: false,
      relationships: {children: {singular: false, relationships: {}}},
    },
    () => {},
    true,
  );

  const state0 = [
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
  ];
  expect(view.data).toEqual(state0);

  // Edit root
  ms.push({
    type: 'edit',
    oldRow: {id: 1, name: 'foo', data: 'a', childID: 2},
    row: {id: 1, name: 'foo', data: 'a2', childID: 2},
  });

  expect(view.data).toEqual(state0);
  commit();

  const state1 = [
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
  ];
  expect(view.data).toEqual(state1);

  // Edit leaf
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
  });

  expect(view.data).toEqual(state1);

  commit();

  const state2 = [
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
          data: 'd2',
          childID: null,
        },
      ],
    },
    {
      id: 4,
      name: 'monkey',
      data: 'd2',
      childID: null,
      children: [],
    },
  ];

  expect(view.data).toEqual(state2);
});

test('edit to change the order', () => {
  const ms = createSource(
    lc,
    logConfig,
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

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const view = new SolidView(
    ms.connect([['a', 'asc']]),
    onTransactionCommit,
    {singular: false, relationships: {}},
    () => {},
    true,
  );

  const state0 = [
    {a: 10, b: 'a'},
    {a: 20, b: 'b'},
    {a: 30, b: 'c'},
  ];
  expect(view.data).toEqual(state0);

  ms.push({
    type: 'edit',
    oldRow: {a: 20, b: 'b'},
    row: {a: 5, b: 'b2'},
  });

  expect(view.data).toEqual(state0);
  commit();

  const state1 = [
    {a: 5, b: 'b2'},
    {a: 10, b: 'a'},
    {a: 30, b: 'c'},
  ];
  expect(view.data).toEqual(state1);

  ms.push({
    type: 'edit',
    oldRow: {a: 5, b: 'b2'},
    row: {a: 4, b: 'b3'},
  });

  expect(view.data).toEqual(state1);
  commit();

  const state2 = [
    {a: 4, b: 'b3'},
    {a: 10, b: 'a'},
    {a: 30, b: 'c'},
  ];
  expect(view.data).toEqual(state2);

  ms.push({
    type: 'edit',
    oldRow: {a: 4, b: 'b3'},
    row: {a: 20, b: 'b4'},
  });

  expect(view.data).toEqual(state2);
  commit();

  expect(view.data).toEqual([
    {a: 10, b: 'a'},
    {a: 20, b: 'b4'},
    {a: 30, b: 'c'},
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
    cleanup() {
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

  const view = new SolidView(
    input,
    onTransactionCommit,
    {
      singular: false,
      relationships: {labels: {singular: false, relationships: {}}},
    },
    () => {},
    true,
  );

  const state0: unknown[] = [];
  expect(view.data).toEqual(state0);

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

  expect(view.data).toEqual(state0);

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

  expect(view.data).toEqual(state0);
  commit();
  const state1 = [
    {
      id: 1,
      labels: [
        {
          id: 1,
          name: 'label1',
        },
      ],
      title: 'issue1',
    },
    {
      id: 2,
      labels: [
        {
          id: 2,
          name: 'label2',
        },
      ],
      title: 'issue2',
    },
  ];
  expect(view.data).toEqual(state1);

  view.push({
    type: 'edit',
    oldNode: {
      row: {id: 1, title: 'issue1'},
      relationships: {},
    },
    node: {row: {id: 1, title: 'issue1 changed'}, relationships: {}},
  });

  expect(view.data).toEqual(state1);
  commit();

  const state2 = [
    {
      id: 1,
      labels: [
        {
          id: 1,
          name: 'label1',
        },
      ],
      title: 'issue1 changed',
    },
    {
      id: 2,
      labels: [
        {
          id: 2,
          name: 'label2',
        },
      ],
      title: 'issue2',
    },
  ];
  expect(view.data).toEqual(state2);

  // And now edit to change order
  view.push({
    type: 'edit',
    oldNode: {row: {id: 1, title: 'issue1 changed'}, relationships: {}},
    node: {row: {id: 3, title: 'issue1 is now issue3'}, relationships: {}},
  });

  expect(view.data).toEqual(state2);
  commit();

  expect(view.data).toEqual([
    {
      id: 2,
      labels: [
        {
          id: 2,
          name: 'label2',
        },
      ],
      title: 'issue2',
    },
    {
      id: 3,
      labels: [
        {
          id: 1,
          name: 'label1',
        },
      ],
      title: 'issue1 is now issue3',
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

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const view = new SolidView(
    input,
    onTransactionCommit,
    {
      singular: false,
      relationships: {labels: {singular: false, relationships: {}}},
    },
    () => {},
    true,
  );

  const state0: unknown[] = [];
  expect(view.data).toEqual(state0);

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
  expect(view.data).toEqual(state0);
  commit();

  const state1 = [
    {
      id: 1,
      labels: [
        {
          id: 1,
          issueId: 1,
          labelId: 1,
          extra: 'a',
        },
      ],
      name: 'issue',
    },
  ];
  expect(view.data).toEqual(state1);

  view.push({
    type: 'remove',
    ...changeSansType,
  });
  expect(view.data).toEqual(state1);
  commit();

  const state2: unknown[] = [];
  expect(view.data).toEqual(state2);

  view.push({
    type: 'add',
    ...changeSansType,
  });
  // no commit

  expect(view.data).toEqual(state2);

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

  expect(view.data).toEqual(state2);
  commit();

  const state3 = [
    {
      id: 1,
      labels: [
        {
          id: 1,
          issueId: 1,
          labelId: 1,
          extra: 'a',
        },
        {
          id: 2,
          issueId: 1,
          labelId: 2,
          extra: 'b',
        },
      ],
      name: 'issue',
    },
  ];
  expect(view.data).toEqual(state3);

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
  expect(view.data).toEqual(state3);
  commit();

  const state4 = [
    {
      id: 1,
      labels: [
        {
          id: 1,
          issueId: 1,
          labelId: 1,
          extra: 'a',
        },
        {
          id: 2,
          issueId: 1,
          labelId: 2,
          extra: 'b2',
        },
      ],
      name: 'issue',
    },
  ];
  expect(view.data).toEqual(state4);
});

test('queryComplete promise', async () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  const queryCompleteResolver = resolver<true>();

  const onTransactionCommit = () => {};

  const view = new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    onTransactionCommit,
    {singular: false, relationships: {}},
    () => {},
    queryCompleteResolver.promise,
  );

  expect(view.data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
  ]);

  expect(view.resultDetails).toEqual({type: 'unknown'});

  queryCompleteResolver.resolve(true);
  await 1;
  expect(view.resultDetails).toEqual({type: 'complete'});
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

  const view: SolidView<HumanReadable<TestReturn>> = solidViewFactory(
    undefined as unknown as Query<typeof schema, 'test', TestReturn>,
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    {singular: false, relationships: {}},
    onDestroy,
    onTransactionCommit,
    true,
  );

  expect(onTransactionCommit).toHaveBeenCalledTimes(1);
  expect(view).toBeDefined();
  expect(onDestroy).not.toHaveBeenCalled();
  view.destroy();
  expect(onDestroy).toHaveBeenCalledTimes(1);
});
