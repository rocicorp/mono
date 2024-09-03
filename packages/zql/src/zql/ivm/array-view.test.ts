import {deepClone} from 'shared/src/deep-clone.js';
import {Immutable} from 'shared/src/immutable.js';
import {expect, test} from 'vitest';
import {ArrayView, EntryList} from './array-view.js';
import {Change, ChangeType} from './change.js';
import {Join} from './join.js';
import {MemorySource} from './memory-source.js';
import {MemoryStorage} from './memory-storage.js';
import {Schema} from './schema.js';

test('basics', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: ChangeType.Add});
  ms.push({row: {a: 2, b: 'b'}, type: ChangeType.Add});

  const view = new ArrayView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
  );

  let callCount = 0;
  let data: unknown[] = [];
  const listener = (d: Immutable<EntryList>) => {
    ++callCount;
    data = deepClone(d) as unknown[];
  };
  const unlisten = view.addListener(listener);

  view.hydrate();
  expect(data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
  ]);

  expect(callCount).toBe(1);
  expect(() => view.hydrate()).toThrow("Can't hydrate twice");

  ms.push({row: {a: 3, b: 'c'}, type: ChangeType.Add});

  // We don't get called until flush.
  expect(callCount).toBe(1);

  view.flush();
  expect(callCount).toBe(2);
  expect(data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
    {a: 3, b: 'c'},
  ]);

  ms.push({row: {a: 2, b: 'b'}, type: ChangeType.Remove});
  expect(callCount).toBe(2);
  ms.push({row: {a: 1, b: 'a'}, type: ChangeType.Remove});
  expect(callCount).toBe(2);

  view.flush();
  expect(callCount).toBe(3);
  expect(data).toEqual([{a: 3, b: 'c'}]);

  unlisten();
  ms.push({row: {a: 3, b: 'c'}, type: ChangeType.Remove});
  expect(callCount).toBe(3);

  view.flush();
  expect(callCount).toBe(3);
  expect(data).toEqual([{a: 3, b: 'c'}]);
});

test('tree', () => {
  const ms = new MemorySource(
    'table',
    {id: {type: 'number'}, name: {type: 'string'}},
    ['id'],
  );
  ms.push({
    type: ChangeType.Add,
    row: {id: 1, name: 'foo', childID: 2},
  });
  ms.push({
    type: ChangeType.Add,
    row: {id: 2, name: 'foobar', childID: null},
  });
  ms.push({
    type: ChangeType.Add,
    row: {id: 3, name: 'mon', childID: 4},
  });
  ms.push({
    type: ChangeType.Add,
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
  const listener = (d: Immutable<EntryList>) => {
    data = deepClone(d) as unknown[];
  };
  view.addListener(listener);

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
  ms.push({type: ChangeType.Add, row: {id: 5, name: 'chocolate', childID: 2}});
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
  ms.push({
    type: ChangeType.Remove,
    row: {id: 5, name: 'chocolate', childID: 2},
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
    type: ChangeType.Remove,
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
    type: ChangeType.Add,
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
  view.addListener(d => {
    data = deepClone(d) as unknown[];
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
    type: ChangeType.Add,
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
    type: ChangeType.Remove,
    ...changeSansType,
  });
  view.flush();

  expect(data).toEqual([]);

  view.push({
    type: ChangeType.Add,
    ...changeSansType,
  });
  // no commit
  expect(data).toEqual([]);

  view.push({
    type: ChangeType.Child,
    row: {
      id: 1,
      name: 'issue',
    },
    child: {
      relationshipName: 'labels',
      change: {
        type: ChangeType.Add,
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
