import {MemorySource} from './memory-source.js';
import {Entry, View} from './view.js';
import {expect, test} from 'vitest';
import {Join} from './join.js';
import {MemoryStorage} from './memory-storage.js';

test('basics', () => {
  const ms = new MemorySource({a: 'number', b: 'string'}, ['a']);
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  const view = new View(ms.connect([['b', 'asc']]));

  let callCount = 0;
  let data: Entry[] = [];
  const listener = (d: Iterable<Entry>) => {
    callCount++;
    data = [...d];
  };
  view.addListener(listener);

  view.hydrate();
  expect(data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
  ]);

  expect(callCount).toBe(1);
  expect(() => view.hydrate()).toThrow("Can't hydrate twice");

  ms.push({row: {a: 3, b: 'c'}, type: 'add'});
  expect(callCount).toBe(2);
  expect(data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
    {a: 3, b: 'c'},
  ]);

  ms.push({row: {a: 2, b: 'b'}, type: 'remove'});
  expect(callCount).toBe(3);
  expect(data).toEqual([
    {a: 1, b: 'a'},
    {a: 3, b: 'c'},
  ]);

  view.removeListener(listener);
  ms.push({row: {a: 1, b: 'a'}, type: 'remove'});
  expect(callCount).toBe(3);

  view.addListener(listener);
  ms.push({row: {a: 3, b: 'c'}, type: 'remove'});
  expect(callCount).toBe(4);
  expect(data).toEqual([]);
});

test('tree', () => {
  const ms = new MemorySource({id: 'number', name: 'string'}, ['id']);
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

  const join = new Join(
    ms.connect([['name', 'asc']]),
    ms.connect([['name', 'desc']]),
    new MemoryStorage(),
    'childID',
    'id',
    'children',
  );

  const expand = (entries: Iterable<Entry>): Entry[] =>
    [...entries].map(e =>
      e.children
        ? {
            ...e,
            children: expand(e.children as Iterable<Entry>),
          }
        : e,
    );

  const view = new View(join);
  let data: Entry[] = [];
  const listener = (d: Iterable<Entry>) => {
    data = expand(d);
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
  ms.push({type: 'add', row: {id: 5, name: 'chocolate', childID: 2}});
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

test('stop-iterators', () => {
  const users = new MemorySource({id: 'number', name: 'string'}, ['id']);
  users.push({
    type: 'add',
    row: {id: 1, name: 'foo', parentID: 3},
  });
  users.push({
    type: 'add',
    row: {id: 2, name: 'foobar', parentID: 3},
  });
  users.push({
    type: 'add',
    row: {id: 3, name: 'mon', parentID: null},
  });

  const join = new Join(
    users.connect([['name', 'asc']]),
    users.connect([['name', 'desc']]),
    new MemoryStorage(),
    'parentID',
    'id',
    'parent',
  );

  const view = new View(join);

  const iters: Iterator<Entry>[] = [];

  view.addListener(parent => {
    const it1 = parent[Symbol.iterator]();
    const e1 = it1.next().value;
    iters.push(it1);
    const it2 = e1.parent[Symbol.iterator]();
    const e2 = it2.next().value;
    expect(e2).toBeTruthy();
    iters.push(it2);
  });

  view.hydrate();
  expect(iters.length).toBe(2);

  for (const it of iters) {
    expect(() => it.next()).throws('Iterator has been stopped');
  }
});
