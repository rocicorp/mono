import {expect, test} from 'vitest';
import {Catch} from './catch.js';
import {Filter} from './filter.js';
import {MemorySource} from './memory-source.js';

test('basics', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({type: 'add', row: {a: 3, b: 'foo'}});
  ms.push({type: 'add', row: {a: 2, b: 'bar'}});
  ms.push({type: 'add', row: {a: 1, b: 'foo'}});

  const connector = ms.connect([['a', 'asc']]);
  const filter = new Filter(connector, 'all', row => row.b === 'foo');
  const out = new Catch(filter);

  expect(out.fetch()).toEqual([
    {row: {a: 1, b: 'foo'}, relationships: {}},
    {row: {a: 3, b: 'foo'}, relationships: {}},
  ]);

  ms.push({type: 'add', row: {a: 4, b: 'bar'}});
  ms.push({type: 'add', row: {a: 5, b: 'foo'}});
  ms.push({type: 'remove', row: {a: 3, b: 'foo'}});
  ms.push({type: 'remove', row: {a: 2, b: 'bar'}});

  expect(out.pushes).toEqual([
    {
      type: 'add',
      node: {row: {a: 5, b: 'foo'}, relationships: {}},
    },
    {
      type: 'remove',
      node: {row: {a: 3, b: 'foo'}, relationships: {}},
    },
  ]);

  expect(out.cleanup({})).toEqual([
    {
      row: {a: 1, b: 'foo'},
      relationships: {},
    },
    {
      row: {a: 5, b: 'foo'},
      relationships: {},
    },
  ]);
});

test('edit', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, x: {type: 'number'}},
    ['a'],
  );
  for (const row of [
    {a: 1, x: 1},
    {a: 2, x: 2},
    {a: 3, x: 3},
  ]) {
    ms.push({type: 'add', row});
  }

  const connector = ms.connect([['a', 'asc']]);
  const filter = new Filter(
    connector,
    'all',
    row => (row.x as number) % 2 === 0,
  );
  const out = new Catch(filter);

  expect(out.fetch()).toEqual([{row: {a: 2, x: 2}, relationships: {}}]);

  ms.push({type: 'add', row: {a: 4, x: 4}});
  ms.push({type: 'edit', oldRow: {a: 3, x: 3}, row: {a: 3, x: 6}});

  expect(out.pushes).toEqual([
    {
      type: 'add',
      node: {
        row: {a: 4, x: 4},
        relationships: {},
      },
    },
    {
      type: 'add',
      node: {
        row: {a: 3, x: 6},
        relationships: {},
      },
    },
  ]);

  expect(out.fetch({})).toEqual([
    {row: {a: 2, x: 2}, relationships: {}},
    {row: {a: 3, x: 6}, relationships: {}},
    {row: {a: 4, x: 4}, relationships: {}},
  ]);

  out.pushes.length = 0;
  ms.push({type: 'edit', oldRow: {a: 3, x: 6}, row: {a: 3, x: 5}});
  expect(out.pushes).toEqual([
    {
      type: 'remove',
      node: {
        row: {a: 3, x: 6},
        relationships: {},
      },
    },
  ]);
  expect(out.fetch({})).toEqual([
    {row: {a: 2, x: 2}, relationships: {}},
    {row: {a: 4, x: 4}, relationships: {}},
  ]);

  out.pushes.length = 0;
  ms.push({type: 'edit', oldRow: {a: 3, x: 5}, row: {a: 3, x: 7}});
  expect(out.pushes).toEqual([]);
  expect(out.fetch({})).toEqual([
    {row: {a: 2, x: 2}, relationships: {}},
    {row: {a: 4, x: 4}, relationships: {}},
  ]);

  out.pushes.length = 0;
  ms.push({type: 'edit', oldRow: {a: 2, x: 2}, row: {a: 2, x: 4}});
  expect(out.pushes).toEqual([
    {type: 'edit', oldRow: {a: 2, x: 2}, row: {a: 2, x: 4}},
  ]);
  expect(out.fetch({})).toEqual([
    {row: {a: 2, x: 4}, relationships: {}},
    {row: {a: 4, x: 4}, relationships: {}},
  ]);
});
