import {expect, test} from 'vitest';
import {Catch} from './catch.js';
import {ChangeType} from './change.js';
import {Filter} from './filter.js';
import {MemorySource} from './memory-source.js';

test('basics', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({type: ChangeType.Add, row: {a: 3, b: 'foo'}});
  ms.push({type: ChangeType.Add, row: {a: 2, b: 'bar'}});
  ms.push({type: ChangeType.Add, row: {a: 1, b: 'foo'}});

  const connector = ms.connect([['a', 'asc']]);
  const filter = new Filter(connector, 'all', row => row.b === 'foo');
  const out = new Catch(filter);

  expect(out.fetch()).toEqual([
    {row: {a: 1, b: 'foo'}, relationships: {}},
    {row: {a: 3, b: 'foo'}, relationships: {}},
  ]);

  ms.push({type: ChangeType.Add, row: {a: 4, b: 'bar'}});
  ms.push({type: ChangeType.Add, row: {a: 5, b: 'foo'}});
  ms.push({type: ChangeType.Remove, row: {a: 3, b: 'foo'}});
  ms.push({type: ChangeType.Remove, row: {a: 2, b: 'bar'}});

  expect(out.pushes).toEqual([
    {
      type: ChangeType.Add,
      node: {row: {a: 5, b: 'foo'}, relationships: {}},
    },
    {
      type: ChangeType.Remove,
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
