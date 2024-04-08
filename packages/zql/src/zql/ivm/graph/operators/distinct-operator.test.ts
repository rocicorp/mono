import {expect, test} from 'vitest';
import {Multiset} from '../../multiset.js';
import {DifferenceStream} from '../difference-stream.js';

test('distinct', () => {
  type T = {
    id: string;
  };
  const input = new DifferenceStream<T>();
  const output = input.distinct();

  let version = 1;

  const items: Multiset<T>[] = [];
  output.debug((v, d) => {
    expect(v).toBe(version);
    items.push(d);
  });

  input.newData(version, [
    [{id: 'a'}, 1],
    [{id: 'b'}, 2],
    [{id: 'a'}, -1],
    [{id: 'c'}, -3],
  ]);
  input.commit(version);

  expect(items).toEqual([
    [
      [{id: 'b'}, 1],
      [{id: 'c'}, -1],
    ],
  ]);

  version++;
  items.length = 0;
  input.newData(version, [[{id: 'b'}, -2]]);
  input.commit(version);
  expect(items).toEqual([[[{id: 'b'}, -1]]]);

  version++;
  items.length = 0;
  input.newData(version, [[{id: 'd'}, -1]]);
  input.newData(version, [[{id: 'd'}, 1]]);
  input.commit(version);
  expect(items).toEqual([[[{id: 'd'}, -1]], [[{id: 'd'}, 1]]]);

  version++;
  items.length = 0;
  input.newData(version, [[{id: 'e'}, -1]]);
  input.newData(version, [[{id: 'e'}, 5]]);
  input.commit(version);
  expect(items).toEqual([[[{id: 'e'}, -1]], [[{id: 'e'}, 2]]]);

  version++;
  items.length = 0;
  input.newData(version, [[{id: 'e'}, 5]]);
  input.newData(version, [[{id: 'e'}, -6]]);
  input.commit(version);
  expect(items).toEqual([[[{id: 'e'}, 1]], [[{id: 'e'}, -2]]]);
});
