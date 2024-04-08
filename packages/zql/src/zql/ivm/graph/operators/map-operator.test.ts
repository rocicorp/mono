import {expect, test} from 'vitest';
import {Multiset} from '../../multiset.js';
import {DifferenceStream} from '../difference-stream.js';

type E = {id: number};
test('lazy', () => {
  const input = new DifferenceStream<E>();
  let called = false;
  const output = input.map(x => {
    called = true;
    return x;
  });
  const items: Multiset<E>[] = [];
  output.debug((_, d) => {
    items.push(d);
  });

  input.newData(1, [
    [{id: 1}, 1],
    [{id: 2}, 2],
    [{id: 1}, -1],
    [{id: 2}, -2],
  ]);
  input.commit(1);

  // we run the graph but the mapper is not run until we pull on it
  expect(called).toBe(false);

  // drain the output
  for (const item of items) {
    [...item];
  }
  expect(called).toBe(true);
});

test('applies to rows', () => {
  const input = new DifferenceStream<E>();
  const output = input.map(x => ({
    id: x.id * 2,
  }));
  const items: [E, number][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  input.newData(1, [
    [{id: 1}, 1],
    [{id: 2}, 2],
    [{id: 1}, -1],
    [{id: 2}, -2],
  ]);
  input.commit(1);

  expect(items).toEqual([
    [{id: 2}, 1],
    [{id: 4}, 2],
    [{id: 2}, -1],
    [{id: 4}, -2],
  ]);
});
