import {expect, test} from 'vitest';
import {Multiset} from '../../multiset.js';
import {DifferenceStream, concat} from '../difference-stream.js';

test('All branches notify', () => {
  type T = {x: number};
  const inputs = [
    new DifferenceStream<T>(),
    new DifferenceStream<T>(),
    new DifferenceStream<T>(),
  ];
  const output = concat(inputs);

  let version = 1;

  const items: Multiset<T>[] = [];
  output.debug((v, d) => {
    expect(v).toBe(version);
    items.push(d);
  });

  inputs[0].newData(version, [
    [{x: 1}, 1],
    [{x: 2}, 2],
  ]);
  inputs[0].commit(version);

  expect(items).toEqual([
    [
      [{x: 1}, 1],
      [{x: 2}, 2],
    ],
  ]);

  items.length = 0;
  version++;

  inputs[0].newData(version, [[{x: 0}, 1]]);
  inputs[1].newData(version, [[{x: 1}, 1]]);
  inputs[2].newData(version, [[{x: 2}, 2]]);
  inputs[0].commit(version);
  inputs[1].commit(version);
  inputs[2].commit(version);
  expect(items).toEqual([[[{x: 0}, 1]], [[{x: 1}, 1]], [[{x: 2}, 2]]]);
});

test('Test with single input', () => {
  type T = {x: number};
  const input = new DifferenceStream<T>();

  const output = concat([input]);

  const version = 1;

  const items: Multiset<T>[] = [];
  output.debug((v, d) => {
    expect(v).toBe(version);
    items.push(d);
  });

  input.newData(version, [
    [{x: 1}, 1],
    [{x: 2}, 2],
  ]);
  input.commit(version);

  expect(items).toEqual([
    [
      [{x: 1}, 1],
      [{x: 2}, 2],
    ],
  ]);
});
