import {expect, test} from 'vitest';
import type {Entry} from '../../multiset.js';
import {DifferenceStream} from '../difference-stream.js';

type E = {id: number};

test('does not emit any rows that fail the filter', () => {
  const input = new DifferenceStream<E>();

  const out = input.filter([null, 'id'], '<', 0);
  const items: E[] = [];
  out.effect((e: E) => {
    items.push(e);
  });

  input.newDifference(
    1,
    [
      [{id: 1}, 1],
      [{id: 2}, 2],
      [{id: 1}, -1],
      [{id: 2}, -2],
    ],
    undefined,
  );
  input.commit(1);

  expect(items.length).toBe(0);
});

test('emits all rows that pass the filter (including deletes / retractions)', () => {
  const input = new DifferenceStream<E>();
  const out = input.filter([null, 'id'], '>', 0);

  const items: Entry<E>[] = [];
  out.effect((e: E, mult: number) => {
    items.push([e, mult]);
  });

  input.newDifference(
    1,
    [
      [{id: 1}, 1],
      [{id: 2}, 2],
      [{id: 1}, -1],
      [{id: 2}, -2],
    ],
    undefined,
  );
  input.commit(1);

  expect(items).toEqual([
    [{id: 1}, 1],
    [{id: 2}, 2],
    [{id: 1}, -1],
    [{id: 2}, -2],
  ]);
});
