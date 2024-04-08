import {expect, test} from 'vitest';
import {DifferenceEffectOperator} from './difference-effect-operator.js';
import {DifferenceStream} from '../difference-stream.js';

type E = {x: number};
test('calls effect with raw difference events', () => {
  const input = new DifferenceStream<E>();
  const output = new DifferenceStream<E>();

  let called = false;
  let value;
  let mult = 0;
  new DifferenceEffectOperator(input, output, (v, m) => {
    called = true;
    value = v;
    mult = m;
  });

  input.newData(1, [[{x: 1}, 1]]);

  // effect not run until commit
  expect(called).toBe(false);

  input.commit(1);
  expect(called).toBe(true);
  expect(value).toEqual({x: 1});
  expect(mult).toBe(1);

  called = false;
  value = 0;
  mult = 0;
  input.newData(2, [[{x: 1}, -1]]);

  // effect not run until commit
  expect(called).toBe(false);

  input.commit(2);
  expect(called).toBe(true);
  expect(value).toEqual({x: 1});
  expect(mult).toBe(-1);
});
