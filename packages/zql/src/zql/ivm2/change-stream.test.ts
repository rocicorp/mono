import {expect, test} from 'vitest';
import {ChangeStream} from './change-stream.js';

test('needy', () => {
  const arr = [1, 2, 3];
  const cases: {
    numToConsume: number;
    expectError: boolean;
  }[] = [
    {numToConsume: 1, expectError: true},
    {numToConsume: 2, expectError: true},
    {numToConsume: 3, expectError: true},
    {numToConsume: 4, expectError: false},
    {numToConsume: 1, expectError: true},
    {numToConsume: 2, expectError: true},
    {numToConsume: 3, expectError: true},
    {numToConsume: 4, expectError: false},
  ];

  const gen = function* (it: Iterable<number>) {
    for (const x of it) {
      yield x;
    }
  };

  for (const mode of ['normal', 'needy'] as const) {
    for (const wrapInGenerator of [false, true]) {
      for (const c of cases) {
        const {numToConsume, expectError} = c;
        const f = () => {
          let count = 0;
          let it: Iterable<number> = new ChangeStream(arr, mode);
          if (wrapInGenerator) {
            it = gen(it);
          }
          for (const _ of it) {
            if (++count === numToConsume) {
              break;
            }
          }
        };

        if (mode === 'needy' && expectError) {
          expect(f, JSON.stringify({mode, wrapInGenerator, c})).toThrow(
            'NeedyIterator was not fully consumed!',
          );
        } else {
          expect(f, JSON.stringify({mode, wrapInGenerator, c})).not.toThrow();
        }
      }
    }
  }
});

test('once', () => {
  const arr = [1, 2, 3];
  const cs = new ChangeStream(arr);
  expect([...cs]).toEqual(arr);
  expect([...cs]).toEqual([]);
});
