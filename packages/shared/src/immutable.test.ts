import {expectTypeOf, test} from 'vitest';
import type {Immutable, ImmutableArray} from './immutable.ts';
import type {ReadonlyJSONValue} from './json.ts';

test('type testing immutable', () => {
  const x = [
    {
      a: 1,
      b: '2',
      c: [1, 2, 3],
    },
  ];
  const xi = x as Immutable<typeof x>;
  expectTypeOf(xi[0].c).toEqualTypeOf<ImmutableArray<number>>();
});

test('testing with readonly array', () => {
  type NumberOrArray = number | ReadonlyArray<NumberOrArray>;
  type X = ImmutableArray<NumberOrArray>;
  const x: X = [1];
  expectTypeOf(x).toEqualTypeOf<ImmutableArray<NumberOrArray>>();
});

test('type from discord', () => {
  function f(
    flagsUpdate: ImmutableArray<{
      readonly userID: string;
      readonly value: ReadonlyJSONValue;
    }>,
  ) {
    return flagsUpdate;
  }

  expectTypeOf(f).returns.toEqualTypeOf<
    ImmutableArray<{
      readonly userID: string;
      readonly value: ReadonlyJSONValue;
    }>
  >();

  f([
    {
      userID: '1',
      value: [1],
    },
  ]);
});
