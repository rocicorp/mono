import {expectTypeOf, test} from 'vitest';
import {type Hash, newRandomHash} from './hash.ts';

test('hash type', () => {
  const h = newRandomHash();
  expectTypeOf(h).toExtend<Hash>();
  expectTypeOf(h).toExtend<string>();
  const s: string = h;

  // @ts-expect-error Should be an error
  const h2: Hash = 'abc';

  expectTypeOf(s + h2).toBeString();
  expectTypeOf(s + h2).not.toExtend<Hash>();
});
