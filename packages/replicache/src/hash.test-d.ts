import {assertType, test} from 'vitest';
import {type Hash, newRandomHash} from './hash.ts';

test('Hash is a branded string', () => {
  assertType<string>(newRandomHash());
  // @ts-expect-error: string literals cannot be assigned to Hash
  const _: Hash = 'abc';
});
