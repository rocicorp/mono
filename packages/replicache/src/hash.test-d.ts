import {test} from 'vitest';
import {type Hash, newRandomHash} from './hash.ts';

test('type checking only', () => {
  const h = newRandomHash();
  // Should not be an error
  const s: string = h;

  // @ts-expect-error Should be an error
  const h2: Hash = 'abc';

  return s + h2;
});
