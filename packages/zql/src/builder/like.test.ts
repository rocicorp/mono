import {expect, test} from 'vitest';
import {cases} from './like-test-cases.ts';
import {getLikePredicate} from './like.ts';

test('basics', () => {
  for (const {pattern, flags, inputs} of cases) {
    const op = getLikePredicate(pattern, flags);
    for (const [input, expected] of inputs) {
      expect(op(input), JSON.stringify({pattern, flags, input})).equal(
        expected,
      );
    }
  }
});
