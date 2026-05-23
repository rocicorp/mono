// moved to zero-cache to avoid depending on zero-cache in zqlite, which is a lower-level package. --- IGNORE ---

import {expect, test} from 'vitest';

test('noop', () => {
  expect(true).toBe(true);
});
