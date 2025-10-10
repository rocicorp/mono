import {expectTypeOf, test} from 'vitest';
import type {ZeroEvent} from './index.ts';
import type {Extend} from './util.ts';

test('Extends handles narrowing and rejects field type changes', () => {
  expectTypeOf<Extend<ZeroEvent, {type: 'foo/bar/baz'}>>().not.toBeNever();

  expectTypeOf<Extend<ZeroEvent, {type: number}>>().toBeNever();
});
