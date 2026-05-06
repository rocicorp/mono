import {assert} from './asserts.ts';

export const TESTING = import.meta.env?.VITEST;

export function assertTesting(msg = 'Expected to be in test mode'): void {
  assert(TESTING, msg);
}
