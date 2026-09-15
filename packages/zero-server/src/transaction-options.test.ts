import {describe, expect, test} from 'vitest';
import {beginStatement} from './transaction-options.ts';

describe('beginStatement', () => {
  test('is a bare BEGIN when no level is asked for', () => {
    expect(beginStatement(undefined)).toBe('BEGIN');
  });

  test('carries each level Postgres accepts', () => {
    expect(beginStatement('read committed')).toBe(
      'BEGIN ISOLATION LEVEL READ COMMITTED',
    );
    expect(beginStatement('repeatable read')).toBe(
      'BEGIN ISOLATION LEVEL REPEATABLE READ',
    );
    expect(beginStatement('serializable')).toBe(
      'BEGIN ISOLATION LEVEL SERIALIZABLE',
    );
  });
});
