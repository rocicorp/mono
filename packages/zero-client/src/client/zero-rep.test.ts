import {describe, expect, test} from 'vitest';
import {fromReplicacheAuthToken, toReplicacheAuthToken} from './zero-rep.ts';

describe('toReplicacheAuthToken', () => {
  test('returns provided auth token when defined', () => {
    expect(toReplicacheAuthToken('my-token')).toBe('my-token');
  });

  test('returns empty string when auth token missing', () => {
    expect(toReplicacheAuthToken(undefined)).toBe('');
    expect(toReplicacheAuthToken(null)).toBe('');
    expect(toReplicacheAuthToken('')).toBe('');
  });
});

describe('fromReplicacheAuthToken', () => {
  test('returns undefined for falsey string tokens', () => {
    expect(fromReplicacheAuthToken('')).toBeUndefined();
    expect(fromReplicacheAuthToken(null as unknown as string)).toBeUndefined();
    expect(
      fromReplicacheAuthToken(undefined as unknown as string),
    ).toBeUndefined();
  });

  test('returns auth token when provided', () => {
    expect(fromReplicacheAuthToken('my-token')).toBe('my-token');
  });

  test('round-trips values produced by toReplicacheAuthToken', () => {
    expect(fromReplicacheAuthToken(toReplicacheAuthToken('my-token'))).toBe(
      'my-token',
    );
    expect(
      fromReplicacheAuthToken(toReplicacheAuthToken(undefined)),
    ).toBeUndefined();
  });
});
