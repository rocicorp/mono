import {describe, expect, test} from 'vitest';
import {currentValue, parseArgRewrites, rewriteArgs} from './arg-rewrites.ts';

describe('parseArgRewrites', () => {
  test('parses repeated and comma-separated entries', () => {
    expect(
      parseArgRewrites([
        'homeStories.now=hour',
        'goalsThrough.endDay=day, clock.t=now',
      ]),
    ).toEqual([
      {name: 'homeStories', key: 'now', unit: 'hour'},
      {name: 'goalsThrough', key: 'endDay', unit: 'day'},
      {name: 'clock', key: 't', unit: 'now'},
    ]);
  });

  test('rejects malformed entries', () => {
    for (const bad of ['homeStories=hour', '.now=hour', 'a.b=week', 'a.=day']) {
      expect(() => parseArgRewrites([bad])).toThrow('Invalid --arg-rewrite');
    }
  });
});

test('currentValue floors to the start of the UTC unit', () => {
  const t = Date.UTC(2026, 8, 28, 13, 47, 12, 345);
  expect(currentValue('now', t)).toBe(t);
  expect(currentValue('minute', t)).toBe(Date.UTC(2026, 8, 28, 13, 47));
  expect(currentValue('hour', t)).toBe(Date.UTC(2026, 8, 28, 13));
  expect(currentValue('day', t)).toBe(Date.UTC(2026, 8, 28));
});

describe('rewriteArgs', () => {
  const now = Date.UTC(2026, 8, 28, 13, 47);
  const rewrites = parseArgRewrites(['homeStories.now=hour']);

  test('replaces the key in object arguments of matching queries', () => {
    expect(
      rewriteArgs(
        {
          name: 'homeStories',
          args: [{userId: 'u1', now: 1790395200000}],
          ttlMs: 300_000,
        },
        rewrites,
        now,
      ),
    ).toEqual({
      name: 'homeStories',
      args: [{userId: 'u1', now: Date.UTC(2026, 8, 28, 13)}],
      ttlMs: 300_000,
    });
  });

  test('leaves other queries and arguments without the key alone', () => {
    const other = {name: 'homeView', args: ['u1'], ttlMs: 300_000};
    expect(rewriteArgs(other, rewrites, now)).toBe(other);
    const noKey = {name: 'homeStories', args: [{userId: 'u1'}], ttlMs: 1};
    expect(rewriteArgs(noKey, rewrites, now)).toEqual(noKey);
  });
});
