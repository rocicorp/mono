import {describe, expect, test} from 'vitest';
import {classifyQueryNames, inferUserID, parseTimestamp} from './workload.ts';

describe('inferUserID', () => {
  test('picks the most frequent top-level or keyed string argument', () => {
    expect(
      inferUserID([
        {args: ['u1']},
        {args: [{userId: 'u1', limit: 50}]},
        {args: [{id: 'w9', languages: ['en', 'en-US']}]},
        {args: ['u2']},
        {args: [null]},
      ]),
    ).toBe('u1');
  });

  test('ignores strings under keys that are not user keys', () => {
    expect(inferUserID([{args: [{id: 'w9'}]}, {args: []}])).toBeUndefined();
  });

  test('honors custom keys', () => {
    expect(inferUserID([{args: [{owner: 'o1'}]}], ['owner'])).toBe('o1');
  });
});

describe('classifyQueryNames', () => {
  test('uses the threshold and the explicit session names', () => {
    const kinds = classifyQueryNames(
      new Map([
        ['homeView', 90],
        ['workById', 20],
        ['libraryBooksV2', 10],
      ]),
      100,
      0.5,
      new Set(['libraryBooksV2']),
    );
    expect(Object.fromEntries(kinds)).toEqual({
      homeView: 'session',
      workById: 'screen',
      libraryBooksV2: 'session',
    });
  });
});

describe('parseTimestamp', () => {
  test('parses Postgres timestamptz text', () => {
    expect(parseTimestamp('2026-09-26 04:35:22.949+00')).toBe(
      Date.UTC(2026, 8, 26, 4, 35, 22, 949),
    );
    expect(parseTimestamp('2026-09-25 18:53:41.73+00')).toBe(
      Date.UTC(2026, 8, 25, 18, 53, 41, 730),
    );
    expect(parseTimestamp('2026-09-25 18:53:41+0530')).toBe(
      Date.UTC(2026, 8, 25, 13, 23, 41),
    );
    expect(parseTimestamp('')).toBeUndefined();
    expect(parseTimestamp('nope')).toBeUndefined();
  });
});
