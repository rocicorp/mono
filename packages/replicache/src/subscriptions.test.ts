import {expect, test} from 'vitest';
import {
  SubscriptionImplForTesting,
  scanInfoMatchesKey,
} from './subscriptions.ts';

test('scanInfoMatchesKey', () => {
  expect(scanInfoMatchesKey({options: {}}, '', 'a')).toBe(true);
  expect(scanInfoMatchesKey({options: {indexName: 'idx'}}, 'idx', 'a')).toBe(
    true,
  );
  expect(scanInfoMatchesKey({options: {indexName: 'idx'}}, '', 'a')).toBe(
    false,
  );
  expect(scanInfoMatchesKey({options: {}}, 'idx', 'a')).toBe(false);

  expect(scanInfoMatchesKey({options: {prefix: 'p'}}, '', 'a')).toBe(false);

  expect(scanInfoMatchesKey({options: {startKey: 'sk'}}, '', 'a')).toBe(false);
  expect(scanInfoMatchesKey({options: {startKey: 'sk'}}, '', 'skate')).toBe(
    true,
  );
  expect(scanInfoMatchesKey({options: {startKey: 'a'}}, '', 'b')).toBe(true);

  expect(
    scanInfoMatchesKey(
      {options: {prefix: 'a', indexName: 'idx'}},
      'idx',
      '\u0000a\u0000b',
    ),
  ).toBe(true);
  expect(
    scanInfoMatchesKey(
      {options: {prefix: 'sb', indexName: 'idx'}},
      'idx',
      '\u0000sa\u0000p',
    ),
  ).toBe(false);

  expect(
    scanInfoMatchesKey(
      {options: {prefix: 'sa', indexName: 'idx', startSecondaryKey: 'sab'}},
      'idx',
      '\u0000sab\u0000p',
    ),
  ).toBe(true);
  expect(
    scanInfoMatchesKey(
      {options: {prefix: 'sa', indexName: 'idx', startSecondaryKey: 'sab'}},
      'idx',
      '\u0000sac\u0000p',
    ),
  ).toBe(true);
  expect(
    scanInfoMatchesKey(
      {options: {prefix: 'sa', indexName: 'idx', startSecondaryKey: 'sac'}},
      'idx',
      '\u0000sab\u0000p',
    ),
  ).toBe(false);

  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'sa',
          indexName: 'idx',
          startSecondaryKey: 'sab',
          startKey: 'pa',
        },
      },
      'idx',
      '\u0000sac\u0000pa',
    ),
  ).toBe(true);
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'sa',
          indexName: 'idx',
          startSecondaryKey: 'sab',
          startKey: 'pab',
        },
      },
      'idx',
      '\u0000sac\u0000pab',
    ),
  ).toBe(true);
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'sa',
          indexName: 'idx',
          startSecondaryKey: 'sab',
          startKey: 'pab',
        },
      },
      'idx',
      '\u0000sac\u0000pac',
    ),
  ).toBe(true);
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'sa',
          indexName: 'idx',
          startSecondaryKey: 'sab',
          startKey: 'pac',
        },
      },
      'idx',
      '\u0000sac\u0000pab',
    ),
  ).toBe(false);
});

test('scanInfoMatchesKey limit optimizations', () => {
  // Start key tests
  // Changed key is equal to inclusive start key
  expect(
    scanInfoMatchesKey(
      {
        options: {
          startKey: 'pac2',
          limit: 10,
        },
      },
      '',
      'pac2',
    ),
  ).toBe(true);

  expect(
    scanInfoMatchesKey(
      {
        options: {
          startKey: 'pac2',
          limit: 10,
        },
        inclusiveLimitKey: 'pac8',
      },
      '',
      'pac2',
    ),
  ).toBe(true);

  // Changed key is after start key, no inclusive limit key
  expect(
    scanInfoMatchesKey(
      {
        options: {
          startKey: 'pac2',
          limit: 10,
        },
      },
      '',
      'pac4',
    ),
  ).toBe(true);

  // Changed key is between inclusive start and inclusive limit keys
  expect(
    scanInfoMatchesKey(
      {
        options: {
          startKey: 'pac2',
          limit: 10,
        },
        inclusiveLimitKey: 'pac8',
      },
      '',
      'pac4',
    ),
  ).toBe(true);

  // Changed key is equal to inclusive limit key
  expect(
    scanInfoMatchesKey(
      {
        options: {
          startKey: 'pac2',
          limit: 10,
        },
        inclusiveLimitKey: 'pac8',
      },
      '',
      'pac8',
    ),
  ).toBe(true);

  // Changed key is after inclusive limit
  expect(
    scanInfoMatchesKey(
      {
        options: {
          startKey: 'pac2',
          limit: 10,
        },
        inclusiveLimitKey: 'pac8',
      },
      '',
      'pac9',
    ),
  ).toBe(false);

  // Changed key is before start key
  expect(
    scanInfoMatchesKey(
      {
        options: {
          startKey: 'pac2',
          limit: 10,
        },
      },
      '',
      'pac1',
    ),
  ).toBe(false);

  expect(
    scanInfoMatchesKey(
      {
        options: {
          startKey: 'pac2',
          limit: 10,
        },
        inclusiveLimitKey: 'pac8',
      },
      '',
      'pac1',
    ),
  ).toBe(false);

  // Changed key is equal to exclusive start key
  expect(
    scanInfoMatchesKey(
      {
        options: {
          startKey: 'pac2',
          startExclusive: true,
          limit: 10,
        },
      },
      '',
      'pac2',
    ),
  ).toBe(false);

  expect(
    scanInfoMatchesKey(
      {
        options: {
          startKey: 'pac2',
          startExclusive: true,
          limit: 10,
        },
        inclusiveLimitKey: 'pac8',
      },
      '',
      'pac2',
    ),
  ).toBe(false);

  // No limit
  expect(
    scanInfoMatchesKey(
      {
        options: {
          startKey: 'pac2',
        },
        inclusiveLimitKey: 'pac8',
      },
      '',
      'pac9',
    ),
  ).toBe(true);

  // Prefix tests
  // Changed key matches prefix and is less than inclusive limit key
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac',
          limit: 10,
        },
        inclusiveLimitKey: 'pac8',
      },
      '',
      'pac1',
    ),
  ).toBe(true);

  // Changed key matches prefix and equals inclusive limit key
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac',
          limit: 10,
        },
        inclusiveLimitKey: 'pac8',
      },
      '',
      'pac8',
    ),
  ).toBe(true);

  // Changed key matches prefix but is after inclusive limit
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac',
          limit: 10,
        },
        inclusiveLimitKey: 'pac8',
      },
      '',
      'pac9',
    ),
  ).toBe(false);

  // Changed key doesn't match prefix but is less than inclusive limit
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac',
          limit: 10,
        },
        inclusiveLimitKey: 'pac8',
      },
      '',
      'pab',
    ),
  ).toBe(false);

  // No limit
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac',
        },
        inclusiveLimitKey: 'pac8',
      },
      '',
      'pac9',
    ),
  ).toBe(true);

  // Start and prefix tests
  // Changed key is equal to inclusive start key and matches prefix
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac2',
          startKey: 'pac22',
          limit: 10,
        },
      },
      '',
      'pac22',
    ),
  ).toBe(true);
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac2',
          startKey: 'pac22',
          limit: 10,
        },
        inclusiveLimitKey: 'pac28',
      },
      '',
      'pac22',
    ),
  ).toBe(true);

  // Changed key is after start key and matches prefix, no inclusive limit key
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac2',
          startKey: 'pac22',
          limit: 10,
        },
      },
      '',
      'pac24',
    ),
  ).toBe(true);

  // Changed key is between inclusive start and inclusive limit keys and matches prefix
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac2',
          startKey: 'pac22',
          limit: 10,
        },
        inclusiveLimitKey: 'pac28',
      },
      '',
      'pac24',
    ),
  ).toBe(true);

  // Changed key is equal to inclusive limit key and matches prefix
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac2',
          startKey: 'pac22',
          limit: 10,
        },
        inclusiveLimitKey: 'pac28',
      },
      '',
      'pac28',
    ),
  ).toBe(true);

  // Changed key match prefix but is after inclusive limit
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac2',
          startKey: 'pac22',
          limit: 10,
        },
        inclusiveLimitKey: 'pac28',
      },
      '',
      'pac29',
    ),
  ).toBe(false);

  // Changed key matches prefix but is before start key
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac2',
          startKey: 'pac22',
          limit: 10,
        },
      },
      '',
      'pac21',
    ),
  ).toBe(false);

  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac2',
          startKey: 'pac22',
          limit: 10,
        },
        inclusiveLimitKey: 'pac28',
      },
      '',
      'pac21',
    ),
  ).toBe(false);

  // Changed key is equal to exclusive start key
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac2',
          startKey: 'pac22',
          startExclusive: true,
          limit: 10,
        },
      },
      '',
      'pac22',
    ),
  ).toBe(false);

  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac2',
          startKey: 'pac22',
          startExclusive: true,
          limit: 10,
        },
        inclusiveLimitKey: 'pac28',
      },
      '',
      'pac22',
    ),
  ).toBe(false);

  // No limit
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac2',
          startKey: 'pac22',
        },
        inclusiveLimitKey: 'pac28',
      },
      '',
      'pac29',
    ),
  ).toBe(true);

  // Changed key is between startKey and lastKey inclusive, but doesnt match prefix
  expect(
    scanInfoMatchesKey(
      {
        options: {
          prefix: 'pac2',
          startKey: 'pab1',
        },
        inclusiveLimitKey: 'pac28',
      },
      '',
      'pab2',
    ),
  ).toBe(false);
});

test('isEqual', () => {
  const dataLog: unknown[] = [];
  const isEqualLog: unknown[] = [];
  const s1 = new Set([1, 2, 3]);
  const s2 = new Set([3, 2, 1]);
  const s3 = new Set([4]);
  const queryResults = [
    {a: 1},
    {b: 2},
    {c: 3},
    {c: 3},
    undefined,
    undefined,
    1,
    2,
    2,
    1,
    s1,
    s2,
    s3,
  ];

  type ElementType<T> =
    T extends Array<infer ElementType> ? ElementType : never;

  const sub = new SubscriptionImplForTesting(
    (): Promise<ElementType<typeof queryResults>> =>
      Promise.reject('should not be called'),
    v => {
      dataLog.push(v);
    },
    undefined,
    undefined,
    (a, b) => {
      isEqualLog.push([a, b]);
      if (a === b) {
        return true;
      }

      if (a instanceof Set && b instanceof Set) {
        return a.size === b.size && [...a].every(v => b.has(v));
      }

      return false;
    },
  );

  for (const result of queryResults) {
    sub.onData(result);
  }
  expect(dataLog).toEqual([
    {a: 1},
    {b: 2},
    {c: 3},
    {c: 3},
    undefined,
    1,
    2,
    1,
    s1,
    // s2 is considered equal to s1
    s3,
  ]);
  expect(isEqualLog).toEqual([
    [{a: 1}, {b: 2}],
    [{b: 2}, {c: 3}],
    [{c: 3}, {c: 3}],
    [{c: 3}, undefined],
    [undefined, undefined],
    [undefined, 1],
    [1, 2],
    [2, 2],
    [2, 1],
    [1, s1],
    [s1, s2],
    [s2, s3],
  ]);
});
