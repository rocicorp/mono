import {expect} from 'chai';
import {scanInfoMatchesKey} from './subscriptions.js';

test('scanInfoMatchesKey', () => {
  expect(scanInfoMatchesKey({options: {}}, '', 'a')).to.be.true;
  expect(scanInfoMatchesKey({options: {indexName: 'idx'}}, 'idx', 'a')).to.be
    .true;
  expect(scanInfoMatchesKey({options: {indexName: 'idx'}}, '', 'a')).to.be
    .false;
  expect(scanInfoMatchesKey({options: {}}, 'idx', 'a')).to.be.false;

  expect(scanInfoMatchesKey({options: {prefix: 'p'}}, '', 'a')).to.be.false;

  expect(scanInfoMatchesKey({options: {startKey: 'sk'}}, '', 'a')).to.be.false;
  expect(scanInfoMatchesKey({options: {startKey: 'sk'}}, '', 'skate')).to.be
    .true;
  expect(scanInfoMatchesKey({options: {startKey: 'a'}}, '', 'b')).to.be.true;

  expect(
    scanInfoMatchesKey(
      {options: {prefix: 'a', indexName: 'idx'}},
      'idx',
      '\u0000a\u0000b',
    ),
  ).to.be.true;
  expect(
    scanInfoMatchesKey(
      {options: {prefix: 'sb', indexName: 'idx'}},
      'idx',
      '\u0000sa\u0000p',
    ),
  ).to.be.false;

  expect(
    scanInfoMatchesKey(
      {options: {prefix: 'sa', indexName: 'idx', startSecondaryKey: 'sab'}},
      'idx',
      '\u0000sab\u0000p',
    ),
  ).to.be.true;
  expect(
    scanInfoMatchesKey(
      {options: {prefix: 'sa', indexName: 'idx', startSecondaryKey: 'sab'}},
      'idx',
      '\u0000sac\u0000p',
    ),
  ).to.be.true;
  expect(
    scanInfoMatchesKey(
      {options: {prefix: 'sa', indexName: 'idx', startSecondaryKey: 'sac'}},
      'idx',
      '\u0000sab\u0000p',
    ),
  ).to.be.false;

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
  ).to.be.true;
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
  ).to.be.true;
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
  ).to.be.true;
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
  ).to.be.false;
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
  ).to.be.true;

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
  ).to.be.true;

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
  ).to.be.true;

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
  ).to.be.true;

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
  ).to.be.true;

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
  ).to.be.false;

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
  ).to.be.false;

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
  ).to.be.false;

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
  ).to.be.false;

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
  ).to.be.false;

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
  ).to.be.true;

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
  ).to.be.true;

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
  ).to.be.true;

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
  ).to.be.false;

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
  ).to.be.false;

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
  ).to.be.true;

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
  ).to.be.true;
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
  ).to.be.true;

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
  ).to.be.true;

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
  ).to.be.true;

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
  ).to.be.true;

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
  ).to.be.false;

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
  ).to.be.false;

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
  ).to.be.false;

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
  ).to.be.false;

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
  ).to.be.false;

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
  ).to.be.true;

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
  ).to.be.false;
});
