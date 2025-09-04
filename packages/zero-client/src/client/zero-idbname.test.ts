import {expect, test} from 'vitest';
import {h64} from '../../../shared/src/hash.ts';
import {Zero} from './zero.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';

const schema = createSchema({
  tables: [
    table('foo')
      .columns({
        id: string(),
        value: string(),
      })
      .primaryKey('id'),
  ],
});

const userID = 'test-user';
const storageKey = 'test-storage';

test('idbName generation with URL configuration', async () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const testCases: any[] = [
    {
      name: 'basic mutate and query URLs',
      config: {
        mutate: {url: 'https://example.com/mutate'},
        query: {url: 'https://example.com/query'},
      },
    },
    {
      name: 'different mutate URL',
      config: {
        mutate: {url: 'https://different.com/mutate'},
        query: {url: 'https://example.com/query'},
      },
    },
    {
      name: 'different query URL',
      config: {
        mutate: {url: 'https://example.com/mutate'},
        query: {url: 'https://different.com/query'},
      },
    },
    {
      name: 'no URLs provided',
      config: {},
    },
    {
      name: 'legacy push parameter',
      config: {
        push: {url: 'https://example.com/mutate'},
        query: {url: 'https://example.com/query'},
      },
    },
    {
      name: 'mutate takes precedence over push',
      config: {
        push: {url: 'https://push.com/mutate'},
        mutate: {url: 'https://mutate.com/mutate'},
        query: {url: 'https://example.com/query'},
      },
    },
    {
      name: 'explicit undefined parameters',
      config: {
        mutate: undefined,
        query: undefined,
      },
    },
    {
      name: 'with mutate query parameters',
      config: {
        mutate: {
          url: 'https://example.com/mutate',
          queryParams: {apiKey: 'test123', version: 'v2'},
        },
        query: {url: 'https://example.com/query'},
      },
    },
    {
      name: 'with query parameters for both mutate and query',
      config: {
        mutate: {
          url: 'https://example.com/mutate',
          queryParams: {apiKey: 'mutate123'},
        },
        query: {
          url: 'https://example.com/query',
          queryParams: {apiKey: 'query456', format: 'json'},
        },
      },
    },
    {
      name: 'different storage key produces different hash',
      config: {
        mutate: {url: 'https://example.com/mutate'},
        query: {url: 'https://example.com/query'},
      },
      storageKey: 'different-storage-key',
    },
  ];

  for (const testCase of testCases) {
    const testStorageKey = testCase.storageKey ?? storageKey;
    const zero = new Zero({
      userID,
      storageKey: testStorageKey,
      schema,
      kvStore: 'mem',
      ...testCase.config,
    });

    // Calculate the expected name from the config
    const expectedName = `rep:zero-${userID}-${h64(
      JSON.stringify({
        storageKey: testStorageKey,
        mutateUrl:
          testCase.config.mutate?.url ?? testCase.config.push?.url ?? '',
        queryUrl: testCase.config.query?.url ?? '',
        mutateQueryParams:
          testCase.config.mutate?.queryParams ??
          testCase.config.push?.queryParams ??
          {},
        queryQueryParams: testCase.config.query?.queryParams ?? {},
      }),
    ).toString(36)}`;

    // The idbName should start with the expected prefix
    expect(zero.idbName, `Test case: ${testCase.name}`).toMatch(
      new RegExp(`^${expectedName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`),
    );

    await zero.close();
  }
});
