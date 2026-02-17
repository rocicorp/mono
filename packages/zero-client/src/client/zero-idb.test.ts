import {expect, test} from 'vitest';
import {hasMemStore} from '../../../replicache/src/kv/mem-store.ts';
import {h64} from '../../../shared/src/hash.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {Zero} from './zero.ts';

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
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  const testCases: any[] = [
    {
      name: 'basic mutate and query URLs',
      config: {
        mutateURL: 'https://example.com/mutate',
        queryURL: 'https://example.com/query',
      },
    },
    {
      name: 'different mutate URL',
      config: {
        mutateURL: 'https://different.com/mutate',
        queryURL: 'https://example.com/query',
      },
    },
    {
      name: 'different query URL',
      config: {
        mutateURL: 'https://example.com/mutate',
        queryURL: 'https://different.com/query',
      },
    },
    {
      name: 'no URLs provided',
      config: {},
    },
    {
      name: 'only mutate URL provided',
      config: {
        mutateURL: 'https://example.com/mutate',
      },
    },
    {
      name: 'only query URL provided',
      config: {
        queryURL: 'https://example.com/query',
      },
    },
    {
      name: 'different storage key produces different hash',
      config: {
        mutateURL: 'https://example.com/mutate',
        queryURL: 'https://example.com/query',
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
        mutateUrl: testCase.config.mutateURL ?? '',
        queryUrl: testCase.config.queryURL ?? '',
      }),
    ).toString(36)}`;

    // The idbName should start with the expected prefix
    expect(zero.idbName, `Test case: ${testCase.name}`).toMatch(
      new RegExp(`^${expectedName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`),
    );

    await zero.close();
  }
});

test('dropAllDatabases drops all known mem stores', async () => {
  const z1 = new Zero({
    userID: 'drop-all-user-1',
    storageKey: 'drop-all-storage-1',
    schema,
    kvStore: 'mem',
  });
  const z2 = new Zero({
    userID: 'drop-all-user-2',
    storageKey: 'drop-all-storage-2',
    schema,
    kvStore: 'mem',
  });

  const dbNames = [z1.idbName, z2.idbName];

  for (const dbName of dbNames) {
    expect(hasMemStore(dbName)).toBe(true);
  }

  await z1.close();
  await z2.close();

  const result = await z1.dropAllDatabases();

  for (const dbName of dbNames) {
    expect(result.dropped).toContain(dbName);
    expect(hasMemStore(dbName)).toBe(false);
  }
  expect(result.errors).toHaveLength(0);
});
