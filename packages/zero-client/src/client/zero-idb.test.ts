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

const schemaV2 = createSchema({
  tables: [
    table('foo')
      .columns({
        id: string(),
        value: string(),
        value2: string(),
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

test('delete closes and removes all databases for the same zero instance', async () => {
  const userIDForDrop = 'drop-db-user';
  const storageKeyForDrop = 'drop-db-storage';

  const zOld = new Zero({
    userID: userIDForDrop,
    storageKey: storageKeyForDrop,
    schema,
    kvStore: 'mem',
  });
  const oldDBName = zOld.idbName;
  await zOld.close();

  const zCurrent = new Zero({
    userID: userIDForDrop,
    storageKey: storageKeyForDrop,
    schema: schemaV2,
    kvStore: 'mem',
  });
  const currentDBName = zCurrent.idbName;

  const zOther = new Zero({
    userID: 'drop-db-other-user',
    storageKey: 'drop-db-other-storage',
    schema,
    kvStore: 'mem',
  });
  const otherDBName = zOther.idbName;

  expect(zCurrent.closed).toBe(false);
  expect(hasMemStore(oldDBName)).toBe(true);
  expect(hasMemStore(currentDBName)).toBe(true);
  expect(hasMemStore(otherDBName)).toBe(true);

  const result = await zCurrent.delete();

  expect(zCurrent.closed).toBe(true);
  expect(result.errors).toHaveLength(0);
  expect(result.deleted).toContain(oldDBName);
  expect(result.deleted).toContain(currentDBName);
  expect(hasMemStore(oldDBName)).toBe(false);
  expect(hasMemStore(currentDBName)).toBe(false);

  expect(hasMemStore(otherDBName)).toBe(true);

  await zOther.close();
  await zOther.delete();
});
