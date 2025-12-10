import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {randomUint64} from '../../../shared/src/random-uint64.ts';
import {TestMemStore} from '../kv/test-mem-store.ts';
import {
  IDBDatabasesStore,
  PROFILE_ID_KEY,
  type IndexedDBDatabase,
} from './idb-databases-store.ts';

afterEach(() => {
  vi.restoreAllMocks();
});

test('getDatabases with no existing record in db', async () => {
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  expect(await store.getDatabases()).toEqual({});
});

test('putDatabase with no existing record in db', async () => {
  vi.setSystemTime(1);

  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB = {
    name: 'testName',
    replicacheName: 'testReplicacheName',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion',
  };
  expect(await store.putDatabase(testDB)).toEqual({
    testName: withLastOpenedTimestampMS(testDB, 1),
  });
  expect(await store.getDatabases()).toEqual({
    testName: withLastOpenedTimestampMS(testDB, 1),
  });
});

test('putDatabase updates lastOpenedTimestampMS', async () => {
  vi.setSystemTime(1);

  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB = {
    name: 'testName',
    replicacheName: 'testReplicacheName',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion',
  };
  expect(await store.putDatabase(testDB)).toEqual({
    testName: withLastOpenedTimestampMS(testDB, 1),
  });
  expect(await store.getDatabases()).toEqual({
    testName: withLastOpenedTimestampMS(testDB, 1),
  });

  vi.setSystemTime(2);
  expect(await store.putDatabase(testDB)).toEqual({
    testName: withLastOpenedTimestampMS(testDB, 2),
  });
  expect(await store.getDatabases()).toEqual({
    testName: withLastOpenedTimestampMS(testDB, 2),
  });
});

test('putDatabase ignores passed in lastOpenedTimestampMS', async () => {
  vi.setSystemTime(2);

  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB = {
    name: 'testName',
    replicacheName: 'testReplicacheName',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion',
    lastOpenedTimestampMS: 1,
  };
  expect(await store.putDatabase(testDB)).toEqual({
    testName: withLastOpenedTimestampMS(testDB, 2),
  });
  expect(await store.getDatabases()).toEqual({
    testName: withLastOpenedTimestampMS(testDB, 2),
  });
});

function withLastOpenedTimestampMS(
  db: IndexedDBDatabase,
  lastOpenedTimestampMS: number,
): IndexedDBDatabase {
  return {
    ...db,
    lastOpenedTimestampMS,
  };
}

test('putDatabase sequence', async () => {
  vi.setSystemTime(1);
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB1 = {
    name: 'testName1',
    replicacheName: 'testReplicacheName1',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion1',
  };

  expect(await store.putDatabase(testDB1)).toEqual({
    testName1: withLastOpenedTimestampMS(testDB1, 1),
  });
  expect(await store.getDatabases()).toEqual({
    testName1: withLastOpenedTimestampMS(testDB1, 1),
  });

  const testDB2 = {
    name: 'testName2',
    replicacheName: 'testReplicacheName2',
    replicacheFormatVersion: 2,
    schemaVersion: 'testSchemaVersion2',
  };

  vi.setSystemTime(2);

  expect(await store.putDatabase(testDB2)).toEqual({
    testName1: withLastOpenedTimestampMS(testDB1, 1),
    testName2: withLastOpenedTimestampMS(testDB2, 2),
  });
  expect(await store.getDatabases()).toEqual({
    testName1: withLastOpenedTimestampMS(testDB1, 1),
    testName2: withLastOpenedTimestampMS(testDB2, 2),
  });
});

test('close closes kv store', async () => {
  const memstore = new TestMemStore();
  const store = new IDBDatabasesStore(_ => memstore);
  expect(memstore.closed).toBe(false);
  await store.close();
  expect(memstore.closed).toBe(true);
});

test('clear', async () => {
  vi.setSystemTime(1);
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB1 = {
    name: 'testName1',
    replicacheName: 'testReplicacheName1',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion1',
  };

  expect(await store.putDatabase(testDB1)).toEqual({
    testName1: withLastOpenedTimestampMS(testDB1, Date.now()),
  });
  expect(await store.getDatabases()).toEqual({
    testName1: withLastOpenedTimestampMS(testDB1, Date.now()),
  });

  await store.clearDatabases();

  expect(await store.getDatabases()).toEqual({});

  const testDB2 = {
    name: 'testName2',
    replicacheName: 'testReplicacheName2',
    replicacheFormatVersion: 2,
    schemaVersion: 'testSchemaVersion2',
  };

  vi.setSystemTime(2);

  expect(await store.putDatabase(testDB2)).toEqual({
    testName2: withLastOpenedTimestampMS(testDB2, Date.now()),
  });
  expect(await store.getDatabases()).toEqual({
    testName2: withLastOpenedTimestampMS(testDB2, Date.now()),
  });
});

describe('getProfileID', () => {
  // mock import {randomUint64} from '../../../shared/src/random-uint64.ts'; to return predictable values
  vi.mock('../../../shared/src/random-uint64.ts', async importOriginal => {
    const original = await importOriginal<
      // oxlint-disable-next-line consistent-type-imports
      typeof import('../../../shared/src/random-uint64.ts')
    >();

    return {
      randomUint64: vi.fn(() => original.randomUint64()),
    };
  });

  beforeEach(() => {
    // mock localStorage.getItem to return a predictable value
    const localStorageMock = {
      getItem: vi.fn().mockReturnValue('p00000g000000000099'),
      setItem: vi.fn(),
    };
    vi.stubGlobal('localStorage', localStorageMock);
    return () => {
      vi.unstubAllGlobals();
    };
  });

  test('empty KV Store, empty localStorage', async () => {
    vi.mocked(localStorage.getItem).mockReturnValueOnce(null);
    vi.mocked(randomUint64)
      .mockReturnValueOnce(1234n)
      .mockReturnValueOnce(5678n);
    const store = new IDBDatabasesStore(_ => new TestMemStore());
    const profileID = await store.getProfileID();
    expect(profileID).toBe('p000j900000000005he');

    const profileID2 = await store.getProfileID();
    expect(profileID2).toBe(profileID);
  });

  test('Fallback to localStorage', async () => {
    const mockedProfileID = 'pMockedProfileID1234567';
    vi.mocked(localStorage.getItem).mockReturnValue(mockedProfileID);

    const store = new IDBDatabasesStore(_ => new TestMemStore());
    const profileID = await store.getProfileID();
    expect(profileID).toBe(mockedProfileID);

    expect(vi.mocked(localStorage.getItem)).toBeCalledTimes(1);
    expect(vi.mocked(localStorage.getItem)).toHaveBeenCalledWith(
      PROFILE_ID_KEY,
    );
    expect(vi.mocked(localStorage.setItem)).toHaveBeenCalledWith(
      PROFILE_ID_KEY,
      mockedProfileID,
    );

    vi.mocked(localStorage.getItem).mockClear();
    vi.mocked(localStorage.setItem).mockClear();
    const profileID2 = await store.getProfileID();
    expect(profileID2).toBe(profileID);

    // not called again
    expect(vi.mocked(localStorage.getItem)).not.toHaveBeenCalled();
    expect(vi.mocked(localStorage.setItem)).not.toHaveBeenCalled();
  });
});
