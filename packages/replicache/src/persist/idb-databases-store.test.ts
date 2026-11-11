import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {TestLogSink} from '../../../shared/src/logging-test-utils.ts';
import {randomUint64} from '../../../shared/src/random-uint64.ts';
import {TestMemStore} from '../kv/test-mem-store.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {
  IDBDatabasesStore,
  PROFILE_ID_KEY,
  type IndexedDBDatabase,
} from './idb-databases-store.ts';

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

afterEach(() => {
  vi.restoreAllMocks();
});

test('getDatabases with no existing record in db', async () => {
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  expect(await store.getDatabases()).toEqual({});
});

test('putDatabase with no existing record in db', async () => {
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB: IndexedDBDatabase = {
    name: 'testName',
    replicacheName: 'testReplicacheName',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion',
  };
  expect(await store.putDatabase(testDB)).toEqual({
    testName: testDB,
  });
  expect(await store.getDatabases()).toEqual({
    testName: testDB,
  });
});

test('putDatabase sequence', async () => {
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB1: IndexedDBDatabase = {
    name: 'testName1',
    replicacheName: 'testReplicacheName1',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion1',
  };

  expect(await store.putDatabase(testDB1)).toEqual({
    testName1: testDB1,
  });
  expect(await store.getDatabases()).toEqual({
    testName1: testDB1,
  });

  const testDB2: IndexedDBDatabase = {
    name: 'testName2',
    replicacheName: 'testReplicacheName2',
    replicacheFormatVersion: 2,
    schemaVersion: 'testSchemaVersion2',
  };

  expect(await store.putDatabase(testDB2)).toEqual({
    testName1: testDB1,
    testName2: testDB2,
  });
  expect(await store.getDatabases()).toEqual({
    testName1: testDB1,
    testName2: testDB2,
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
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB1: IndexedDBDatabase = {
    name: 'testName1',
    replicacheName: 'testReplicacheName1',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion1',
  };

  expect(await store.putDatabase(testDB1)).toEqual({
    testName1: testDB1,
  });
  expect(await store.getDatabases()).toEqual({
    testName1: testDB1,
  });

  await store.clearDatabases();

  expect(await store.getDatabases()).toEqual({});

  const testDB2: IndexedDBDatabase = {
    name: 'testName2',
    replicacheName: 'testReplicacheName2',
    replicacheFormatVersion: 2,
    schemaVersion: 'testSchemaVersion2',
  };

  expect(await store.putDatabase(testDB2)).toEqual({
    testName2: testDB2,
  });
  expect(await store.getDatabases()).toEqual({
    testName2: testDB2,
  });
});

describe('a malformed registry', () => {
  const valid: IndexedDBDatabase = {
    name: 'valid',
    replicacheName: 'app',
    replicacheFormatVersion: 7,
    schemaVersion: '1',
  };
  const other: IndexedDBDatabase = {...valid, name: 'other'};

  async function setUp(dbs: unknown) {
    const kv = new TestMemStore();
    await withWrite(kv, w => w.put('dbs', dbs as never));
    const sink = new TestLogSink();
    const store = new IDBDatabasesStore(
      _ => kv,
      new LogContext('debug', {}, sink),
    );
    return {kv, sink, store};
  }

  test.each([
    ['a missing field', {name: 'bad'}],
    ['a field of the wrong type', {...valid, name: 'bad', schemaVersion: 1}],
    ['an entry that is not an object', 'bad'],
    ['a key that is not its name', {...valid, name: 'not-bad'}],
  ])('reads past an entry with %s', async (_, bad) => {
    const {sink, store} = await setUp({valid, bad});
    expect(await store.getDatabases()).toEqual({valid});
    expect(sink.messages).toEqual([
      [
        'warn',
        {},
        ['Ignoring malformed entry "bad" in the databases registry.'],
      ],
    ]);
  });

  test('reads a record that is not an object as empty', async () => {
    const {sink, store} = await setUp(['not', 'a', 'record']);
    expect(await store.getDatabases()).toEqual({});
    expect(sink.messages).toEqual([
      ['warn', {}, ['Ignoring the databases registry: it is not an object.']],
    ]);
  });

  test('putDatabase succeeds and writes the record back without it', async () => {
    const {kv, store} = await setUp({valid, bad: {name: 'bad'}});
    expect(await store.putDatabase(other)).toEqual({valid, other});
    expect(await withRead(kv, r => r.get('dbs'))).toEqual({valid, other});
  });

  test('deleteDatabases succeeds and writes the record back without it', async () => {
    const {kv, store} = await setUp({valid, other, bad: {name: 'bad'}});
    await store.deleteDatabases(['other']);
    expect(await withRead(kv, r => r.get('dbs'))).toEqual({valid});
  });
});

describe('getProfileID', () => {
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
