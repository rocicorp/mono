import {resolver} from '@rocicorp/resolver';
import {deepEqual} from '../src/json.js';
import {assert} from '../src/asserts.js';
import {
  MutatorDefs,
  PatchOperation,
  ReadTransaction,
  Replicache,
  ReplicacheOptions,
  TEST_LICENSE_KEY,
  WriteTransaction,
  IndexDefinitions,
  JSONValue,
  UpdateNeededReason,
} from '../out/replicache.js';
import {
  jsonArrayTestData,
  TestDataObject,
  jsonObjectTestData,
  getTmcwData,
} from './data.js';
import type {Bencher, Benchmark} from './perf.js';
import {dropStore as dropIDBStore} from '../src/kv/idb-util.js';
import {uuid} from '../src/uuid.js';
import type {ReplicacheInternalAPI} from '../src/replicache-options.js';

const valSize = 1024;

type Writable<T> = {-readonly [P in keyof T]: T[P]};

export function benchmarkPopulate(opts: {
  numKeys: number;
  clean: boolean;
  indexes?: number;
}): Benchmark {
  let repToClose: ReplicacheWithPopulate | undefined;
  return {
    name: `populate ${valSize}x${opts.numKeys} (${
      opts.clean ? 'clean' : 'dirty'
    }, ${`indexes: ${opts.indexes || 0}`})`,
    group: 'replicache',
    byteSize: opts.numKeys * valSize,
    async teardownEach() {
      await closeAndCleanupRep(repToClose);
    },
    async run(bencher: Bencher) {
      const indexes: IndexDefinitions = createIndexDefinitions(
        opts.indexes ?? 0,
      );
      const rep = (repToClose = makeRepWithPopulate({indexes}));

      // Wait for init.
      await rep.clientID;

      if (!opts.clean) {
        await rep.mutate.populate({
          numKeys: opts.numKeys,
          randomValues: jsonArrayTestData(opts.numKeys, valSize),
        });
      }
      const randomValues = jsonArrayTestData(opts.numKeys, valSize);
      bencher.reset();
      await rep.mutate.populate({numKeys: opts.numKeys, randomValues});
      bencher.stop();
    },
  };
}

export function benchmarkPersist(opts: {
  numKeys: number;
  indexes?: number;
}): Benchmark {
  let repToClose: Replicache | undefined;
  return {
    name: `persist ${valSize}x${opts.numKeys} (${`indexes: ${
      opts.indexes || 0
    }`})`,
    group: 'replicache',
    byteSize: opts.numKeys * valSize,
    async teardownEach() {
      await closeAndCleanupRep(repToClose);
    },
    async run(bencher: Bencher) {
      const indexes: IndexDefinitions = createIndexDefinitions(
        opts.indexes ?? 0,
      );
      const rep = (repToClose = makeRepWithPopulate({indexes}));
      const randomValues = jsonArrayTestData(opts.numKeys, valSize);
      await rep.mutate.populate({numKeys: opts.numKeys, randomValues});
      bencher.reset();
      await rep.persist();
      bencher.stop();
    },
  };
}

export function benchmarkRefreshSimple(opts: {
  numKeys: number;
  indexes?: number;
}): Benchmark {
  const repName = makeRepName();
  let repToClose: Replicache;
  return {
    name: `refresh simple ${valSize}x${opts.numKeys} (${`indexes: ${
      opts.indexes ?? 0
    }`})`,
    group: 'replicache',
    byteSize: opts.numKeys * valSize * ((opts.indexes ?? 0) + 1),
    async teardownEach() {
      if (repToClose) {
        await closeAndCleanupRep(repToClose);
      }
    },
    async run(bencher: Bencher) {
      const indexes = createIndexDefinitions(opts.indexes ?? 0);
      const rep = (repToClose = new ReplicachePerfTest({
        name: repName,
        pullInterval: null,
        indexes,
      }));

      await setupPersistedData(repName, opts.numKeys, indexes);

      const initialScanResolver = resolver<void>();
      const cancel = rep.subscribe(async tx => (await tx.get('key0')) ?? {}, {
        onData: r => {
          if (r) {
            initialScanResolver.resolve();
          }
        },
      });
      await initialScanResolver.promise;
      cancel();
      bencher.reset();
      await rep.refresh();
      bencher.stop();
    },
  };
}

export function benchmarkRefresh(opts: {
  numKeysPersisted: number;
  numKeysPerMutation: number;
  numMutationsRefreshed: number;
  numMutationsRebased: number;
  indexes?: number;
}): Benchmark {
  assert(opts.numKeysPerMutation < opts.numKeysPersisted);
  const repName = makeRepName();
  const repsToClose: Replicache[] = [];
  return {
    name: `refresh, ${valSize}x${opts.numKeysPersisted} (${`indexes: ${
      opts.indexes || 0
    }`}) existing, refreshing ${
      opts.numMutationsRefreshed
    } mutations, rebasing ${
      opts.numMutationsRebased
    } mutations, with ${valSize}x${opts.numKeysPerMutation} per mutation`,
    group: 'replicache',
    async teardownEach() {
      for (const reps of repsToClose) {
        await closeAndCleanupRep(reps);
      }
    },
    async run(bencher: Bencher) {
      const indexes = createIndexDefinitions(opts.indexes ?? 0);
      await setupPersistedData(repName, 10000, indexes);
      const repA = new ReplicachePerfTest({
        name: repName,
        pullInterval: null,
        mutators: {putMap},
        indexes,
      });
      const repB = new ReplicachePerfTest({
        name: repName,
        pullInterval: null,
        mutators: {putMap},
        indexes,
      });

      repsToClose.push(repA);
      repsToClose.push(repB);

      async function putMapMutations(rep: typeof repA, num: number) {
        for (let i = 0; i < num; i++) {
          const entries = sampleSize(
            range(opts.numKeysPersisted),
            opts.numKeysPerMutation,
          ).map(i => [`key${i}`, jsonObjectTestData(valSize)]);
          await rep.mutate.putMap(Object.fromEntries(entries));
        }
      }

      await putMapMutations(repB, opts.numMutationsRefreshed);
      await repB.persist();

      await putMapMutations(repA, opts.numMutationsRebased);

      const initialScanResolver = resolver<void>();
      const cancel = repA.subscribe(
        async tx => {
          // eslint-disable-next-line @typescript-eslint/no-unused-vars
          for await (const _ of tx.scan({prefix: 'key'})) {
            return true;
          }
          return false;
        },
        {
          onData: r => {
            if (r) {
              initialScanResolver.resolve();
            }
          },
        },
      );
      await initialScanResolver.promise;
      cancel();

      bencher.reset();
      await repA.refresh();
      bencher.stop();
    },
  };
}

export function benchmarkRebase(opts: {
  mutations: number;
  targetSizePerMutation: number;
  numKeys?: number;
  targetSizePerKey?: number;
}): Benchmark {
  const repName = makeRepName();
  let repToClose: Replicache;
  const {
    mutations,
    targetSizePerMutation,
    numKeys = 1000,
    targetSizePerKey = 1024,
  } = opts;
  return {
    name: `rebase ${mutations}x${targetSizePerMutation}`,
    group: 'replicache',
    async teardownEach() {
      if (repToClose) {
        await closeAndCleanupRep(repToClose);
      }
    },
    async run(bencher: Bencher) {
      const rep = (repToClose = new ReplicachePerfTest({
        name: repName,
        pullInterval: null,
        pushDelay: 9999,
        mutators: {putMap},
        // eslint-disable-next-line require-await
        puller: async () => {
          return {
            response: {
              cookie: 1,
              lastMutationIDChanges: {},
              patch: [
                {
                  op: 'put',
                  key: 'pull-done',
                  value: true,
                },
              ],
            },
            httpRequestInfo: {
              httpStatusCode: 200,
              errorMessage: '',
            },
          };
        },
      }));

      // Create a bunch of keys.
      await rep.mutate.putMap(
        Object.fromEntries(
          Array.from({length: numKeys}).map((_, i) => [
            `key${i}`,
            jsonObjectTestData(targetSizePerKey),
          ]),
        ),
      );

      for (let i = 0; i < mutations; i++) {
        await rep.mutate.putMap({
          key: jsonObjectTestData(targetSizePerMutation),
        });
      }

      const {promise, resolve} = resolver<void>();
      let subscribeCallCount = 0;
      const cancel = rep.subscribe(tx => tx.get('pull-done'), {
        onData: r => {
          subscribeCallCount++;
          if (r) {
            resolve();
          }
        },
      });

      bencher.reset();

      // pull will rebase.
      rep.pull();
      await promise;

      bencher.stop();
      cancel();

      assert(
        subscribeCallCount === 2,
        'subscribe should have been called: ' + subscribeCallCount,
      );

      await rep.query(async tx => {
        assert(await tx.has('key'), 'key not found');
        for (let i = 0; i < numKeys; i++) {
          assert(await tx.has(`key${i}`), `key${i} not found`);
        }
        assert(await tx.has('pull-done'), 'pull-done not found');
      });
    },
  };
}

class ReplicachePerfTest<MD extends MutatorDefs> extends Replicache<MD> {
  private readonly _internalAPI: ReplicacheInternalAPI;
  constructor(options: Omit<ReplicacheOptions<MD>, 'licenseKey'>) {
    let internalAPI!: ReplicacheInternalAPI;
    super({
      ...options,
      licenseKey: TEST_LICENSE_KEY,
      exposeInternalAPI: (api: ReplicacheInternalAPI) => {
        internalAPI = api;
      },
      enableLicensing: false,
      enableMutationRecovery: false,
      enableScheduledRefresh: false,
      enableScheduledPersist: false,
    } as ReplicacheOptions<MD>);
    this._internalAPI = internalAPI;
  }

  // eslint-disable-next-line @typescript-eslint/no-empty-function
  onUpdateNeeded: ((reason: UpdateNeededReason) => void) | null = () => {};

  persist(): Promise<void> {
    return this._internalAPI.persist();
  }

  refresh(): Promise<void> {
    return this._internalAPI.refresh();
  }
}

async function setupPersistedData(
  replicacheName: string,
  numKeys: number,
  indexes: IndexDefinitions = {},
): Promise<void> {
  const randomValues = jsonArrayTestData(numKeys, valSize);
  const patch: PatchOperation[] = [];
  for (let i = 0; i < numKeys; i++) {
    patch.push({
      op: 'put',
      key: `key${i}`,
      value: randomValues[i],
    });
  }

  let repToClose;
  try {
    // populate store using pull (as opposed to mutators)
    // so that a snapshot commit is created, which new clients
    // can use to bootstrap.
    const rep = (repToClose = new ReplicachePerfTest({
      name: replicacheName,
      indexes,
      pullInterval: null,
      // eslint-disable-next-line require-await
      puller: async () => {
        return {
          response: {
            cookie: 1,
            lastMutationIDChanges: {},
            patch,
          },
          httpRequestInfo: {
            httpStatusCode: 200,
            errorMessage: '',
          },
        };
      },
    }));

    const initialPullResolver = resolver<void>();
    rep.subscribe(tx => tx.get('key0'), {
      onData: r => r && initialPullResolver.resolve(),
    });
    await initialPullResolver.promise;

    await rep.persist();
  } finally {
    await repToClose?.close();
  }
}

export function benchmarkStartupUsingBasicReadsFromPersistedData(opts: {
  numKeysPersisted: number;
  numKeysToRead: number;
}): Benchmark {
  const repName = makeRepName();
  let repToClose: Replicache | undefined;
  return {
    name: `startup read ${valSize}x${opts.numKeysToRead} from ${valSize}x${opts.numKeysPersisted} stored`,
    group: 'replicache',
    byteSize: opts.numKeysToRead * valSize,
    async setup() {
      await setupPersistedData(repName, opts.numKeysPersisted);
    },
    async teardownEach() {
      await repToClose?.close();
    },
    async teardown() {
      await closeAndCleanupRep(repToClose);
    },
    async run(bencher: Bencher) {
      const randomKeysToRead = sampleSize(
        range(opts.numKeysPersisted),
        opts.numKeysToRead,
      ).map(i => `key${i}`);
      bencher.reset();
      const rep = (repToClose = new ReplicachePerfTest({
        name: repName,
        pullInterval: null,
      }));
      let getCount = 0;
      await rep.query(async (tx: ReadTransaction) => {
        for (const randomKey of randomKeysToRead) {
          // use the values to be confident we're not optimizing away.
          getCount += Object.keys(
            (await tx.get(randomKey)) as TestDataObject,
          ).length;
        }
      });
      bencher.stop();
      console.log(getCount);
    },
  };
}

export function benchmarkStartupUsingScanFromPersistedData(opts: {
  numKeysPersisted: number;
  numKeysToRead: number;
}): Benchmark {
  const repName = makeRepName();
  let repToClose: Replicache | undefined;
  return {
    name: `startup scan ${valSize}x${opts.numKeysToRead} from ${valSize}x${opts.numKeysPersisted} stored`,
    group: 'replicache',
    byteSize: opts.numKeysToRead * valSize,
    async setup() {
      await setupPersistedData(repName, opts.numKeysPersisted);
    },
    async teardownEach() {
      await repToClose?.close();
    },
    async teardown() {
      await closeAndCleanupRep(repToClose);
    },
    async run(bencher: Bencher) {
      const randomIndex = Math.floor(
        Math.random() * (opts.numKeysPersisted - opts.numKeysToRead),
      );
      const keys = Array.from(
        {length: opts.numKeysPersisted - opts.numKeysToRead},
        (_, index) => `key${index}`,
      );
      const sortedKeys = keys.sort();
      const randomStartKey = sortedKeys[randomIndex];
      bencher.reset();
      const rep = (repToClose = new ReplicachePerfTest({
        name: repName,
        pullInterval: null,
      }));
      await rep.query(async (tx: ReadTransaction) => {
        let count = 0;
        for await (const value of tx.scan({
          start: {key: randomStartKey},
          limit: 100,
        })) {
          // use the value to be confident we're not optimizing away.
          count += Object.keys(value as TestDataObject).length;
        }
        console.log(count);
      });
      bencher.stop();
    },
  };
}

export function benchmarkReadTransaction(opts: {
  numKeys: number;
  useMemstore: boolean;
}): Benchmark {
  let rep: ReplicacheWithPopulate;
  return {
    name: `${opts.useMemstore ? '[MemStore] ' : ''}read tx ${valSize}x${
      opts.numKeys
    }`,
    group: 'replicache',
    byteSize: opts.numKeys * valSize,
    async setup() {
      rep = makeRepWithPopulate();
      await rep.mutate.populate({
        numKeys: opts.numKeys,
        randomValues: jsonArrayTestData(opts.numKeys, valSize),
      });
    },
    async teardown() {
      await closeAndCleanupRep(rep);
    },
    async run(bench: Bencher) {
      let getCount = 0;
      let hasCount = 0;
      await rep.query(async (tx: ReadTransaction) => {
        for (let i = 0; i < opts.numKeys; i++) {
          // use the values to be confident we're not optimizing away.
          getCount += Object.keys(
            (await tx.get(`keys${i}`)) as TestDataObject,
          ).length;
          hasCount += (await tx.has(`key${i}`)) === true ? 1 : 0;
        }
      });
      bench.stop();
      console.log(getCount, hasCount);
    },
  };
}

export function benchmarkScan(opts: {numKeys: number}): Benchmark {
  let rep: ReplicacheWithPopulate;
  return {
    name: `scan ${valSize}x${opts.numKeys}`,
    group: 'replicache',
    byteSize: opts.numKeys * valSize,

    async setup() {
      rep = makeRepWithPopulate();
      await rep.mutate.populate({
        numKeys: opts.numKeys,
        randomValues: jsonArrayTestData(opts.numKeys, valSize),
      });
    },
    async teardown() {
      await closeAndCleanupRep(rep);
    },
    async run() {
      await rep.query(async (tx: ReadTransaction) => {
        let count = 0;
        for await (const value of tx.scan()) {
          // use the value to be confident we're not optimizing away.
          count += (value as ArrayLike<unknown>).length;
        }
        console.log(count);
      });
    },
  };
}

export function benchmarkCreateIndex(opts: {numKeys: number}): Benchmark {
  // Creation of indexes is part of creating a Replicache instance. Creating an
  // index on an empty Replicache instance is not very interesting. We therefore
  // populate the instance with persisted data. We then create a Replicache
  // instance without an index and one with an index. We then subtract the time
  // it took to create the instance without the index.
  let rep: Replicache | undefined;
  const repName = makeRepName();
  return {
    name: `create index with definition ${valSize}x${opts.numKeys}`,
    group: 'replicache',
    async setupEach() {
      await setupPersistedData(repName, opts.numKeys);
    },
    async teardownEach() {
      await closeAndCleanupRep(rep);
    },
    async run(bencher: Bencher) {
      const t0 = performance.now();
      const repNoIndex = makeRep({
        name: repName,
      });
      // Wait for opening being done.
      await repNoIndex.query(() => undefined);
      const t1 = performance.now();
      await repNoIndex.close();

      bencher.reset();
      rep = makeRep({
        name: repName,
        indexes: {
          idx: {jsonPointer: '/ascii'},
        },
      });
      // Wait for opening being done.
      await rep.query(() => undefined);

      bencher.stop();
      bencher.subtract(t1 - t0);
    },
  };
}

export function sleep(ms: number): Promise<void> {
  return new Promise<void>(resolve => {
    setTimeout(() => resolve(), ms);
  });
}

// goal: 95% of writes/sub/read cycle complete in <1ms with 100 active subscriptions, 5 of which are dirty, which each read 10KB > each, while there is 100MB of data in Replicache.
export function benchmarkWriteSubRead(opts: {
  valueSize: number;
  numSubsTotal: number;
  keysPerSub: number;
  keysWatchedPerSub: number;
  numSubsDirty: number;
}): Benchmark {
  const {valueSize, numSubsTotal, keysPerSub, keysWatchedPerSub, numSubsDirty} =
    opts;

  const numKeys = keysPerSub * numSubsTotal;
  const cacheSizeMB = (numKeys * valueSize) / 1024 / 1024;
  const kbReadPerSub = (keysWatchedPerSub * valueSize) / 1024;
  const makeKey = (index: number) => `key${index}`;

  let repToClose: Replicache | undefined;
  return {
    name: `writeSubRead ${cacheSizeMB}MB total, ${numSubsTotal} subs total, ${numSubsDirty} subs dirty, ${kbReadPerSub}kb read per sub`,
    group: 'replicache',
    async teardownEach() {
      await closeAndCleanupRep(repToClose);
    },
    async run(bencher: Bencher) {
      const keys = Array.from({length: numKeys}, (_, index) => makeKey(index));
      const sortedKeys = keys.sort();
      const initData: Readonly<Record<string, TestDataObject>> =
        Object.fromEntries(
          keys.map(key => [key, jsonObjectTestData(valueSize)]),
        );
      const dataFromSubscribe: Record<string, TestDataObject> = {};

      const rep = (repToClose = makeRep({
        mutators: {
          putMap,
        },
      }));

      await rep.mutate.putMap(initData);
      let onDataCallCount = 0;

      const subs = Array.from({length: numSubsTotal}, (_, i) => {
        const startKeyIndex = i * keysPerSub;
        return rep.subscribe(
          async tx => {
            const startKey = sortedKeys[startKeyIndex];
            return await tx
              .scan({
                start: {key: startKey},
                limit: keysWatchedPerSub,
              })
              .toArray();
          },
          {
            onData(v) {
              onDataCallCount++;
              const vals = v as TestDataObject[];
              for (const [j, val] of vals.entries()) {
                dataFromSubscribe[sortedKeys[startKeyIndex + j]] = val;
              }
            },
          },
        );
      });

      // We need to wait until all the initial async onData have been called.
      while (onDataCallCount !== numSubsTotal) {
        await sleep(10);
      }

      // Build our random changes ahead of time, outside the timed window.
      // invalidate numSubsDirty different subscriptions by writing to the first key each is scanning.
      const changes = Object.fromEntries(
        sampleSize(range(numSubsTotal), numSubsDirty).map(v => [
          sortedKeys[v * keysPerSub],
          jsonObjectTestData(valueSize),
        ]),
      );

      // OK time the below!
      bencher.reset();

      // In a single transaction, invalidate numSubsDirty subscriptions.
      await rep.mutate.putMap(changes);

      bencher.stop();

      subs.forEach(c => c());

      assert(onDataCallCount === numSubsTotal + numSubsDirty);
      for (const [changeKey, changeValue] of Object.entries(changes)) {
        assert(deepEqual(changeValue, dataFromSubscribe[changeKey]));
      }
    },
  };
}

// This benchmark is based on a reduced test case from Tom McWright
function benchmarkTmcw(kind: 'populate' | 'persist'): Benchmark {
  let repToClose: Replicache | undefined;
  let updates: JSONValue[] | undefined;

  return {
    name: `${kind} tmcw`,
    group: 'replicache',
    async setup() {
      updates = (await getTmcwData()).features;
    },
    async teardownEach() {
      await closeAndCleanupRep(repToClose);
    },
    async run(bencher: Bencher) {
      const rep = (repToClose = makeRep({
        mutators: {
          async putFeatures(tx: WriteTransaction, updates: Array<JSONValue>) {
            for (let i = 0; i < updates.length; i++) {
              await tx.put(String(i), updates[i]);
            }
          },
        },
      }));

      assert(updates);

      // Wait for init.
      await rep.clientID;

      if (kind === 'populate') {
        bencher.reset();
        await rep.mutate.putFeatures(updates);
      } else {
        await rep.mutate.putFeatures(updates);
        bencher.reset();
        await rep.persist();
      }
      bencher.stop();
    },
  };
}

function makeRepName(): string {
  return `bench${uuid()}`;
}

function makeRep<MD extends MutatorDefs>(
  options: Omit<ReplicacheOptions<MD>, 'name' | 'licenseKey'> & {
    name?: string;
  } = {},
) {
  const name = makeRepName();
  return new ReplicachePerfTest<MD>({
    name,
    pullInterval: null,
    ...options,
  });
}

type PopulateMutatorDefs = {
  populate: typeof populate;
};

type ReplicacheWithPopulate = ReplicachePerfTest<PopulateMutatorDefs>;

async function populate(
  tx: WriteTransaction,
  {
    numKeys,
    randomValues: randomValues,
  }: {numKeys: number; randomValues: TestDataObject[]},
) {
  for (let i = 0; i < numKeys; i++) {
    await tx.put(`key${i}`, randomValues[i]);
  }
}

async function putMap(
  tx: WriteTransaction,
  map: Record<string, TestDataObject>,
) {
  for (const [key, value] of Object.entries(map)) {
    await tx.put(key, value);
  }
}

function makeRepWithPopulate<MD extends PopulateMutatorDefs>(
  options: Partial<ReplicacheOptions<MD>> = {},
) {
  return makeRep({
    ...options,
    mutators: {...(options.mutators ?? {}), populate},
  });
}

function createIndexDefinitions(numIndexes: number): IndexDefinitions {
  const indexes: Writable<IndexDefinitions> = {};
  for (let i = 0; i < numIndexes; i++) {
    indexes[`idx${i}`] = {
      jsonPointer: '/ascii',
    };
  }
  return indexes;
}

async function closeAndCleanupRep(rep: Replicache | undefined): Promise<void> {
  if (rep) {
    await rep.close();
    await dropIDBStore(rep.idbName);
  }
}

export function benchmarks(): Benchmark[] {
  return [
    // write/sub/read 1mb
    benchmarkWriteSubRead({
      valueSize: 1024,
      numSubsTotal: 64,
      keysPerSub: 16,
      keysWatchedPerSub: 16,
      numSubsDirty: 5,
    }),
    // write/sub/read 4mb
    benchmarkWriteSubRead({
      valueSize: 1024,
      numSubsTotal: 128,
      keysPerSub: 32,
      keysWatchedPerSub: 16,
      numSubsDirty: 5,
    }),
    // write/sub/read 16mb
    benchmarkWriteSubRead({
      valueSize: 1024,
      numSubsTotal: 128,
      keysPerSub: 128,
      keysWatchedPerSub: 16,
      numSubsDirty: 5,
    }),
    // write/sub/read 64mb
    benchmarkWriteSubRead({
      valueSize: 1024,
      numSubsTotal: 128,
      keysPerSub: 512,
      keysWatchedPerSub: 16,
      numSubsDirty: 5,
    }),
    // 128 mb is unusable
    benchmarkPopulate({numKeys: 1000, clean: true}),
    benchmarkPopulate({numKeys: 1000, clean: true, indexes: 1}),
    benchmarkPopulate({numKeys: 1000, clean: true, indexes: 2}),
    benchmarkPopulate({numKeys: 10000, clean: true}),
    benchmarkPopulate({numKeys: 10000, clean: true, indexes: 1}),
    benchmarkPopulate({numKeys: 10000, clean: true, indexes: 2}),
    benchmarkScan({numKeys: 1000}),
    benchmarkScan({numKeys: 10_000}),
    benchmarkCreateIndex({numKeys: 5000}),
    benchmarkStartupUsingBasicReadsFromPersistedData({
      numKeysPersisted: 100000,
      numKeysToRead: 100,
    }),
    benchmarkStartupUsingScanFromPersistedData({
      numKeysPersisted: 100000,
      numKeysToRead: 100,
    }),

    benchmarkPersist({numKeys: 1000}),
    benchmarkPersist({numKeys: 1000, indexes: 1}),
    benchmarkPersist({numKeys: 1000, indexes: 2}),
    benchmarkPersist({numKeys: 10000}),
    benchmarkPersist({numKeys: 10000, indexes: 1}),
    benchmarkPersist({numKeys: 10000, indexes: 2}),

    benchmarkRefreshSimple({numKeys: 1000}),
    benchmarkRefreshSimple({numKeys: 1000, indexes: 1}),
    benchmarkRefreshSimple({numKeys: 1000, indexes: 2}),
    benchmarkRefreshSimple({numKeys: 10000}),
    benchmarkRefreshSimple({numKeys: 10000, indexes: 1}),
    benchmarkRefreshSimple({numKeys: 10000, indexes: 2}),

    benchmarkRefresh({
      numKeysPersisted: 1000,
      numKeysPerMutation: 10,
      numMutationsRefreshed: 10,
      numMutationsRebased: 10,
    }),
    benchmarkRefresh({
      numKeysPersisted: 1000,
      numKeysPerMutation: 10,
      numMutationsRefreshed: 10,
      numMutationsRebased: 10,
      indexes: 1,
    }),
    benchmarkRefresh({
      numKeysPersisted: 1000,
      numKeysPerMutation: 10,
      numMutationsRefreshed: 100,
      numMutationsRebased: 100,
    }),
    benchmarkRefresh({
      numKeysPersisted: 1000,
      numKeysPerMutation: 10,
      numMutationsRefreshed: 100,
      numMutationsRebased: 100,
      indexes: 1,
    }),

    benchmarkTmcw('populate'),
    benchmarkTmcw('persist'),

    benchmarkRebase({
      mutations: 1000,
      targetSizePerMutation: 1024,
    }),
  ];
}

function* rangeIter(end: number) {
  for (let i = 0; i < end; i++) {
    yield i;
  }
}

function range(end: number): number[] {
  return [...rangeIter(end)];
}

function sampleSize<T>(arr: Iterable<T>, n: number): T[] {
  return shuffle(arr).slice(0, n);
}

function shuffle<T>(arr: Iterable<T>): T[] {
  const shuffled = [...arr];
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }
  return shuffled;
}
