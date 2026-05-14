// oxlint-disable no-console
import {resolver} from '@rocicorp/resolver';
import {afterAll, beforeAll} from 'vitest';
import {assert} from '../../shared/src/asserts.ts';
import {bench, describe} from '../../shared/src/bench.ts';
import {deepEqual} from '../../shared/src/json.ts';
import type {JSONValue} from '../../shared/src/json.ts';
import {randomUint64} from '../../shared/src/random-uint64.ts';
import {
  jsonArrayTestData,
  jsonObjectTestData,
  type TestDataObject,
} from '../../shared/src/test-data.ts';
import type {Writable} from '../../shared/src/writable.ts';
import {ReplicacheImpl} from './impl.ts';
import type {IndexDefinitions} from './index-defs.ts';
import {dropIDBStoreWithMemFallback} from './kv/idb-store-with-mem-fallback.ts';
import type {PatchOperation} from './patch-operation.ts';
import type {ReplicacheOptions} from './replicache-options.ts';
import tmcwData from './resources/tmcw.json';
import type {ReadTransaction, WriteTransaction} from './transactions.ts';
import type {MutatorDefs} from './types.ts';

const valSize = 1024;

class ReplicachePerfTest<MD extends MutatorDefs> extends ReplicacheImpl<MD> {
  constructor(options: Omit<ReplicacheOptions<MD>, 'licenseKey'>) {
    super(
      {...options},
      {
        enableMutationRecovery: false,
        enableScheduledRefresh: false,
        enableScheduledPersist: false,
      },
    );
  }
}

function makeRepName(): string {
  return `bench${randomUint64()}`;
}

function makeRep<MD extends MutatorDefs>(
  options: Partial<Omit<ReplicacheOptions<MD>, 'licenseKey'>> & {
    name?: string | undefined;
  } = {},
): ReplicachePerfTest<MD> {
  return new ReplicachePerfTest<MD>({
    pullInterval: null,
    ...options,
    name: options.name ?? makeRepName(),
  } as Omit<ReplicacheOptions<MD>, 'licenseKey'>);
}

type PopulateMutatorDefs = {populate: typeof populate};
type ReplicacheWithPopulate = ReplicachePerfTest<PopulateMutatorDefs>;

async function populate(
  tx: WriteTransaction,
  {numKeys, randomValues}: {numKeys: number; randomValues: TestDataObject[]},
) {
  for (let i = 0; i < numKeys; i++) {
    await tx.set(`key${i}`, randomValues[i]);
  }
}

async function putMap(
  tx: WriteTransaction,
  map: Record<string, TestDataObject>,
) {
  for (const [key, value] of Object.entries(map)) {
    await tx.set(key, value);
  }
}

function makeRepWithPopulate(
  options: Omit<
    Partial<ReplicacheOptions<PopulateMutatorDefs>>,
    'mutators' | 'name'
  > = {},
): ReplicacheWithPopulate {
  return new ReplicachePerfTest<PopulateMutatorDefs>({
    name: makeRepName(),
    pullInterval: null,
    ...options,
    mutators: {populate},
  });
}

function createIndexDefinitions(numIndexes: number): IndexDefinitions {
  const indexes: Writable<IndexDefinitions> = {};
  for (let i = 0; i < numIndexes; i++) {
    indexes[`idx${i}`] = {jsonPointer: '/ascii'};
  }
  return indexes;
}

async function closeAndCleanupRep(
  rep: ReplicacheImpl | undefined,
): Promise<void> {
  if (rep) {
    await rep.close();
    await dropIDBStoreWithMemFallback(rep.idbName);
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
    patch.push({op: 'put', key: `key${i}`, value: randomValues[i]});
  }

  let repToClose: ReplicacheImpl | undefined;
  try {
    const rep = (repToClose = new ReplicachePerfTest({
      name: replicacheName,
      indexes,
      pullInterval: null,
      // oxlint-disable-next-line require-await
      puller: async () => ({
        response: {
          cookie: 1,
          lastMutationIDChanges: {},
          patch,
        },
        httpRequestInfo: {
          httpStatusCode: 200,
          errorMessage: '',
        },
      }),
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

function sleep(ms: number): Promise<void> {
  return new Promise<void>(resolve => setTimeout(resolve, ms));
}

function* rangeIter(end: number) {
  for (let i = 0; i < end; i++) yield i;
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

// Slow bench options: fewer samples for benchmarks that can take several seconds
// per iteration (e.g. persist/refresh with IDB and large datasets).
const slowBenchOpts = {min_samples: 5} as const;

describe('replicache', () => {
  // -------------------------------------------------------------------------
  // writeSubRead
  // -------------------------------------------------------------------------

  function writeSubReadName(opts: {
    valueSize: number;
    numSubsTotal: number;
    keysPerSub: number;
    keysWatchedPerSub: number;
    numSubsDirty: number;
  }): string {
    const {
      valueSize,
      numSubsTotal,
      keysPerSub,
      keysWatchedPerSub,
      numSubsDirty,
    } = opts;
    const numKeys = keysPerSub * numSubsTotal;
    const cacheSizeMB = (numKeys * valueSize) / 1024 / 1024;
    const kbReadPerSub = (keysWatchedPerSub * valueSize) / 1024;
    return `writeSubRead ${cacheSizeMB}MB total, ${numSubsTotal} subs total, ${numSubsDirty} subs dirty, ${kbReadPerSub}kb read per sub`;
  }

  function makeWriteSubReadBench(opts: {
    valueSize: number;
    numSubsTotal: number;
    keysPerSub: number;
    keysWatchedPerSub: number;
    numSubsDirty: number;
  }) {
    const {
      valueSize,
      numSubsTotal,
      keysPerSub,
      keysWatchedPerSub,
      numSubsDirty,
    } = opts;
    const numKeys = keysPerSub * numSubsTotal;
    const makeKey = (index: number) => `key${index}`;

    bench(
      writeSubReadName(opts),
      async function* () {
        const keys = Array.from({length: numKeys}, (_, index) =>
          makeKey(index),
        );
        const sortedKeys = keys.sort();
        const initData: Readonly<Record<string, TestDataObject>> =
          Object.fromEntries(
            keys.map(key => [key, jsonObjectTestData(valueSize)]),
          );
        const dataFromSubscribe: Record<string, TestDataObject> = {};

        const rep = makeRep<{putMap: typeof putMap}>({
          mutators: {putMap},
        });

        await rep.mutate.putMap(initData);
        let onDataCallCount = 0;

        const subs = Array.from({length: numSubsTotal}, (_, i) => {
          const startKeyIndex = i * keysPerSub;
          return rep.subscribe(
            tx => {
              const startKey = sortedKeys[startKeyIndex];
              return tx
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

        while (onDataCallCount !== numSubsTotal) {
          await sleep(10);
        }

        const changes = Object.fromEntries(
          sampleSize(range(numSubsTotal), numSubsDirty).map(v => [
            sortedKeys[v * keysPerSub],
            jsonObjectTestData(valueSize),
          ]),
        );

        try {
          yield async () => {
            await rep.mutate.putMap(changes);
          };
        } finally {
          subs.forEach(c => c());
          await closeAndCleanupRep(rep);
        }

        assert(
          onDataCallCount === numSubsTotal + numSubsDirty,
          () =>
            `Expected onDataCallCount (${onDataCallCount}) to equal numSubsTotal + numSubsDirty (${numSubsTotal + numSubsDirty})`,
        );
        for (const [changeKey, changeValue] of Object.entries(changes)) {
          assert(
            deepEqual(changeValue, dataFromSubscribe[changeKey]),
            () =>
              `Expected subscription data for key '${changeKey}' to match the change value`,
          );
        }
      },
      slowBenchOpts,
    );
  }

  // write/sub/read 1mb
  makeWriteSubReadBench({
    valueSize: 1024,
    numSubsTotal: 64,
    keysPerSub: 16,
    keysWatchedPerSub: 16,
    numSubsDirty: 5,
  });
  // write/sub/read 4mb
  makeWriteSubReadBench({
    valueSize: 1024,
    numSubsTotal: 128,
    keysPerSub: 32,
    keysWatchedPerSub: 16,
    numSubsDirty: 5,
  });
  // write/sub/read 16mb
  makeWriteSubReadBench({
    valueSize: 1024,
    numSubsTotal: 128,
    keysPerSub: 128,
    keysWatchedPerSub: 16,
    numSubsDirty: 5,
  });
  // write/sub/read 64mb
  makeWriteSubReadBench({
    valueSize: 1024,
    numSubsTotal: 128,
    keysPerSub: 512,
    keysWatchedPerSub: 16,
    numSubsDirty: 5,
  });

  // -------------------------------------------------------------------------
  // populate
  // -------------------------------------------------------------------------

  for (const numKeys of [1000, 10000]) {
    for (const numIndexes of [0, 1, 2]) {
      bench(
        `populate ${valSize}x${numKeys} (clean, indexes: ${numIndexes})`,
        async function* () {
          const indexes = createIndexDefinitions(numIndexes);
          const rep = makeRepWithPopulate({indexes});
          await rep.clientGroupID;
          const randomValues = jsonArrayTestData(numKeys, valSize);

          try {
            yield async () => {
              await rep.mutate.populate({numKeys, randomValues});
            };
          } finally {
            await closeAndCleanupRep(rep);
          }
        },
        slowBenchOpts,
      );
    }
  }

  // -------------------------------------------------------------------------
  // scan
  // -------------------------------------------------------------------------

  for (const numKeys of [1000, 10_000]) {
    describe(`scan ${valSize}x${numKeys}`, () => {
      let rep: ReplicacheWithPopulate;

      beforeAll(async () => {
        rep = makeRepWithPopulate();
        await rep.mutate.populate({
          numKeys,
          randomValues: jsonArrayTestData(numKeys, valSize),
        });
      });

      afterAll(async () => {
        await closeAndCleanupRep(rep);
      });

      bench(`scan ${valSize}x${numKeys}`, async () => {
        await rep.query(async (tx: ReadTransaction) => {
          let count = 0;
          for await (const value of tx.scan()) {
            count += (value as ArrayLike<unknown>).length;
          }
          console.log(count);
        });
      });
    });
  }

  // -------------------------------------------------------------------------
  // createIndex
  // -------------------------------------------------------------------------

  bench(
    `create index with definition ${valSize}x5000`,
    async function* () {
      const repName = makeRepName();
      await setupPersistedData(repName, 5000);

      const openedReps: ReplicacheImpl[] = [];
      try {
        yield async () => {
          const rep = makeRep({
            name: repName,
            indexes: {idx: {jsonPointer: '/ascii'}},
          });
          openedReps.push(rep);
          await rep.query(() => undefined);
        };
      } finally {
        await Promise.all(openedReps.map(r => r.close()));
        if (openedReps.length > 0) {
          await dropIDBStoreWithMemFallback(openedReps[0].idbName);
        }
      }
    },
    slowBenchOpts,
  );

  // -------------------------------------------------------------------------
  // startup
  // -------------------------------------------------------------------------

  describe(`startup read ${valSize}x100 from ${valSize}x100000 stored`, () => {
    const repName = makeRepName();
    let lastRep: ReplicachePerfTest<Record<never, never>> | undefined;

    beforeAll(async () => {
      await setupPersistedData(repName, 100000);
    });

    afterAll(async () => {
      await closeAndCleanupRep(lastRep);
    });

    bench(
      `startup read ${valSize}x100 from ${valSize}x100000 stored`,
      async function* () {
        const randomKeysToRead = sampleSize(range(100000), 100).map(
          i => `key${i}`,
        );
        const openedReps: ReplicachePerfTest<Record<never, never>>[] = [];

        yield async () => {
          const rep = new ReplicachePerfTest({
            name: repName,
            pullInterval: null,
          });
          openedReps.push(rep);
          lastRep = rep;
          let getCount = 0;
          await rep.query(async (tx: ReadTransaction) => {
            for (const randomKey of randomKeysToRead) {
              getCount += Object.keys(
                (await tx.get(randomKey)) as TestDataObject,
              ).length;
            }
          });
          console.log(getCount);
        };

        // Close all reps opened during measurement to stop heartbeat timers
        await Promise.all(openedReps.map(r => r.close()));
      },
      slowBenchOpts,
    );
  });

  describe(`startup scan ${valSize}x100 from ${valSize}x100000 stored`, () => {
    const repName = makeRepName();
    let lastRep: ReplicachePerfTest<Record<never, never>> | undefined;

    beforeAll(async () => {
      await setupPersistedData(repName, 100000);
    });

    afterAll(async () => {
      await closeAndCleanupRep(lastRep);
    });

    bench(
      `startup scan ${valSize}x100 from ${valSize}x100000 stored`,
      async function* () {
        const keys = Array.from(
          {length: 100000 - 100},
          (_, index) => `key${index}`,
        );
        const sortedKeys = keys.sort();
        const randomIndex = Math.floor(Math.random() * sortedKeys.length);
        const randomStartKey = sortedKeys[randomIndex];

        const openedReps: ReplicachePerfTest<Record<never, never>>[] = [];

        yield async () => {
          const rep = new ReplicachePerfTest({
            name: repName,
            pullInterval: null,
          });
          openedReps.push(rep);
          lastRep = rep;
          await rep.query(async (tx: ReadTransaction) => {
            let count = 0;
            for await (const value of tx.scan({
              start: {key: randomStartKey},
              limit: 100,
            })) {
              count += Object.keys(value as TestDataObject).length;
            }
            console.log(count);
          });
        };

        // Close all reps opened during measurement to stop heartbeat timers
        await Promise.all(openedReps.map(r => r.close()));
      },
      slowBenchOpts,
    );
  });

  // -------------------------------------------------------------------------
  // persist
  // -------------------------------------------------------------------------

  for (const numKeys of [1000, 10000]) {
    for (const numIndexes of [0, 1, 2]) {
      bench(
        `persist ${valSize}x${numKeys} (indexes: ${numIndexes})`,
        async function* () {
          const indexes = createIndexDefinitions(numIndexes);
          const rep = makeRepWithPopulate({indexes});
          const randomValues = jsonArrayTestData(numKeys, valSize);
          await rep.mutate.populate({numKeys, randomValues});

          try {
            yield async () => {
              await rep.persist();
            };
          } finally {
            await closeAndCleanupRep(rep);
          }
        },
        slowBenchOpts,
      );
    }
  }

  // -------------------------------------------------------------------------
  // refreshSimple
  // -------------------------------------------------------------------------

  for (const numKeys of [1000, 10000]) {
    for (const numIndexes of [0, 1, 2]) {
      const repName = makeRepName();
      bench(
        `refresh simple ${valSize}x${numKeys} (indexes: ${numIndexes})`,
        async function* () {
          const indexes = createIndexDefinitions(numIndexes);
          const rep = new ReplicachePerfTest({
            name: repName,
            pullInterval: null,
            indexes,
          });

          await setupPersistedData(repName, numKeys, indexes);

          const initialScanResolver = resolver<void>();
          const cancel = rep.subscribe(
            async tx => (await tx.get('key0')) ?? {},
            {
              onData: r => {
                if (r) initialScanResolver.resolve();
              },
            },
          );
          await initialScanResolver.promise;
          cancel();

          try {
            yield async () => {
              await rep.refresh();
            };
          } finally {
            await closeAndCleanupRep(rep);
          }
        },
        slowBenchOpts,
      );
    }
  }

  // -------------------------------------------------------------------------
  // refresh
  // -------------------------------------------------------------------------

  function makeRefreshBench(opts: {
    numKeysPersisted: number;
    numKeysPerMutation: number;
    numMutationsRefreshed: number;
    numMutationsRebased: number;
    indexes?: number | undefined;
  }) {
    const {
      numKeysPersisted,
      numKeysPerMutation,
      numMutationsRefreshed,
      numMutationsRebased,
      indexes: numIndexes = 0,
    } = opts;

    assert(
      numKeysPerMutation < numKeysPersisted,
      'Expected numKeysPerMutation to be less than numKeysPersisted',
    );

    const name = `refresh, ${valSize}x${numKeysPersisted} (indexes: ${numIndexes}) existing, refreshing ${numMutationsRefreshed} mutations, rebasing ${numMutationsRebased} mutations, with ${valSize}x${numKeysPerMutation} per mutation`;

    bench(
      name,
      async function* () {
        const repName = makeRepName();
        const indexes = createIndexDefinitions(numIndexes);

        await setupPersistedData(repName, numKeysPersisted, indexes);

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

        async function putMapMutations(
          rep: typeof repA,
          num: number,
        ): Promise<void> {
          for (let i = 0; i < num; i++) {
            const entries = sampleSize(
              range(numKeysPersisted),
              numKeysPerMutation,
            ).map(i => [`key${i}`, jsonObjectTestData(valSize)]);
            await rep.mutate.putMap(Object.fromEntries(entries));
          }
        }

        await putMapMutations(repB, numMutationsRefreshed);
        await repB.persist();
        await putMapMutations(repA, numMutationsRebased);

        const initialScanResolver = resolver<void>();
        const cancel = repA.subscribe(
          async tx => {
            for await (const _ of tx.scan({prefix: 'key'})) {
              return true;
            }
            return false;
          },
          {
            onData: r => {
              if (r) initialScanResolver.resolve();
            },
          },
        );
        await initialScanResolver.promise;
        cancel();

        try {
          yield async () => {
            await repA.refresh();
          };
        } finally {
          await closeAndCleanupRep(repA);
          await closeAndCleanupRep(repB);
        }
      },
      slowBenchOpts,
    );
  }

  makeRefreshBench({
    numKeysPersisted: 1000,
    numKeysPerMutation: 10,
    numMutationsRefreshed: 10,
    numMutationsRebased: 10,
  });
  makeRefreshBench({
    numKeysPersisted: 1000,
    numKeysPerMutation: 10,
    numMutationsRefreshed: 10,
    numMutationsRebased: 10,
    indexes: 1,
  });
  makeRefreshBench({
    numKeysPersisted: 1000,
    numKeysPerMutation: 10,
    numMutationsRefreshed: 100,
    numMutationsRebased: 100,
  });
  makeRefreshBench({
    numKeysPersisted: 1000,
    numKeysPerMutation: 10,
    numMutationsRefreshed: 100,
    numMutationsRebased: 100,
    indexes: 1,
  });

  // -------------------------------------------------------------------------
  // tmcw
  // -------------------------------------------------------------------------

  describe('tmcw', () => {
    const updates: JSONValue[] = tmcwData.features as JSONValue[];

    bench(
      'populate tmcw',
      async function* () {
        const rep = makeRep<{
          putFeatures: (
            tx: WriteTransaction,
            updates: JSONValue[],
          ) => Promise<void>;
        }>({
          mutators: {
            async putFeatures(tx: WriteTransaction, updates: JSONValue[]) {
              for (let i = 0; i < updates.length; i++) {
                await tx.set(String(i), updates[i]);
              }
            },
          },
        });

        await rep.clientGroupID;

        try {
          yield async () => {
            await rep.mutate.putFeatures(updates);
          };
        } finally {
          await closeAndCleanupRep(rep);
        }
      },
      slowBenchOpts,
    );

    bench(
      'persist tmcw',
      async function* () {
        const rep = makeRep<{
          putFeatures: (
            tx: WriteTransaction,
            updates: JSONValue[],
          ) => Promise<void>;
        }>({
          mutators: {
            async putFeatures(tx: WriteTransaction, updates: JSONValue[]) {
              for (let i = 0; i < updates.length; i++) {
                await tx.set(String(i), updates[i]);
              }
            },
          },
        });

        await rep.clientGroupID;
        await rep.mutate.putFeatures(updates);

        try {
          yield async () => {
            await rep.persist();
          };
        } finally {
          await closeAndCleanupRep(rep);
        }
      },
      slowBenchOpts,
    );
  });

  // -------------------------------------------------------------------------
  // rebase
  // -------------------------------------------------------------------------

  // Rebase benchmark: measures pull/rebase of 1000 pending mutations.
  // Pre-creates one rep per call (1 warmup + min_samples measurements) so each
  // call gets a fresh rep with unconsumed mutations. Mitata always makes exactly
  // 1 warmup call when the measured function takes > 500µs (which rebase does).
  bench(
    'rebase 1000x1024',
    async function* () {
      const mutations = 1000;
      const targetSizePerMutation = 1024;
      const numKeys = 1000;
      const targetSizePerKey = 1024;

      async function createRebaseRep(): Promise<
        ReplicachePerfTest<{putMap: typeof putMap}>
      > {
        const repName = makeRepName();
        const rep = new ReplicachePerfTest({
          name: repName,
          pullInterval: null,
          pushDelay: 9999,
          mutators: {putMap},
          // oxlint-disable-next-line require-await
          puller: async () => ({
            response: {
              cookie: 1,
              lastMutationIDChanges: {},
              patch: [{op: 'put' as const, key: 'pull-done', value: true}],
            },
            httpRequestInfo: {
              httpStatusCode: 200,
              errorMessage: '',
            },
          }),
        });
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
        return rep;
      }

      // 1 warmup call + min_samples (1) measurement call = 2 total calls.
      const [warmupRep, ...measureReps] = await Promise.all([
        createRebaseRep(),
        createRebaseRep(),
      ]);
      const allReps = [warmupRep, ...measureReps];
      let idx = 0;

      yield async () => {
        const rep = allReps[idx++];
        const {promise, resolve} = resolver<void>();
        const cancel = rep.subscribe(tx => tx.get('pull-done'), {
          onData: r => {
            if (r) resolve();
          },
        });
        // Suppress "Closed" rejection when rep.close() aborts the in-flight pull.
        rep.pull().catch(() => {});
        await promise;
        cancel();
      };

      // Cleanup after all calls so IDB deletion is not included in timing.
      for (const rep of allReps) {
        await closeAndCleanupRep(rep);
      }
    },
    {min_samples: 1, min_cpu_time: 0},
  );
});
