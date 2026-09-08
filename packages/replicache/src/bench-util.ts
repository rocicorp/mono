import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {randomUint64} from '../../shared/src/random-uint64.ts';
import {
  jsonArrayTestData,
  type TestDataObject,
} from '../../shared/src/test-data.ts';
import type {Writable} from '../../shared/src/writable.ts';
import {getKVStoreProvider} from './get-kv-store-provider.ts';
import {ReplicacheImpl} from './impl.ts';
import type {IndexDefinitions} from './index-defs.ts';
import type {StoreProvider} from './kv/store.ts';
import type {PatchOperation} from './patch-operation.ts';
import type {ReplicacheOptions} from './replicache-options.ts';
import type {WriteTransaction} from './transactions.ts';
import type {MutatorDefs} from './types.ts';

export {ReplicacheImpl};

export const valSize = 1024;

/**
 * The key/value store every benchmark rep is created with. Defaults to `'idb'`
 * so the browser perf harness is unchanged; React Native sets an
 * expo-sqlite/op-sqlite {@link StoreProvider} (or `'mem'`) before running.
 */
let benchKVStore: 'mem' | 'idb' | StoreProvider | undefined = 'idb';

export function setBenchKVStore(
  kvStore: 'mem' | 'idb' | StoreProvider | undefined,
): void {
  benchKVStore = kvStore;
}

export function getBenchKVStore(): 'mem' | 'idb' | StoreProvider | undefined {
  return benchKVStore;
}

function benchKVStoreProvider(): StoreProvider {
  return getKVStoreProvider(new LogContext(), benchKVStore);
}

export class ReplicachePerfTest<
  MD extends MutatorDefs,
> extends ReplicacheImpl<MD> {
  constructor(options: Omit<ReplicacheOptions<MD>, 'licenseKey'>) {
    super(
      {...options, kvStore: options.kvStore ?? benchKVStore},
      {
        enableMutationRecovery: false,
        enableScheduledRefresh: false,
        enableScheduledPersist: false,
      },
    );
  }
}

export function makeRepName(): string {
  return `bench${randomUint64()}`;
}

export function makeRep<MD extends MutatorDefs>(
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

/**
 * Payload for the benchmark mutators, handed over out of band rather than as
 * mutator arguments.
 *
 * A local mutation stores its arguments in the commit as `mutatorArgsJSON` so
 * the mutation can be rebased, and `persist` then serializes that to disk.
 * Passing the dataset as an argument therefore made every benchmark write its
 * own test data twice — measured on device, roughly half of what
 * `persist 1024x1000` serialized was the harness handing itself its data, not
 * storage work. Real mutations take small arguments; these now do too.
 *
 * Set these outside the timed region, exactly where the data used to be
 * generated.
 */
let populateValues: readonly TestDataObject[] = [];

export function setPopulateValues(values: readonly TestDataObject[]): void {
  populateValues = values;
}

export async function populate(
  tx: WriteTransaction,
  {numKeys}: {numKeys: number},
): Promise<void> {
  for (let i = 0; i < numKeys; i++) {
    await tx.set(`key${i}`, populateValues[i]);
  }
}

/** See {@link setPopulateValues} for why this is not a mutator argument. */
let putMapEntries: Record<string, TestDataObject> = {};

export function setPutMapEntries(map: Record<string, TestDataObject>): void {
  putMapEntries = map;
}

export async function putMap(tx: WriteTransaction): Promise<void> {
  for (const [key, value] of Object.entries(putMapEntries)) {
    await tx.set(key, value);
  }
}

export type PopulateMutatorDefs = {populate: typeof populate};
export type ReplicacheWithPopulate = ReplicachePerfTest<PopulateMutatorDefs>;

export function makeRepWithPopulate(
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

export function createIndexDefinitions(numIndexes: number): IndexDefinitions {
  const indexes: Writable<IndexDefinitions> = {};
  for (let i = 0; i < numIndexes; i++) {
    indexes[`idx${i}`] = {jsonPointer: '/ascii'};
  }
  return indexes;
}

export async function closeAndCleanupRep(
  rep: ReplicacheImpl | undefined,
): Promise<void> {
  if (rep) {
    await rep.close();
    await benchKVStoreProvider().drop(rep.idbName);
  }
}

export async function setupPersistedData(
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

export function sleep(ms: number): Promise<void> {
  return new Promise<void>(resolve => setTimeout(resolve, ms));
}

export function* rangeIter(end: number): Generator<number> {
  for (let i = 0; i < end; i++) yield i;
}

export function range(end: number): number[] {
  return [...rangeIter(end)];
}

export function sampleSize<T>(arr: Iterable<T>, n: number): T[] {
  return shuffle(arr).slice(0, n);
}

export function shuffle<T>(arr: Iterable<T>): T[] {
  const shuffled = [...arr];
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }
  return shuffled;
}
