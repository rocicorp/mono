import {resolver} from '@rocicorp/resolver';
import {randomUint64} from '../../shared/src/random-uint64.ts';
import {
  jsonArrayTestData,
  type TestDataObject,
} from '../../shared/src/test-data.ts';
import type {Writable} from '../../shared/src/writable.ts';
import {ReplicacheImpl} from './impl.ts';
import type {IndexDefinitions} from './index-defs.ts';
import {dropIDBStoreWithMemFallback} from './kv/idb-store-with-mem-fallback.ts';
import type {PatchOperation} from './patch-operation.ts';
import type {ReplicacheOptions} from './replicache-options.ts';
import type {WriteTransaction} from './transactions.ts';
import type {MutatorDefs} from './types.ts';

export {ReplicacheImpl};

export const valSize = 1024;

export class ReplicachePerfTest<
  MD extends MutatorDefs,
> extends ReplicacheImpl<MD> {
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

export async function populate(
  tx: WriteTransaction,
  {numKeys, randomValues}: {numKeys: number; randomValues: TestDataObject[]},
): Promise<void> {
  for (let i = 0; i < numKeys; i++) {
    await tx.set(`key${i}`, randomValues[i]);
  }
}

export async function putMap(
  tx: WriteTransaction,
  map: Record<string, TestDataObject>,
): Promise<void> {
  for (const [key, value] of Object.entries(map)) {
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
    await dropIDBStoreWithMemFallback(rep.idbName);
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
