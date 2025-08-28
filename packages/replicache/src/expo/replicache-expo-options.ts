import type {StoreProvider} from '../kv/store.ts';
import type {ReplicacheOptions} from '../replicache-options.ts';
import type {MutatorDefs} from '../types.ts';

export type ReplicacheExpoOptions<MD extends MutatorDefs> = Omit<
  ReplicacheOptions<MD>,
  'kvStore'
> & {
  kvStore: 'expo-sqlite' | 'mem' | StoreProvider | undefined;
};
