import {MemStore} from './kv/mem-store.ts';

export function createMemStore(name: string): MemStore {
  return new MemStore(name);
}
