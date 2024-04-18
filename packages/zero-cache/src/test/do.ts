import type {DurableObjectStorage} from '@cloudflare/workers-types';
import {env, runInDurableObject} from 'cloudflare:test';

export function runWithDurableObjectStorage<R>(
  fn: (storage: DurableObjectStorage) => R | Promise<R>,
): Promise<R> {
  const {runnerDO} = env;
  const id = runnerDO.newUniqueId();
  const stub = runnerDO.get(id);
  return runInDurableObject(stub, (_, {storage}) => fn(storage));
}
