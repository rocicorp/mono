import type {Patch, NullableVersion} from 'reflect-protocol';
import type {DurableStorage} from '../storage/durable-storage.js';
import {userValuePrefix, userValueSchema} from '../types/user-value.js';

export async function getPatch(
  storage: DurableStorage,
  fromCookie: NullableVersion,
): Promise<Patch> {
  const result = await storage.list({prefix: userValuePrefix}, userValueSchema);

  const patch: Patch = [];
  for (const [key, value] of result) {
    const validValue = value;

    // TODO: More efficient way of finding changed values.
    if (fromCookie !== null && validValue.version <= fromCookie) {
      continue;
    }

    const unwrappedKey = key.substring(userValuePrefix.length);
    const unwrappedValue = validValue.value;
    if (validValue.deleted) {
      if (fromCookie !== null) {
        patch.push({
          op: 'del',
          key: unwrappedKey,
        });
      }
    } else {
      patch.push({
        op: 'put',
        key: unwrappedKey,
        value: unwrappedValue,
      });
    }
  }
  return patch;
}
