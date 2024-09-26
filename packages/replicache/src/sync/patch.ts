import type {LogContext} from '@rocicorp/logger';
import {assertObject} from 'shared/src/asserts.js';
import type {ReadonlyJSONObject, ReadonlyJSONValue} from 'shared/src/json.js';
import type {Write} from '../db/write.js';
import {
  type FrozenJSONObject,
  type FrozenJSONValue,
  deepFreeze,
} from '../frozen-json.js';
import type {PatchOperationInternal} from '../patch-operation.js';

export async function apply(
  lc: LogContext,
  dbWrite: Write,
  patch: readonly PatchOperationInternal[],
): Promise<void> {
  for (const p of patch) {
    switch (p.op) {
      case 'put': {
        await dbWrite.put(lc, p.key, deepFreeze(p.value));
        break;
      }
      case 'update': {
        const existing = await dbWrite.get(p.key);
        const entries: [
          string,
          FrozenJSONValue | ReadonlyJSONValue | undefined,
        ][] = [];
        const addToEntries = (toAdd: FrozenJSONObject | ReadonlyJSONObject) => {
          for (const [key, value] of Object.entries(toAdd)) {
            if (
              !p.constrain ||
              p.constrain.length === 0 ||
              p.constrain.indexOf(key) > -1
            ) {
              entries.push([key, value]);
            }
          }
        };
        if (existing !== undefined) {
          assertObject(existing);
          addToEntries(existing);
        }
        if (p.merge) {
          addToEntries(p.merge);
        }
        await dbWrite.put(lc, p.key, deepFreeze(Object.fromEntries(entries)));
        break;
      }
      case 'del':
        await dbWrite.del(lc, p.key);
        break;

      case 'clear':
        await dbWrite.clear();
        break;
    }
  }
}
