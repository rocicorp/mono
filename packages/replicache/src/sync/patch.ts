import type {LogContext} from '@rocicorp/logger';
import {assertObject} from '../../../shared/src/asserts.ts';
import type {
  ReadonlyJSONObject,
  ReadonlyJSONValue,
} from '../../../shared/src/json.ts';
import type {Write} from '../db/write.ts';
import {
  type FrozenJSONObject,
  type FrozenJSONValue,
  deepFreeze,
} from '../frozen-json.ts';
import type {PatchOperationInternal} from '../patch-operation.ts';
import type {DiffOperation} from '../btree/node.ts';

export type Diff =
  | DiffOperation<string>
  | {
      op: 'clear';
    };

export async function apply(
  lc: LogContext,
  dbWrite: Write,
  patch: readonly PatchOperationInternal[],
): Promise<readonly Diff[]> {
  const ret: Diff[] = [];
  for (const p of patch) {
    switch (p.op) {
      case 'put': {
        const existing = await dbWrite.get(p.key);
        const frozen = deepFreeze(p.value);
        await dbWrite.put(lc, p.key, frozen);
        if (existing === undefined) {
          ret.push({
            op: 'add',
            key: p.key,
            newValue: frozen,
          });
        } else {
          ret.push({
            op: 'change',
            key: p.key,
            oldValue: existing,
            newValue: frozen,
          });
        }
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
        const frozen = deepFreeze(Object.fromEntries(entries));
        await dbWrite.put(lc, p.key, frozen);
        if (existing === undefined) {
          ret.push({
            op: 'add',
            key: p.key,
            newValue: frozen,
          });
        } else {
          ret.push({
            op: 'change',
            key: p.key,
            oldValue: existing,
            newValue: frozen,
          });
        }

        break;
      }
      case 'del': {
        const existing = await dbWrite.get(p.key);
        if (existing === undefined) {
          continue;
        }
        await dbWrite.del(lc, p.key);
        ret.push({
          op: 'del',
          key: p.key,
          oldValue: existing,
        });
        break;
      }
      case 'clear':
        await dbWrite.clear();
        ret.push({
          op: 'clear',
        });
        break;
    }
  }

  return ret;
}
