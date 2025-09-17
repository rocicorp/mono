/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members, @typescript-eslint/prefer-promise-reject-errors */
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
): Promise<void> {
  for (const p of patch) {
    switch (p.op) {
      case 'put': {
        const frozen = deepFreeze(p.value);
        await dbWrite.put(lc, p.key, frozen);
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

        break;
      }
      case 'del': {
        const existing = await dbWrite.get(p.key);
        if (existing === undefined) {
          continue;
        }
        await dbWrite.del(lc, p.key);
        break;
      }
      case 'clear':
        await dbWrite.clear();
        break;
    }
  }
}
