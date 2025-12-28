import type {LogContext} from '@rocicorp/logger';
import {compareUTF8} from 'compare-utf8';
import {assertObject} from '../../../shared/src/asserts.ts';
import type {
  ReadonlyJSONObject,
  ReadonlyJSONValue,
} from '../../../shared/src/json.ts';
import type {DiffOperation} from '../btree/node.ts';
import type {Write} from '../db/write.ts';
import {
  type FrozenJSONObject,
  type FrozenJSONValue,
  deepFreeze,
} from '../frozen-json.ts';
import type {PatchOperationInternal} from '../patch-operation.ts';

export type Diff =
  | DiffOperation<string>
  | {
      op: 'clear';
    };

/**
 * Optimizes a patch array by:
 * 1. Dropping all operations before the last 'clear'
 * 2. For each key: put/del replace all previous operations; updates accumulate
 * 3. Removing standalone 'del' operations after a clear (deleting from empty tree)
 * 4. Merging updates after puts into a single put operation
 * Note: Order is preserved for operations on the same key, but operations
 * on different keys can be reordered.
 */
export function optimizePatch(
  patch: readonly PatchOperationInternal[],
): PatchOperationInternal[] {
  if (patch.length === 0) {
    return [];
  }

  // Build result array
  const result: PatchOperationInternal[] = [];

  // Find the last clear operation, add it to result, and track start index
  let i = 0;
  let hasClear = false;
  for (i = patch.length - 1; i >= 0; i--) {
    if (patch[i].op === 'clear') {
      result.push(patch[i]);
      hasClear = true;
      break;
    }
  }
  // After loop: i is either the clear index (if found) or -1 (if not found)
  // Increment to get the start index for processing remaining operations
  i++;

  // Track operations for each key
  // del and put replace all previous operations
  // updates after puts get merged into the put
  const keyOps = new Map<string, PatchOperationInternal[]>();

  for (; i < patch.length; i++) {
    const p = patch[i];

    switch (p.op) {
      case 'put':
      case 'del': {
        // put and del replaces all previous operations on that key
        keyOps.set(p.key, [p]);
        break;
      }
      case 'update': {
        // update accumulates with previous operations
        const existing = keyOps.get(p.key);
        if (existing) {
          // Merge with existing put if possible
          if (existing.length === 1 && existing[0].op === 'put') {
            const {value} = existing[0];
            assertObject(value);
            const merged = mergeUpdate(p, value);
            keyOps.set(p.key, [{op: 'put', key: p.key, value: merged}]);
          } else {
            // Can't merge, accumulate the update
            existing.push(p);
          }
        } else {
          // No existing operation, just store the update
          keyOps.set(p.key, [p]);
        }
        break;
      }
    }
  }

  // Add all remaining key operations, but skip standalone del after clear
  for (const ops of keyOps.values()) {
    // Skip standalone del operations after clear (deleting from empty tree is pointless)
    if (hasClear && ops.length === 1 && ops[0].op === 'del') {
      continue;
    }
    result.push(...ops);
  }

  return result.sort((a, b) => {
    if (a.op === 'clear') return -1;
    if (b.op === 'clear') return 1;
    return compareUTF8(a.key, b.key);
  });
}

export async function apply(
  lc: LogContext,
  dbWrite: Write,
  patch: readonly PatchOperationInternal[],
): Promise<void> {
  // Optimize the patch to remove redundant operations
  const optimized = optimizePatch(patch);

  let i = 0;

  // Handle clear if present (always at index 0 after optimization)
  if (optimized.length > 0 && optimized[0].op === 'clear') {
    await dbWrite.clear();
    i = 1;
  }

  // Check if we can bulk load put some operations
  const bulkLoadStart = i;
  while (i < optimized.length && optimized[i].op === 'put') {
    i++;
  }

  if (i > bulkLoadStart) {
    await bulkLoadPuts(lc, dbWrite, optimized.slice(bulkLoadStart, i));
  }

  // Apply remaining operations individually
  while (i < optimized.length) {
    const op = optimized[i];

    switch (op.op) {
      case 'put': {
        const frozen = deepFreeze(op.value);
        await dbWrite.put(lc, op.key, frozen);
        break;
      }
      case 'update': {
        const existing = await dbWrite.get(op.key);
        if (existing !== undefined) {
          assertObject(existing);
        }
        const frozen = mergeUpdate(op, existing);
        await dbWrite.put(lc, op.key, frozen);
        break;
      }
      case 'del': {
        const existing = await dbWrite.get(op.key);
        if (existing === undefined) {
          i++;
          continue;
        }
        await dbWrite.del(lc, op.key);
        break;
      }
    }
    i++;
  }
}

function mergeUpdate(
  op: Extract<PatchOperationInternal, {op: 'update'}>,
  existing: ReadonlyJSONObject | undefined,
) {
  const entries: [string, FrozenJSONValue | ReadonlyJSONValue | undefined][] =
    [];
  const addToEntries = (toAdd: FrozenJSONObject | ReadonlyJSONObject) => {
    for (const [key, value] of Object.entries(toAdd)) {
      if (
        !op.constrain ||
        op.constrain.length === 0 ||
        op.constrain.indexOf(key) > -1
      ) {
        entries.push([key, value]);
      }
    }
  };
  if (existing !== undefined) {
    addToEntries(existing);
  }
  if (op.merge) {
    addToEntries(op.merge);
  }
  return deepFreeze(Object.fromEntries(entries));
}

async function bulkLoadPuts(
  lc: LogContext,
  dbWrite: Write,
  puts: readonly PatchOperationInternal[],
): Promise<void> {
  if (puts.length === 0) {
    return;
  }

  // Sort entries by key for bulk loading
  const entries: [string, FrozenJSONValue][] = puts.map(p => {
    if (p.op !== 'put') {
      throw new Error('Expected put operation');
    }
    return [p.key, deepFreeze(p.value)];
  });

  // already sorted

  // Use putMany which will use BTreeWrite.fromEntries if the map is empty
  await dbWrite.putMany(lc, entries);
}
