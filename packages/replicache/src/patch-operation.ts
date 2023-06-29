import {assertArray, assertObject, assertString} from 'shared/src/asserts.js';
import {ReadonlyJSONValue, assertJSONValue} from './json.js';

/**
 * This type describes the patch field in a {@link PullResponse} and it is used
 * to describe how to update the Replicache key-value store.
 */
export type PatchOperation =
  | {
      readonly op: 'put';
      readonly key: string;
      readonly value: ReadonlyJSONValue;
    }
  | {
      readonly op: 'del';
      readonly key: string;
    }
  | {
      readonly op: 'clear';
    };

export function assertPatchOperations(
  p: unknown,
): asserts p is PatchOperation[] {
  assertArray(p);
  for (const item of p) {
    assertPatchOperation(item);
  }
}

function assertPatchOperation(p: unknown): asserts p is PatchOperation {
  assertObject(p);
  switch (p.op) {
    case 'put':
      assertString(p.key);
      assertJSONValue(p.value);
      break;
    case 'del':
      assertString(p.key);
      break;
    case 'clear':
      break;
    default:
      throw new Error(
        `unknown patch op \`${p.op}\`, expected one of \`put\`, \`del\`, \`clear\``,
      );
  }
}
