import {assertArray, assertObject, assertString} from 'shared/src/asserts.js';
import {
  type ReadonlyJSONObject,
  type ReadonlyJSONValue,
  assertJSONObject,
  assertJSONValue,
} from 'shared/src/json.js';

export type PatchOperationInternal =
  | {
      readonly op: 'put';
      readonly key: string;
      readonly value: ReadonlyJSONValue;
    }
  | {
      readonly op: 'update';
      readonly key: string;
      readonly merge?: ReadonlyJSONObject | undefined;
      readonly constrain?: string[] | undefined;
    }
  | {
      readonly op: 'del';
      readonly key: string;
    }
  | {
      readonly op: 'clear';
    };

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
): asserts p is PatchOperationInternal[] {
  assertArray(p);
  for (const item of p) {
    assertPatchOperation(item);
  }
}

function assertPatchOperation(p: unknown): asserts p is PatchOperationInternal {
  assertObject(p);
  switch (p.op) {
    case 'put':
      assertString(p.key);
      assertJSONValue(p.value);
      break;
    case 'update':
      assertString(p.key);
      if (p.merge !== undefined) {
        assertJSONObject(p.merge);
      }
      if (p.constrain !== undefined) {
        assertArray(p.constrain);
        for (const key of p.constrain) {
          assertString(key);
        }
      }
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
