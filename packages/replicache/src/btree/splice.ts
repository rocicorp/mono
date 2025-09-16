/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {deepEqual, type ReadonlyJSONValue} from '../../../shared/src/json.ts';

export type Splice = [at: number, removed: number, added: number, from: number];

const SPLICE_UNASSIGNED = -1;
export const SPLICE_AT = 0;
export const SPLICE_REMOVED = 1;
export const SPLICE_ADDED = 2;
export const SPLICE_FROM = 3;

const KEY = 0;
const VALUE = 1;

type Entry<V> = readonly [key: string, value: V, ...rest: unknown[]];

export function* computeSplices<T>(
  previous: readonly Entry<T>[],
  current: readonly Entry<T>[],
): Generator<Splice, void> {
  let previousIndex = 0;
  let currentIndex = 0;
  let splice: Splice | undefined;

  function ensureAssigned(splice: Splice, index: number): void {
    if (splice[SPLICE_FROM] === SPLICE_UNASSIGNED) {
      splice[SPLICE_FROM] = index;
    }
  }

  function newSplice(): Splice {
    return [previousIndex, 0, 0, SPLICE_UNASSIGNED];
  }

  while (previousIndex < previous.length && currentIndex < current.length) {
    if (previous[previousIndex][KEY] === current[currentIndex][KEY]) {
      if (
        deepEqual(
          // These are really Hash | InternalValue
          previous[previousIndex][VALUE] as ReadonlyJSONValue,
          current[currentIndex][VALUE] as ReadonlyJSONValue,
        )
      ) {
        if (splice) {
          ensureAssigned(splice, 0);
          yield splice;
          splice = undefined;
        }
      } else {
        if (!splice) {
          splice = newSplice();
        }
        splice[SPLICE_ADDED]++;
        splice[SPLICE_REMOVED]++;
        ensureAssigned(splice, currentIndex);
      }
      previousIndex++;
      currentIndex++;
    } else if (previous[previousIndex][KEY] < current[currentIndex][KEY]) {
      // previous was removed
      if (!splice) {
        splice = newSplice();
      }
      splice[SPLICE_REMOVED]++;

      previousIndex++;
    } else {
      // current was added
      if (!splice) {
        splice = newSplice();
      }
      splice[SPLICE_ADDED]++;
      ensureAssigned(splice, currentIndex);

      currentIndex++;
    }
  }

  if (currentIndex < current.length) {
    if (!splice) {
      splice = newSplice();
    }
    splice[SPLICE_ADDED] += current.length - currentIndex;
    ensureAssigned(splice, currentIndex);
  }

  if (previousIndex < previous.length) {
    if (!splice) {
      splice = newSplice();
    }
    splice[SPLICE_REMOVED] += previous.length - previousIndex;
  }

  if (splice) {
    ensureAssigned(splice, 0);
    yield splice;
  }
}
