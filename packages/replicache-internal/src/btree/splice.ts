import {deepEqual} from '../json.js';
import type {ReadonlyEntry} from './node';

export type Splice = [at: number, removed: number, added: number, from: number];

const SPLICE_UNASSIGNED = -1;
export const SPLICE_AT = 0;
export const SPLICE_REMOVED = 1;
export const SPLICE_ADDED = 2;
export const SPLICE_FROM = 3;

const KEY = 0;
const VALUE = 1;

export function* computeSplices<T>(
  previous: readonly ReadonlyEntry<T>[],
  current: readonly ReadonlyEntry<T>[],
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
        deepEqual(previous[previousIndex][VALUE], current[currentIndex][VALUE])
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
