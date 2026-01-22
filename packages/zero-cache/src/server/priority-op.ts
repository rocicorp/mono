import {resolver, type Resolver} from '@rocicorp/resolver';
import {must} from '../../../shared/src/must.ts';
import {assert} from '../../../shared/src/asserts.ts';

let priorityOpCounter = 0;

let priorityOpResolver: Resolver<void> | undefined = undefined;

/**
 * Run an operation with priority, indicating that IVM should use smaller time
 * slices to allow this operation to proceed more quickly
 */
export async function runPriorityOp<T>(op: () => Promise<T>) {
  priorityOpCounter++;
  if (priorityOpResolver === undefined) {
    assert(priorityOpResolver === undefined);
    priorityOpResolver = resolver();
  }
  try {
    return await op();
  } finally {
    priorityOpCounter--;
    if (priorityOpCounter === 0) {
      const priorityOpResolve = must(priorityOpResolver).resolve;
      priorityOpResolver = undefined;
      priorityOpResolve();
    }
  }
}

/**
 * If a priority op is running, returns a promise that resolves when it is
 * complete, otherwise returns a promise that resolves immediately.
 */
export function noPriorityOpRunningPromise(): Promise<void> {
  return priorityOpResolver?.promise ?? Promise.resolve();
}

export function isPriorityOpRunning() {
  return priorityOpCounter > 0;
}
