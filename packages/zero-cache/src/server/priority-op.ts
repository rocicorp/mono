import {resolver, type Resolver} from '@rocicorp/resolver';
import {must} from '../../../shared/src/must.ts';

let priorityOpCounter = 0;

let priorityOpResolver: Resolver<void> | undefined = undefined;

/**
 * Run an operation with priority, indicating that IVM should use smaller time
 * slices to allow this operation to proceed more quickly
 */
async function runPriorityOp<T>(op: () => Promise<T>) {
  priorityOpCounter++;
  if (priorityOpResolver === undefined) {
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
 * Temporary mechanism for debugging, allows IVM to wait until no
 * priorty ops are running before processing IVM time slices.
 */
export function noPriorityOpRunningPromise(): Promise<void> | undefined {
  return priorityOpResolver?.promise;
}

export function isPriorityOpRunning() {
  return priorityOpCounter > 0;
}
