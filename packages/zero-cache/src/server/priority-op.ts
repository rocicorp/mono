import type {LogContext} from '@rocicorp/logger';

let priorityOpCounter = 0;
let runningPriorityOpCounter = 0;

/**
 * Run an operation with priority, indicating that IVM should use smaller time
 * slices to allow this operation to proceed more quickly
 */
export async function runPriorityOp<T>(
  lc: LogContext,
  description: string,
  op: () => Promise<T>,
) {
  const id = priorityOpCounter++;
  runningPriorityOpCounter++;
  const start = Date.now();
  lc = lc.withContext('priorityOpID', id);
  try {
    lc.debug?.(`running priority op ${description}`);
    const result = await op();
    lc.debug?.(
      `finished priority op ${description} in ${Date.now() - start} ms`,
    );
    return result;
  } catch (e) {
    lc.debug?.(`failed priority op ${description} in ${Date.now() - start} ms`);
    throw e;
  } finally {
    runningPriorityOpCounter--;
  }
}

export function isPriorityOpRunning() {
  return runningPriorityOpCounter > 0;
}
