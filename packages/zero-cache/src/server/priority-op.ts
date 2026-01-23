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
  try {
    lc.debug?.(`running priority op ${id} ${description}`);
    const result = await op();
    lc.debug?.(
      `finished priority op ${id} ${description} in ${Date.now() - start} ms`,
    );
    return result;
  } catch (e) {
    lc.debug?.(
      `failed priority op ${id} ${description} in ${Date.now() - start} ms`,
    );
    throw e;
  } finally {
    runningPriorityOpCounter--;
  }
}

export function isPriorityOpRunning() {
  return runningPriorityOpCounter > 0;
}
