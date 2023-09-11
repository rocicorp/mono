type LogFunction = ((...args: unknown[]) => void) | undefined;

declare const performance: {
  now(): number;
};

/**
 * Times some async function and writes the result to a provided log function.
 * The log function can be undefined to simplify use with OptionalLogger.
 * @param log Log function to write to (ie LogContext.log)
 * @param label Label to write at start and end of function
 * @param fn Function to time
 * @returns The result of fn
 */
export async function timed<R>(
  log: LogFunction | undefined,
  label: string,
  fn: () => Promise<R>,
): Promise<R> {
  log?.(`Starting ${label}`);
  const clock = typeof performance !== 'undefined' ? performance : Date;
  const t0 = clock.now();
  try {
    return await fn();
  } finally {
    const t1 = clock.now();
    log?.(`Finished ${label} in ${t1 - t0}ms`);
  }
}
