/**
 * Creates a promise that resolves after [[ms]] milliseconds. Note that if you
 * pass in `0` no `setTimeout` is used and the promise resolves immediately. In
 * other words no macro task is used in that case.
 */
export function sleep(ms: number): Promise<void> {
  if (ms === 0) {
    return Promise.resolve();
  }

  return new Promise(resolve => {
    setTimeout(() => resolve(), ms);
  });
}
