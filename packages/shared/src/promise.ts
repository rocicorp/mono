/**
 * Like Promise.race but returns the index of the first promise that resolved.
 */
export function promiseRace(ps: Promise<unknown>[]): Promise<number> {
  return Promise.race(ps.map((p, i) => p.then(() => i)));
}
