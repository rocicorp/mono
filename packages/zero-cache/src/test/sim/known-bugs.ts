/**
 * Failures of bugs that a pinned test reproduces, which a sweep counts rather
 * than fails on: a sweep that reached one would otherwise end there, before it
 * found anything new.
 */
const KNOWN_BUGS = [
  {
    // A replicated row key change onto a phantom row, which a run snapshotted
    // ahead of the replica inserted at the new key. Pinned in
    // `backfill.sim.test.ts`.
    name: 'key-change-onto-phantom',
    error: 'SqliteError: UNIQUE constraint failed',
  },
];

/**
 * The known bug that `e`, a run's failure, is, if every problem it reports is
 * that bug's. `ZERO_SIM_KNOWN_BUGS=fail` makes none of them known.
 */
export function knownBug(e: unknown): string | undefined {
  if (process.env['ZERO_SIM_KNOWN_BUGS'] === 'fail') {
    return undefined;
  }
  const message = e instanceof Error ? e.message : String(e);
  // A failure is a line saying where, a line per problem, and then the trace.
  const [, ...problems] = message.split('\n\nthe last')[0].split('\n');
  return KNOWN_BUGS.find(
    bug => problems.length > 0 && problems.every(p => p.includes(bug.error)),
  )?.name;
}
