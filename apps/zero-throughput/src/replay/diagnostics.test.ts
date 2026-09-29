import {expect, test} from 'vitest';
import {summarizeResets} from './diagnostics.ts';

test('summarizeResets counts resets by phase and reason', () => {
  const start = Date.parse('2026-09-28T19:00:00.000Z');
  const log = [
    '2026-09-28T15:00:30.000-04:00 pid=1,worker=syncer resetting pipelines: Advancement exceeded timeout at 10 of 900 changes after 5000 ms.',
    '2026-09-28T15:01:30.000-04:00 pid=1,worker=syncer resetting pipelines: Advancement projected to exceed hydration time at 5 of 900 changes',
    "2026-09-28T15:01:40.000-04:00 [ 'pid=1', 'worker=syncer',",
    "  'clientGroupID=cg' ] resetting pipelines: Advancement exceeded timeout at 1 of 2 changes",
    '2026-09-28T15:01:41.000-04:00 pid=1,worker=syncer some other line',
  ].join('\n');
  expect(
    summarizeResets(log, start, [
      {label: 'quiet', startMs: 0, endMs: 60_000},
      {label: 'loaded', startMs: 60_000, endMs: 120_000},
    ]),
  ).toEqual({
    total: 3,
    byPhase: {quiet: 1, loaded: 2},
    byReason: {
      'Advancement exceeded timeout at': 2,
      'Advancement projected to exceed': 1,
    },
  });
});
