import {expect, test} from 'vitest';
import {
  c15Findings,
  c9ResourceFindings,
  type C15Observations,
} from './chaos.ts';

test('accepts C9 recovery when earlier fat payloads make live pages decrease', () => {
  expect(
    c9ResourceFindings({
      changeLogLiveBytesBefore: 20_406_272,
      changeLogLiveBytesDuring: 13_434_880,
      changeLogLiveBytesAfter: 1_486_848,
      slotRetainedBytesBefore: 1_054_720,
      slotRetainedBytesDuring: 56_292_600,
      slotRetainedBytesAfter: 858_072,
    }),
  ).toEqual([]);
});

test('reports C9 live-page and WAL recovery failures', () => {
  expect(
    c9ResourceFindings({
      changeLogLiveBytesBefore: 10,
      changeLogLiveBytesDuring: 20,
      changeLogLiveBytesAfter: 20,
      slotRetainedBytesBefore: 10,
      slotRetainedBytesDuring: 20,
      slotRetainedBytesAfter: 20,
    }),
  ).toEqual([
    'C9: live change-log pages did not drain after the backup recovered',
    'C9: retained WAL did not drain after the backup recovered',
  ]);
});

test('reports an unavailable C9 live-page sample and an unpinned slot', () => {
  expect(
    c9ResourceFindings({
      changeLogLiveBytesBefore: -1,
      changeLogLiveBytesDuring: 20,
      changeLogLiveBytesAfter: 10,
      slotRetainedBytesBefore: 20,
      slotRetainedBytesDuring: 20,
      slotRetainedBytesAfter: 10,
    }),
  ).toEqual([
    'C9: change-log live-page usage was not measurable',
    'C9: the minio outage did not grow retained WAL',
  ]);
});

const c15 = (overrides: Partial<C15Observations> = {}): C15Observations => ({
  table: 'c15_backfill_run1',
  fixtureRows: 200_000,
  fixtureRowsSettled: true,
  runAnnounced: true,
  markedBeforeRestart: true,
  rowsFilledBeforeRestart: 40_000,
  resumedStart: 'resumed',
  demotions: 0,
  restores: 0,
  rowsAfterResume: [
    {node: 'rm', rows: 200_000},
    {node: 'vs-0', rows: 200_000},
  ],
  ...overrides,
});

test('accepts a backfill that resumed from the mark and completed', () => {
  expect(c15Findings(c15())).toEqual([]);
});

test('reports a restarted run that started over instead of resuming', () => {
  expect(c15Findings(c15({resumedStart: 'zero'}))).toEqual([
    "C15's restarted run of c15_backfill_run1 started from zero rather than " +
      "resuming from the replica's mark; the whole table is being copied again",
  ]);
});

test('reports a backfill that was dropped rather than resumed', () => {
  expect(c15Findings(c15({resumedStart: 'none-observed'}))).toEqual([
    'C15 restarted the RM mid-backfill but c15_backfill_run1 never announced ' +
      'another run; the backfill was dropped rather than resumed',
  ]);
});

test('reports a missing mark as unordered-or-too-small, not as a resume failure', () => {
  // The run finished before the RM was killed, so `resumedStart` says
  // nothing -- reporting it as a resume failure would be a false negative.
  expect(
    c15Findings(
      c15({
        markedBeforeRestart: false,
        rowsFilledBeforeRestart: 200_000,
        resumedStart: 'none-observed',
      }),
    ),
  ).toEqual([
    'C15 never saw a mark for c15_backfill_run1 (200000 of 200000 rows ' +
      'backfilled). Either the run was not ordered -- check that ' +
      '`ZERO_CHANGE_STREAMER_BACKFILL_RESUME=on` reached the RM and that the ' +
      'key cleared the correlation gate -- or it finished before the restart, ' +
      'which makes the fixture too small. The restart proved nothing either ' +
      'way.',
  ]);
});

test('reports a run that never announced itself', () => {
  expect(
    c15Findings(c15({runAnnounced: false, markedBeforeRestart: false})),
  ).toEqual([
    'C15 created c15_backfill_run1 with 200000 rows but no backfill run ' +
      'announced itself; either the change source did not pick the table up, ' +
      'or run announcements are not being logged',
  ]);
});

test('reports demotions, restores and a short replica alongside the resume', () => {
  expect(
    c15Findings(
      c15({
        demotions: 1,
        restores: 2,
        rowsAfterResume: [
          {node: 'rm', rows: 200_000},
          {node: 'vs-0', rows: 199_998},
        ],
      }),
    ),
  ).toEqual([
    'C15 demoted 1 follower(s) to PG after a backfill was interrupted; a ' +
      'backfill restart is not a replication gap',
    'C15 sent 2 follower(s) back to a litestream restore after a backfill ' +
      'was interrupted; a backfill restart is not a replication gap',
    "C15's resumed backfill of c15_backfill_run1 left 1 replica(s) short of " +
      '200000 rows: vs-0=199998',
  ]);
});

test('reports an unsettled fixture instead of judging the run', () => {
  // The column was added while the rows were still arriving, so nothing after
  // that is worth reading: report the setup, not a resume verdict.
  expect(
    c15Findings(
      c15({
        fixtureRowsSettled: false,
        runAnnounced: false,
        markedBeforeRestart: false,
        resumedStart: 'none-observed',
      }),
    ),
  ).toEqual([
    "C15's 200000 fixture rows did not reach every replica before the column " +
      'was added; the run it measured started from an unsettled table',
  ]);
});
