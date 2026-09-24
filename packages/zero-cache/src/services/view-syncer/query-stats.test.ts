import {LogContext} from '@rocicorp/logger';
import {expect, test, vi} from 'vitest';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {PlanWarning} from '../../../../zql/src/planner/planner-warnings.ts';
import {QueryStats, type QueryIdentity} from './query-stats.ts';

function query(hash: string, queryName?: string): QueryIdentity {
  return {queryName, shape: {hash, zql: `zql-${hash}`}};
}

function flush(stats: QueryStats) {
  const sink = new TestLogSink();
  stats.flush(new LogContext('info', undefined, sink));
  return sink.messages.map(([level, , [message, fields]]) => {
    expect(level).toBe('info');
    return {message, ...(fields as Record<string, unknown>)} as Record<
      string,
      unknown
    >;
  });
}

const missingIndex: PlanWarning = {
  type: 'missing-index',
  table: 'comment',
  path: ['comments'],
  columns: ['issueID'],
  rows: 50_000,
  perRow: true,
  suggestedIndex: ['issueID', 'id'],
};

test('aggregates hydrations and advancements per query shape', () => {
  let now = 1000;
  const stats = new QueryStats({now: () => now});
  const q = query('a', 'issues');
  stats.recordHydration(q, {
    outcome: 'finished',
    timeMs: 10,
    rowCount: 5,
    rowsRead: 50,
    planWarnings: [missingIndex],
  });
  stats.recordHydration(q, {outcome: 'aborted', timeMs: 30});
  stats.recordHydration(q, {outcome: 'failed', timeMs: 2});
  stats.recordAdvance(q, {timeMs: 4, changes: 3, timedOut: false});
  stats.recordAdvance(q, {timeMs: 1, changes: 1, timedOut: true});
  now = 61_000;

  const counts = {
    timeMs: 47,
    hydrations: {count: 3, sumMs: 42, minMs: 2, maxMs: 30},
    hydrationsAborted: 1,
    hydrationsFailed: 1,
    hydrationRowCount: 5,
    hydrationRowsRead: 50,
    advances: {count: 2, sumMs: 5, minMs: 1, maxMs: 4},
    advanceChanges: 4,
    advanceTimeouts: 1,
  };
  expect(flush(stats)).toEqual([
    {
      message: 'query stats',
      zeroEvent: 'query-stats',
      intervalMs: 60_000,
      queryName: 'issues',
      queryShape: 'a',
      ...counts,
      planWarnings: [missingIndex],
      zql: 'zql-a',
    },
    {
      message: 'query stats summary',
      zeroEvent: 'query-stats-summary',
      intervalMs: 60_000,
      shapes: 1,
      shapesReported: 1,
      ...counts,
    },
  ]);
});

test('keys shapes by query name and hash', () => {
  const stats = new QueryStats();
  stats.recordAdvance(query('a', 'x'), {
    timeMs: 1,
    changes: 1,
    timedOut: false,
  });
  stats.recordAdvance(query('a', 'y'), {
    timeMs: 1,
    changes: 1,
    timedOut: false,
  });
  stats.recordAdvance(query('a'), {timeMs: 1, changes: 1, timedOut: false});
  stats.recordAdvance(query('a', 'x'), {
    timeMs: 1,
    changes: 1,
    timedOut: false,
  });
  expect(
    flush(stats).map(({queryName, queryShape, advances}) => ({
      queryName,
      queryShape,
      advances: (advances as {count: number}).count,
    })),
  ).toEqual([
    {queryName: 'x', queryShape: 'a', advances: 2},
    {queryName: 'y', queryShape: 'a', advances: 1},
    {queryName: undefined, queryShape: 'a', advances: 1},
    {queryName: undefined, queryShape: undefined, advances: 4},
  ]);
});

test('reports the shapes that took the most time, and all in the summary', () => {
  const stats = new QueryStats({maxReported: 2});
  for (const [hash, timeMs] of [
    ['a', 1],
    ['b', 30],
    ['c', 20],
  ] as const) {
    stats.recordHydration(query(hash), {outcome: 'finished', timeMs});
  }
  const logged = flush(stats);
  expect(logged.map(({queryShape}) => queryShape)).toEqual([
    'b',
    'c',
    undefined,
  ]);
  expect(logged[2]).toMatchObject({
    shapes: 3,
    shapesReported: 2,
    timeMs: 51,
    hydrations: {count: 3, sumMs: 51, minMs: 1, maxMs: 30},
  });
});

test('counts shapes beyond maxShapes in the summary only', () => {
  const stats = new QueryStats({maxShapes: 1});
  stats.recordHydration(query('a'), {outcome: 'finished', timeMs: 1});
  stats.recordHydration(query('b'), {outcome: 'finished', timeMs: 2});
  stats.recordHydration(query('b'), {outcome: 'finished', timeMs: 2});
  const logged = flush(stats);
  expect(logged.map(({queryShape}) => queryShape)).toEqual(['a', undefined]);
  expect(logged[1]).toMatchObject({
    shapes: 2,
    shapesReported: 1,
    hydrations: {count: 3, sumMs: 5},
  });

  // The limit applies per interval.
  stats.recordHydration(query('b'), {outcome: 'finished', timeMs: 2});
  expect(flush(stats).map(({queryShape}) => queryShape)).toEqual([
    'b',
    undefined,
  ]);
});

test('logs nothing for an interval without work', () => {
  const stats = new QueryStats();
  expect(flush(stats)).toEqual([]);
});

test('start flushes every interval and once more when stopped', () => {
  vi.useFakeTimers();
  try {
    const sink = new TestLogSink();
    const stats = new QueryStats();
    const stop = stats.start(new LogContext('info', undefined, sink), 60_000);
    const events = () =>
      sink.messages.map(
        ([, , [, fields]]) => (fields as {zeroEvent: string}).zeroEvent,
      );

    stats.recordHydration(query('a'), {outcome: 'finished', timeMs: 1});
    vi.advanceTimersByTime(60_000);
    expect(events()).toEqual(['query-stats', 'query-stats-summary']);

    stats.recordHydration(query('a'), {outcome: 'finished', timeMs: 1});
    stop();
    expect(events()).toHaveLength(4);

    vi.advanceTimersByTime(60_000);
    expect(events()).toHaveLength(4);
  } finally {
    vi.useRealTimers();
  }
});
