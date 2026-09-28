import {describe, expect, test} from 'vitest';
import {distribution, Recorder, slope} from './recorder.ts';
import {exponential, seededRandom} from './sessions.ts';

test('distribution reports nearest-rank percentiles', () => {
  const values = Array.from({length: 100}, (_, i) => i + 1);
  expect(distribution(values)).toEqual({
    count: 100,
    p50: 50,
    p95: 95,
    p99: 99,
    max: 100,
  });
  expect(distribution([])).toEqual({count: 0, p50: 0, p95: 0, p99: 0, max: 0});
});

test('slope fits a least-squares line', () => {
  expect(
    slope([
      [0, 1],
      [1, 3],
      [2, 5],
    ]),
  ).toBe(2);
  expect(slope([[0, 1]])).toBe(0);
});

describe('Recorder', () => {
  test('buckets samples and summarizes phases', () => {
    let now = 0;
    const recorder = new Recorder(60_000, () => now);
    const phases = [
      {label: 'quiet', startMs: 0, endMs: 120_000},
      {label: 'rate=24', startMs: 120_000, endMs: 240_000},
    ];
    for (let minute = 0; minute < 4; minute++) {
      now = minute * 60_000 + 59_000;
      recorder.hydrated('homeView', 'session', 100 * (minute + 1), false);
      recorder.hydrated('workById', 'screen', 50, true);
      recorder.backfillPage(100, minute >= 2 ? 100 : 0, 5);
      recorder.gauges({
        activeSessions: 10,
        pendingQueries: 0,
        eventLoopDelayP99Ms: 1,
      });
    }
    now = 240_000;

    const timeline = recorder.timeline(phases);
    expect(timeline.map(b => [b.startS, b.phase])).toEqual([
      [0, 'quiet'],
      [60, 'quiet'],
      [120, 'rate=24'],
      [180, 'rate=24'],
      [240, undefined],
    ]);
    expect(timeline[1].sessionHydration.max).toBe(200);
    expect(timeline[1].screenHydration.max).toBe(50);
    expect(timeline[2].backfillRowsWritten).toBe(100);
    expect(timeline[3].activeSessions).toBe(10);

    const [quiet, loaded] = recorder.phaseSummaries(phases);
    expect(quiet.hydration.max).toBe(200);
    expect(loaded.hydration.max).toBe(400);
    expect(loaded.hydrationTail.max).toBe(400);
    expect(loaded.backfillRowsPerSecond).toBeCloseTo(200 / 120, 1);
    expect(quiet.backfillRowsPerSecond).toBe(0);
    expect(loaded.hydrationP95SlopeMsPerMin).toBe(100);

    const byName = recorder.hydrationByName(0, 240_000);
    expect(byName.map(n => [n.name, n.returning, n.ms.count])).toEqual([
      ['homeView', false, 4],
      ['workById', true, 4],
    ]);
  });

  test('counts errors and unexpected closes', () => {
    const recorder = new Recorder(1_000, () => 0);
    recorder.sessionEnded(true, '1000 session ended');
    recorder.sessionEnded(false, '1006 ');
    recorder.queryError('workById', 'boom');
    expect(Object.fromEntries(recorder.errors())).toEqual({
      'close: 1006 ': 1,
      'query workById: boom': 1,
    });
    const [bucket] = recorder.timeline([]);
    expect(bucket.sessionsEnded).toBe(2);
    expect(bucket.unexpectedCloses).toBe(1);
  });
});

test('seededRandom is repeatable and exponential has the right mean', () => {
  const a = seededRandom(42);
  const b = seededRandom(42);
  expect([a(), a(), a()]).toEqual([b(), b(), b()]);
  const random = seededRandom(7);
  let total = 0;
  for (let i = 0; i < 20_000; i++) {
    total += exponential(random, 1_000);
  }
  expect(total / 20_000).toBeGreaterThan(950);
  expect(total / 20_000).toBeLessThan(1_050);
});
