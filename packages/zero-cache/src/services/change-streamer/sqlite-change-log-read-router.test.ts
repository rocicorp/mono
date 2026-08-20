import {describe, expect, test, vi} from 'vitest';
import type {ShardID} from '../../types/shards.ts';
import {isSampledForShard} from './shard-sampling.ts';
import {
  SQLiteChangeLogReadRouter,
  type SQLiteChangeLogCoverage,
} from './sqlite-change-log-read-router.ts';

describe('change-streamer/sqlite-change-log-read-router', () => {
  const shard: ShardID = {appID: 'router', shardNum: 3};
  const warm: SQLiteChangeLogCoverage = {
    seededAtMs: 1_000,
    seedWatermark: '01',
    minWatermark: '02',
    headWatermark: '09',
  };

  test('selection is stable by shard and task, and zero percent still inspects eligibility', () => {
    const inspect = vi.fn(() => warm);
    const zero = new SQLiteChangeLogReadRouter({
      shard,
      readPercent: 0,
      retentionMs: 100,
      now: () => 2_000,
      inspect,
    });
    expect(zero.consume('task-a')).toMatchObject({
      source: 'pg',
      reason: 'percentage',
      coverage: warm,
    });
    expect(inspect).toHaveBeenCalledOnce();

    const selected = Array.from({length: 1_000}, (_, i) => `task-${i}`).find(
      taskID => isSampledForShard(shard, taskID, 10),
    );
    const declined = Array.from({length: 1_000}, (_, i) => `task-${i}`).find(
      taskID => !isSampledForShard(shard, taskID, 10),
    );
    expect(selected).toBeDefined();
    expect(declined).toBeDefined();

    const canary = new SQLiteChangeLogReadRouter({
      shard,
      readPercent: 10,
      retentionMs: 100,
      now: () => 2_000,
      inspect: () => warm,
    });
    expect(canary.consume(selected as string).source).toBe('sqlite');
    expect(canary.consume(selected as string).source).toBe('sqlite');
    expect(canary.consume(declined as string).source).toBe('pg');
  });

  test('declines a cold log and reports its covered range', () => {
    const router = new SQLiteChangeLogReadRouter({
      shard,
      readPercent: 100,
      retentionMs: 1_000,
      now: () => 1_999,
      inspect: () => warm,
    });
    expect(router.consume('task')).toEqual({
      source: 'pg',
      reason: 'cold-log',
      coverage: warm,
    });
  });

  test('a reseed resets warm-up eligibility until retention elapses again', () => {
    let now = 2_000;
    let coverage = warm;
    const router = new SQLiteChangeLogReadRouter({
      shard,
      readPercent: 100,
      retentionMs: 1_000,
      now: () => now,
      inspect: () => coverage,
    });
    expect(router.consume('task').source).toBe('sqlite');

    coverage = {
      ...warm,
      seededAtMs: now,
      seedWatermark: warm.headWatermark,
      minWatermark: warm.headWatermark,
    };
    expect(router.consume('task')).toEqual({
      source: 'pg',
      reason: 'cold-log',
      coverage,
    });

    now += 1_000;
    expect(router.consume('task').source).toBe('sqlite');
  });

  test('coldReadPercent serves a share of cold logs, marked as such', () => {
    const cold: SQLiteChangeLogCoverage = {...warm, seededAtMs: 1_500};
    const router = new SQLiteChangeLogReadRouter({
      shard,
      readPercent: 100,
      coldReadPercent: 100,
      retentionMs: 1_000,
      now: () => 1_999,
      inspect: () => cold,
    });
    // Distinct from 'selected' so bootstrap serving is separable from
    // steady-state serving in the route metrics.
    expect(router.consume('task')).toEqual({
      source: 'sqlite',
      reason: 'selected-cold',
      coverage: cold,
    });
  });

  test('cold selection is a subset of warm selection', () => {
    const cold: SQLiteChangeLogCoverage = {...warm, seededAtMs: 1_500};
    // Selected at 100% but not at 10%: eligible warm, ineligible cold.
    const warmOnly = Array.from({length: 1_000}, (_, i) => `task-${i}`).find(
      taskID => !isSampledForShard(shard, taskID, 10),
    );
    expect(warmOnly).toBeDefined();

    const router = new SQLiteChangeLogReadRouter({
      shard,
      readPercent: 100,
      coldReadPercent: 10,
      retentionMs: 1_000,
      now: () => 1_999,
      inspect: () => cold,
    });
    expect(router.consume(warmOnly as string)).toMatchObject({
      source: 'pg',
      reason: 'cold-log',
    });

    const selected = Array.from({length: 1_000}, (_, i) => `task-${i}`).find(
      taskID => isSampledForShard(shard, taskID, 10),
    );
    expect(selected).toBeDefined();
    expect(router.consume(selected as string)).toMatchObject({
      source: 'sqlite',
      reason: 'selected-cold',
    });
  });

  test('the read percentage still applies to an eligible cold log', () => {
    const cold: SQLiteChangeLogCoverage = {...warm, seededAtMs: 1_500};
    const router = new SQLiteChangeLogReadRouter({
      shard,
      readPercent: 0,
      coldReadPercent: 100,
      retentionMs: 1_000,
      now: () => 1_999,
      inspect: () => cold,
    });
    // Cold eligibility opens the warm-up gate; it does not bypass the
    // rollout dial behind it.
    expect(router.consume('task')).toMatchObject({
      source: 'pg',
      reason: 'percentage',
    });
  });

  test('a snapshot pin is consumed once and cannot change source mid-restore', () => {
    let available = true;
    const router = new SQLiteChangeLogReadRouter({
      shard,
      readPercent: 100,
      retentionMs: 100,
      now: () => 2_000,
      inspect: () => (available ? warm : undefined),
    });

    expect(router.pin('task')).toMatchObject({source: 'sqlite'});
    available = false;
    expect(router.peek('task')).toMatchObject({
      source: 'sqlite',
      reason: 'selected',
      pinned: true,
    });
    expect(router.consume('task')).toMatchObject({
      source: 'sqlite',
      reason: 'selected',
      pinned: true,
    });
    expect(router.peek('task')).toBeUndefined();
    expect(router.consume('task')).toEqual({
      source: 'pg',
      reason: 'log-unavailable',
    });
  });

  test('demote() moves the pin, not just the reservation bounds', () => {
    const router = new SQLiteChangeLogReadRouter({
      shard,
      readPercent: 100,
      retentionMs: 100,
      now: () => 2_000,
      inspect: () => warm,
    });

    expect(router.pin('task')).toMatchObject({source: 'sqlite'});
    expect(router.demote('task')).toEqual({
      source: 'pg',
      reason: 'backup-uncovered',
    });
    // The /changes request that follows the reservation must agree with the
    // bounds it was confirmed with.
    expect(router.peek('task')).toMatchObject({
      source: 'pg',
      reason: 'backup-uncovered',
      pinned: true,
    });
    expect(router.consume('task')).toMatchObject({
      source: 'pg',
      reason: 'backup-uncovered',
      pinned: true,
    });
  });

  test('demote() does not resurrect a released pin', () => {
    const router = new SQLiteChangeLogReadRouter({
      shard,
      readPercent: 100,
      retentionMs: 100,
      now: () => 2_000,
      inspect: () => warm,
    });

    router.pin('task');
    router.release('task');
    expect(router.demote('task')).toEqual({
      source: 'pg',
      reason: 'backup-uncovered',
    });
    // Unpinned: the next request makes a fresh choice rather than inheriting
    // a demotion that belonged to a reservation that is already gone.
    expect(router.peek('task')).toBeUndefined();
    expect(router.consume('task')).toMatchObject({
      source: 'sqlite',
      reason: 'selected',
    });
  });

  test('post-registration failures route retries to PG until a bounded probe succeeds', () => {
    let now = 2_000;
    let available = true;
    const inspect = vi.fn(() => (available ? warm : undefined));
    const router = new SQLiteChangeLogReadRouter({
      shard,
      readPercent: 100,
      retentionMs: 100,
      failureCooldownMs: 500,
      now: () => now,
      inspect,
    });

    expect(router.consume('task').source).toBe('sqlite');
    router.trip();
    expect(router.consume('task')).toEqual({
      source: 'pg',
      reason: 'breaker-open',
    });
    expect(inspect).toHaveBeenCalledTimes(1);

    available = false;
    now += 500;
    expect(router.consume('task')).toEqual({
      source: 'pg',
      reason: 'log-unavailable',
    });
    // The failed probe rearms the cooldown.
    available = true;
    expect(router.consume('task')).toEqual({
      source: 'pg',
      reason: 'breaker-open',
    });

    now += 500;
    expect(router.consume('task').source).toBe('sqlite');
  });

  test('a probe that lands on a cold log still closes the breaker', () => {
    let now = 2_000;
    // Seeded 500 ms before the probe against a 1s retention window: eligible,
    // but still inside its warm-up window, so it stays on PG.
    const coverage: SQLiteChangeLogCoverage = {...warm, seededAtMs: 2_000};
    let available = true;
    const router = new SQLiteChangeLogReadRouter({
      shard,
      readPercent: 100,
      retentionMs: 1_000,
      failureCooldownMs: 500,
      now: () => now,
      inspect: () => (available ? coverage : undefined),
    });

    router.trip();
    now += 500;
    // The probe reads the log successfully; the warm-up gate, not the
    // breaker, is what keeps this task on PG.
    expect(router.consume('task')).toMatchObject({
      source: 'pg',
      reason: 'cold-log',
    });

    // A later ordinary unavailability is startup, not a failed probe, so it
    // must not rearm a full cooldown against an eligibility inspection that
    // already succeeded.
    available = false;
    expect(router.consume('task')).toEqual({
      source: 'pg',
      reason: 'log-unavailable',
    });
    available = true;
    expect(router.consume('task')).toMatchObject({
      source: 'pg',
      reason: 'cold-log',
    });
  });

  test('writer failure keeps the breaker open for the process lifetime', () => {
    let now = 2_000;
    const router = new SQLiteChangeLogReadRouter({
      shard,
      readPercent: 100,
      retentionMs: 100,
      failureCooldownMs: 1,
      now: () => now,
      inspect: () => warm,
    });
    router.trip(true);
    now += 1_000_000;
    expect(router.consume('task')).toEqual({
      source: 'pg',
      reason: 'breaker-open',
    });
  });
});
