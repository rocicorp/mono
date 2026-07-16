import {describe, expect, test} from 'vitest';
import {computeLivenessTimings} from './stream.ts';

describe('computeLivenessTimings', () => {
  // Regression test for https://github.com/rocicorp/mono/pull/6047, which
  // introduced the inbound-liveness watchdog. Postgres treats
  // `wal_sender_timeout = 0` as "disabled", but the watchdog derived every
  // threshold by plain arithmetic on that value, collapsing them all to 0.
  // That fired the teardown on the first tick (and busy-spun the timer at a
  // 0ms interval), producing a continuous reconnect storm for anyone running
  // with wal_sender_timeout disabled.
  test('a disabled (0) wal_sender_timeout disables the liveness timer', () => {
    expect(computeLivenessTimings(0)).toEqual({
      enabled: false,
      manualKeepaliveTimeout: 0,
      inboundTimeoutMs: 0,
      timerIntervalMs: 0,
    });
  });

  test('non-positive / non-finite timeouts are also treated as disabled', () => {
    expect(computeLivenessTimings(-1).enabled).toBe(false);
    expect(computeLivenessTimings(NaN).enabled).toBe(false);
  });

  test('derives timings from the default 60s wal_sender_timeout', () => {
    expect(computeLivenessTimings(60_000)).toEqual({
      enabled: true,
      manualKeepaliveTimeout: 45_000, // 75%
      inboundTimeoutMs: 120_000, // 2x
      timerIntervalMs: 9_000, // manualKeepaliveTimeout / 5
    });
  });

  test('any positive timeout yields positive, non-degenerate timings', () => {
    // A zero polling interval would busy-spin setInterval, and a zero inbound
    // threshold would tear the stream down immediately — the exact failure the
    // disabled-case guard exists to prevent. Neither may happen while enabled.
    for (const ms of [1_000, 10_000, 30_000, 60_000, 300_000]) {
      const t = computeLivenessTimings(ms);
      expect(t.enabled).toBe(true);
      expect(t.timerIntervalMs).toBeGreaterThan(0);
      expect(t.inboundTimeoutMs).toBeGreaterThan(0);
      expect(t.manualKeepaliveTimeout).toBeGreaterThan(0);
    }
  });
});
