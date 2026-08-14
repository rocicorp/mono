import {describe, expect, test} from 'vitest';
import {computeLivenessTimings, evaluateInboundLiveness} from './stream.ts';

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

  test('a positive override replaces only the inbound threshold', () => {
    // An aggressive server-side wal_sender_timeout (e.g. CloudNativePG
    // defaults 5s cluster-wide) yields a 10s inbound fuse, which a busy wal
    // sender can trip while silently decoding — the override widens the
    // client-side threshold without touching the keepalive contract, which
    // genuinely belongs to the server setting.
    expect(computeLivenessTimings(5_000, 120_000)).toEqual({
      enabled: true,
      manualKeepaliveTimeout: 3_750, // still 75% of wal_sender_timeout
      inboundTimeoutMs: 120_000, // the override
      timerIntervalMs: 750, // still manualKeepaliveTimeout / 5
    });
  });

  test('non-positive / non-finite overrides fall back to 2x wal_sender_timeout', () => {
    for (const override of [0, -1, NaN]) {
      expect(computeLivenessTimings(60_000, override).inboundTimeoutMs).toBe(
        120_000,
      );
    }
    expect(computeLivenessTimings(60_000, undefined).inboundTimeoutMs).toBe(
      120_000,
    );
  });

  test('the override does not re-enable liveness when wal_sender_timeout is disabled', () => {
    // With wal_sender_timeout = 0 the server sends no periodic keepalives, so
    // inbound silence is normal on an idle healthy connection — an inbound
    // watchdog would false-positive regardless of its threshold.
    expect(computeLivenessTimings(0, 120_000).enabled).toBe(false);
  });
});

describe('evaluateInboundLiveness', () => {
  const inboundTimeoutMs = 120_000; // 2x a default 60s wal_sender_timeout

  // The whole point of the watchdog: when consumption has caught up (queue
  // empty) and we still haven't heard from upstream past the threshold, the
  // inbound half is dead and we must force a reconnect.
  test('times out when the queue is empty and the gap exceeds the threshold', () => {
    expect(
      evaluateInboundLiveness({
        sinceLastReceived: inboundTimeoutMs + 1,
        inboundTimeoutMs,
        queued: 0,
      }),
    ).toEqual({resetWindow: false, timedOut: true});
  });

  test('does not time out at or below the threshold with an empty queue', () => {
    expect(
      evaluateInboundLiveness({
        sinceLastReceived: inboundTimeoutMs, // exactly at threshold: not yet
        inboundTimeoutMs,
        queued: 0,
      }),
    ).toEqual({resetWindow: false, timedOut: false});
    expect(
      evaluateInboundLiveness({
        sinceLastReceived: inboundTimeoutMs - 1,
        inboundTimeoutMs,
        queued: 0,
      }),
    ).toEqual({resetWindow: false, timedOut: false});
  });

  // Steady-state back-pressure: a non-empty downstream queue means the silence
  // is our own flow control, not upstream death. Never time out; instead signal
  // the caller to refresh the inbound window.
  test('suppresses the timeout while messages are queued, even far past the threshold', () => {
    for (const queued of [1, 5, 100]) {
      expect(
        evaluateInboundLiveness({
          sinceLastReceived: inboundTimeoutMs * 10,
          inboundTimeoutMs,
          queued,
        }),
      ).toEqual({resetWindow: true, timedOut: false});
    }
  });

  // Regression test for the drain-transition race. During a long back-pressure
  // stall the queue stays non-empty, so every tick resets the window (below).
  // The tick that observes the queue finally drained to 0 must NOT immediately
  // fire against the pre-stall timestamp — the window was kept fresh, so
  // `sinceLastReceived` is small and the connection is granted a full
  // inboundTimeoutMs to deliver the next message.
  test('a non-empty queue resets the window so the post-drain tick has a fresh gap', () => {
    // While stalled, every tick sees queued > 0 and resets the window.
    const duringStall = evaluateInboundLiveness({
      sinceLastReceived: inboundTimeoutMs * 5,
      inboundTimeoutMs,
      queued: 6,
    });
    expect(duringStall.resetWindow).toBe(true);
    expect(duringStall.timedOut).toBe(false);

    // The first tick after the queue drains sees only the small gap accumulated
    // since the last (reset) tick — well under the threshold — so it holds.
    const justAfterDrain = evaluateInboundLiveness({
      sinceLastReceived: 200, // ~one polling interval since the window was reset
      inboundTimeoutMs,
      queued: 0,
    });
    expect(justAfterDrain).toEqual({resetWindow: false, timedOut: false});

    // Only if upstream then stays silent past the full threshold do we act.
    const stillSilent = evaluateInboundLiveness({
      sinceLastReceived: inboundTimeoutMs + 1,
      inboundTimeoutMs,
      queued: 0,
    });
    expect(stillSilent).toEqual({resetWindow: false, timedOut: true});
  });
});
