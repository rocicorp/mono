import {describe, expect, test} from 'vitest';
import {E2EServingLagTracker} from './e2e-serving-lag.ts';

describe('view-syncer/e2e-serving-lag', () => {
  function ready(watermark: string, upstreamCommitTimeMs?: number) {
    return {
      state: 'version-ready',
      watermark,
      upstreamCommitTimeMs,
    } as const;
  }

  test('records lag from upstream commit to serve', () => {
    const tracker = new E2EServingLagTracker();

    tracker.onVersionReady(ready('02', 1_000));
    expect(tracker.onVersionServed('02', 1_350)).toEqual({
      lagMs: 350,
      clamped: false,
    });
    // The pending commit is consumed, so serving again records nothing.
    expect(tracker.onVersionServed('02', 1_400)).toBeNull();
  });

  test('serving a later version still covers the pending commit', () => {
    const tracker = new E2EServingLagTracker();

    tracker.onVersionReady(ready('02', 1_000));
    expect(tracker.onVersionServed('05', 1_500)).toEqual({
      lagMs: 500,
      clamped: false,
    });
  });

  test('serving an earlier version does not record', () => {
    const tracker = new E2EServingLagTracker();

    tracker.onVersionReady(ready('05', 1_000));
    expect(tracker.onVersionServed('02', 1_500)).toBeNull();
    // Still outstanding: it is recorded once the version is actually served.
    expect(tracker.pending).toEqual({watermark: '05', commitTimeMs: 1_000});
    expect(tracker.onVersionServed('05', 1_800)).toEqual({
      lagMs: 800,
      clamped: false,
    });
  });

  test('coalesced notifications measure the oldest subsumed commit', () => {
    const tracker = new E2EServingLagTracker();

    // Three commits arrive before the ViewSyncer gets to serve any of them.
    tracker.onVersionReady(ready('02', 1_000));
    tracker.onVersionReady(ready('03', 1_100));
    tracker.onVersionReady(ready('04', 1_250));

    // The reported lag is that of the oldest, which is the worst latency any
    // client actually experienced.
    expect(tracker.onVersionServed('04', 2_000)).toEqual({
      lagMs: 1_000,
      clamped: false,
    });
  });

  test('notifications without a commit time are ignored', () => {
    const tracker = new E2EServingLagTracker();

    tracker.onVersionReady(ready('02', undefined));
    expect(tracker.pending).toBeNull();
    expect(tracker.onVersionServed('02', 1_500)).toBeNull();

    // A ChangeSource reporting no commit time must not clear one that a
    // previous notification did carry.
    tracker.onVersionReady(ready('03', 1_000));
    tracker.onVersionReady(ready('04', undefined));
    expect(tracker.onVersionServed('04', 1_600)).toEqual({
      lagMs: 600,
      clamped: false,
    });
  });

  test('the initial version-ready notification is ignored', () => {
    const tracker = new E2EServingLagTracker();

    // Sent on startup with no watermark; it represents already-current state,
    // not newly replicated work.
    tracker.onVersionReady({state: 'version-ready'});
    expect(tracker.pending).toBeNull();
  });

  test('clock skew is clamped and reported rather than silently swallowed', () => {
    const tracker = new E2EServingLagTracker();

    // The upstream commit time comes from the upstream database's clock, which
    // may run ahead of the ViewSyncer's. A negative duration would corrupt the
    // histogram's sum, but it must not vanish either: skew in this direction
    // biases the metric low, which reads as healthy serving.
    tracker.onVersionReady(ready('02', 5_000));
    expect(tracker.onVersionServed('02', 4_000)).toEqual({
      lagMs: 0,
      clamped: true,
    });
  });

  test('a genuinely zero lag is not reported as a clamp', () => {
    const tracker = new E2EServingLagTracker();

    tracker.onVersionReady(ready('02', 1_000));
    expect(tracker.onVersionServed('02', 1_000)).toEqual({
      lagMs: 0,
      clamped: false,
    });
  });
});
