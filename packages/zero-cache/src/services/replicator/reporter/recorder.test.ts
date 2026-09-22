import type {ObservableResult} from '@opentelemetry/api';
import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {
  estimateUpstreamClockSkewMs,
  ReplicationReportRecorder,
} from './recorder.ts';

test('replication report recorder', () => {
  const recorder = new ReplicationReportRecorder(createSilentLogContext());

  recorder.record({
    lastTimings: {
      sendTimeMs: 1000,
      commitTimeMs: 1100,
      receiveTimeMs: 1200,
      replicateTimeMs: 1550,
    },
    nextSendTimeMs: 11_000,
  });

  function expectObserved(
    observer: (o: ObservableResult) => void,
    expected: number | undefined,
  ) {
    let observed: number | undefined;
    observer({observe: v => (observed = v)});
    expect(observed).toBe(expected);
  }

  expectObserved(recorder.reportUpstreamLag, 200);
  expectObserved(recorder.reportReplicaLag, 350);
  expectObserved(recorder.reportTotalLag, 550);
  expectObserved(recorder.reportLastTotalLag, 550);

  expectObserved(recorder.reportUpstreamLag, 200);
  expectObserved(recorder.reportReplicaLag, 350);
  expectObserved(recorder.reportTotalLag, 550);
  expectObserved(recorder.reportLastTotalLag, 550);

  expectObserved(recorder.reportUpstreamLag, 200);
  expectObserved(recorder.reportReplicaLag, 350);
  expectObserved(recorder.reportTotalLag, 550);
  expectObserved(recorder.reportLastTotalLag, 550);

  expectObserved(recorder.reportUpstreamLag, 200);
  expectObserved(recorder.reportReplicaLag, 350);
  expectObserved(recorder.reportTotalLag, 550);
  expectObserved(recorder.reportLastTotalLag, 550);

  recorder.record({
    lastTimings: {
      sendTimeMs: 11_000,
      commitTimeMs: 11_123,
      receiveTimeMs: 11_250,
      replicateTimeMs: 11_650,
    },
    nextSendTimeMs: 21_000,
  });

  expectObserved(recorder.reportUpstreamLag, 250);
  expectObserved(recorder.reportReplicaLag, 400);
  expectObserved(recorder.reportTotalLag, 650);
  expectObserved(recorder.reportLastTotalLag, 650);
  // commit at 11_123 vs a round-trip midpoint of 11_125.
  expectObserved(recorder.reportUpstreamClockSkew, -2);
});

test('replication report recorder ignores pending report without timings', () => {
  const recorder = new ReplicationReportRecorder(createSilentLogContext());

  recorder.record({
    nextSendTimeMs: 1_000,
  });

  function expectObserved(
    observer: (o: ObservableResult) => void,
    expected: number | undefined,
  ) {
    let observed: number | undefined;
    observer({observe: v => (observed = v)});
    expect(observed).toBe(expected);
  }

  expectObserved(recorder.reportUpstreamLag, undefined);
  expectObserved(recorder.reportReplicaLag, undefined);
  expectObserved(recorder.reportTotalLag, undefined);
  expectObserved(recorder.reportLastTotalLag, undefined);
  expectObserved(recorder.reportUpstreamClockSkew, undefined);
});

describe('estimateUpstreamClockSkewMs', () => {
  test('a commit landing at the round-trip midpoint reads as no skew', () => {
    expect(
      estimateUpstreamClockSkewMs({
        sendTimeMs: 1_000,
        commitTimeMs: 1_100,
        receiveTimeMs: 1_200,
      }),
    ).toBe(0);
  });

  test('an upstream clock running ahead reads positive', () => {
    // This is the direction that biases sync.e2e_serving_lag *low*, so it must
    // not be mistaken for a healthy pipeline.
    expect(
      estimateUpstreamClockSkewMs({
        sendTimeMs: 1_000,
        commitTimeMs: 31_100,
        receiveTimeMs: 1_200,
      }),
    ).toBe(30_000);
  });

  test('an upstream clock running behind reads negative', () => {
    expect(
      estimateUpstreamClockSkewMs({
        sendTimeMs: 1_000,
        commitTimeMs: -28_900,
        receiveTimeMs: 1_200,
      }),
    ).toBe(-30_000);
  });

  test('a commit outside the round trip proves skew regardless of latency', () => {
    // The commit genuinely happened between send and receive, so a commit
    // timestamp before send can only be clock offset. The estimate stays
    // conservative: it reports at least half the amount by which the commit
    // falls outside the window.
    const skew = estimateUpstreamClockSkewMs({
      sendTimeMs: 1_000,
      commitTimeMs: 900,
      receiveTimeMs: 1_200,
    });
    expect(skew).toBeLessThan(0);
    expect(skew).toBe(-200);
  });
});
