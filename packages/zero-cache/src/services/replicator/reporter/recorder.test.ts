import type {ObservableResult} from '@opentelemetry/api';
import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {ReplicationReportRecorder} from './recorder.ts';

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
});
