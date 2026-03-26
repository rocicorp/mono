import type {ObservableResult} from '@opentelemetry/api';
import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {ReplicationReportRecorder} from './recorder.ts';

test('replication report recorder', () => {
  let now: number = 10_000;

  const recorder = new ReplicationReportRecorder(
    createSilentLogContext(),
    () => now,
  );

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
    expected: number,
  ) {
    let observed: number | undefined;
    observer({observe: v => (observed = v)});
    expect(observed).toBe(expected);
  }

  expectObserved(recorder.reportUpstreamLag, 200);
  expectObserved(recorder.reportReplicaLag, 350);
  expectObserved(recorder.reportTotalLag, 550);

  now = 11_550;

  expectObserved(recorder.reportUpstreamLag, 200);
  expectObserved(recorder.reportReplicaLag, 350);
  expectObserved(recorder.reportTotalLag, 550);

  now = 12_123;
  expectObserved(recorder.reportUpstreamLag, 200);
  expectObserved(recorder.reportReplicaLag, 350);
  expectObserved(recorder.reportTotalLag, 1123);

  now = 12_246;
  expectObserved(recorder.reportUpstreamLag, 200);
  expectObserved(recorder.reportReplicaLag, 350);
  expectObserved(recorder.reportTotalLag, 1246);

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
});
