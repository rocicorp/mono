import {expect, test} from 'vitest';
import {c9ResourceFindings} from './chaos.ts';

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
