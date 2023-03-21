import {describe, test, expect} from '@jest/globals';
import {LogContext} from '@rocicorp/logger';
import {BufferSizer} from './buffer-sizer.js';

type Case = {
  name: string;
  initialBufferSizeMs: number;
  minBuferSizeMs: number;
  maxBufferSizeMs: number;
  offsets: [string, number][];
  missables: {missed: number; total: number};
  expectedBufferSizeMs: number;
};

describe('BufferSizer buffer adjustment', () => {
  const cases: Case[] = [
    {
      name: 'no adjustment when no offset and no missable records',
      initialBufferSizeMs: 250,
      minBuferSizeMs: 10,
      maxBufferSizeMs: 1000,
      offsets: [],
      missables: {missed: 0, total: 0},
      expectedBufferSizeMs: 250,
    },
    {
      name: 'no adjustment when offsets but no missable records',
      initialBufferSizeMs: 250,
      minBuferSizeMs: 10,
      maxBufferSizeMs: 1000,
      offsets: [
        ['c1', 6000],
        ['c1', 6250],
        ['c1', 6500],
      ],
      missables: {missed: 0, total: 0},
      expectedBufferSizeMs: 250,
    },
    {
      name: 'adjust up under max',
      initialBufferSizeMs: 250,
      minBuferSizeMs: 10,
      maxBufferSizeMs: 1000,
      offsets: [
        ['c1', 6000],
        ['c1', 6250],
        ['c1', 6500],
      ],
      missables: {missed: 4, total: 100},
      expectedBufferSizeMs: 500,
    },
    {
      name: 'adjust up capped at max',
      initialBufferSizeMs: 250,
      minBuferSizeMs: 10,
      maxBufferSizeMs: 1000,
      offsets: [
        ['c1', 6000],
        ['c1', 6250],
        ['c1', 6500],
        ['c1', 7500],
      ],
      missables: {missed: 4, total: 100},
      expectedBufferSizeMs: 1000,
    },
    {
      name: 'even if offsets high doesnt adjust up if low miss rate',
      initialBufferSizeMs: 250,
      minBuferSizeMs: 10,
      maxBufferSizeMs: 1000,
      offsets: [
        ['c1', 6000],
        ['c1', 6250],
        ['c1', 6500],
        ['c1', 7500],
      ],
      missables: {missed: 1, total: 100},
      expectedBufferSizeMs: 250,
    },
    {
      name: 'even if offsets low adjust up by 10% if high miss rate',
      initialBufferSizeMs: 250,
      minBuferSizeMs: 10,
      maxBufferSizeMs: 1000,
      offsets: [
        ['c1', 6000],
        ['c1', 6100],
        ['c1', 6150],
      ],
      missables: {missed: 4, total: 100},
      expectedBufferSizeMs: 275,
    },
    {
      name: 'multiple clients, adjusts up based on max offset difference',
      initialBufferSizeMs: 250,
      minBuferSizeMs: 10,
      maxBufferSizeMs: 1000,
      offsets: [
        ['c1', 6000],
        ['c2', -6000],
        ['c1', 6250],
        ['c2', -6200],
        ['c1', 6500],
        ['c2', -6700],
      ],
      missables: {missed: 4, total: 100},
      expectedBufferSizeMs: 700,
    },
    {
      name: 'adjust down above min',
      initialBufferSizeMs: 250,
      minBuferSizeMs: 10,
      maxBufferSizeMs: 1000,
      offsets: [
        ['c1', 6000],
        ['c1', 6020],
        ['c1', 6050],
      ],
      missables: {missed: 1, total: 500},
      expectedBufferSizeMs: 50,
    },
    {
      name: 'adjust down floored at min',
      initialBufferSizeMs: 250,
      minBuferSizeMs: 10,
      maxBufferSizeMs: 1000,
      offsets: [
        ['c1', 6000],
        ['c1', 6001],
        ['c1', 6005],
      ],
      missables: {missed: 1, total: 500},
      expectedBufferSizeMs: 10,
    },
    {
      name: 'even if offsets low doesnt adjust down if miss rate is not low enough',
      initialBufferSizeMs: 250,
      minBuferSizeMs: 10,
      maxBufferSizeMs: 1000,
      offsets: [
        ['c1', 6000],
        ['c1', 6020],
        ['c1', 6050],
      ],
      missables: {missed: 1, total: 100},
      expectedBufferSizeMs: 250,
    },
    {
      name: 'if miss rate is low, but offsets higher than current buffer, does not adjust down',
      initialBufferSizeMs: 250,
      minBuferSizeMs: 10,
      maxBufferSizeMs: 1000,
      offsets: [
        ['c1', 6000],
        ['c1', 6100],
        ['c1', 6300],
      ],
      missables: {missed: 1, total: 500},
      expectedBufferSizeMs: 250,
    },
    {
      name: 'doesnt adjust down if less than a 10% change',
      initialBufferSizeMs: 250,
      minBuferSizeMs: 10,
      maxBufferSizeMs: 1000,
      offsets: [
        ['c1', 6000],
        ['c1', 6200],
        ['c1', 6225],
      ],
      missables: {missed: 1, total: 100},
      expectedBufferSizeMs: 250,
    },
    {
      name: 'multiple clients, adjusts down based on max offset difference',
      initialBufferSizeMs: 250,
      minBuferSizeMs: 10,
      maxBufferSizeMs: 1000,
      offsets: [
        ['c1', 6000],
        ['c2', -6000],
        ['c1', 6020],
        ['c2', -6020],
        ['c1', 6050],
        ['c2', -6070],
      ],
      missables: {missed: 1, total: 500},
      expectedBufferSizeMs: 70,
    },
  ];

  for (const c of cases) {
    const adjustBufferSizeIntervalMs = 1000;
    test(c.name, () => {
      const bufferSizer = new BufferSizer({
        initialBufferSizeMs: c.initialBufferSizeMs,
        minBuferSizeMs: c.minBuferSizeMs,
        maxBufferSizeMs: c.maxBufferSizeMs,
        adjustBufferSizeIntervalMs,
      });

      expect(bufferSizer.bufferSizeMs).toEqual(c.initialBufferSizeMs);
      expect(
        bufferSizer.maybeAdjustBufferSize(0, new LogContext('error')),
      ).toEqual(false);
      expect(bufferSizer.bufferSizeMs).toEqual(c.initialBufferSizeMs);

      for (const [id, offset] of c.offsets) {
        bufferSizer.recordOffset(id, offset);
      }
      for (let i = 0; i < c.missables.total; i++) {
        bufferSizer.recordMissable(i < c.missables.missed);
      }

      expect(bufferSizer.bufferSizeMs).toEqual(c.initialBufferSizeMs);
      expect(
        bufferSizer.maybeAdjustBufferSize(
          adjustBufferSizeIntervalMs,
          new LogContext('error'),
        ),
      ).toEqual(c.initialBufferSizeMs !== c.expectedBufferSizeMs);
      expect(bufferSizer.bufferSizeMs).toEqual(c.expectedBufferSizeMs);
    });
  }
});

test('maybeAdjustBufferSize sequence adjustment every adjustBufferSizeIntervalMs and stats are reset on adjustment', () => {
  const adjustBufferSizeIntervalMs = 1000;
  const bufferSizer = new BufferSizer({
    initialBufferSizeMs: 250,
    minBuferSizeMs: 10,
    maxBufferSizeMs: 1000,
    adjustBufferSizeIntervalMs,
  });
  expect(bufferSizer.maybeAdjustBufferSize(0, new LogContext('error'))).toEqual(
    false,
  );
  expect(bufferSizer.bufferSizeMs).toEqual(250);

  bufferSizer.recordOffset('c1', 6000);
  bufferSizer.recordOffset('c1', 6200);
  bufferSizer.recordOffset('c1', 6500);

  bufferSizer.recordMissable(false);
  bufferSizer.recordMissable(false);
  bufferSizer.recordMissable(false);
  bufferSizer.recordMissable(true);

  expect(bufferSizer.bufferSizeMs).toEqual(250);
  expect(bufferSizer.maybeAdjustBufferSize(0, new LogContext('error'))).toEqual(
    false,
  );
  expect(bufferSizer.bufferSizeMs).toEqual(250);
  expect(
    bufferSizer.maybeAdjustBufferSize(
      adjustBufferSizeIntervalMs,
      new LogContext('error'),
    ),
  ).toEqual(true);
  expect(bufferSizer.bufferSizeMs).toEqual(500);

  bufferSizer.recordOffset('c1', 6000);
  bufferSizer.recordOffset('c1', 6200);
  bufferSizer.recordOffset('c1', 6700);

  // percent would still be enough to adjust up if stats were not reset
  bufferSizer.recordMissable(false);
  bufferSizer.recordMissable(false);
  bufferSizer.recordMissable(false);
  bufferSizer.recordMissable(false);

  expect(bufferSizer.bufferSizeMs).toEqual(500);
  expect(
    bufferSizer.maybeAdjustBufferSize(
      adjustBufferSizeIntervalMs,
      new LogContext('error'),
    ),
  ).toEqual(false);
  expect(bufferSizer.bufferSizeMs).toEqual(500);
  expect(
    bufferSizer.maybeAdjustBufferSize(
      adjustBufferSizeIntervalMs * 2,
      new LogContext('error'),
    ),
  ).toEqual(false);
  expect(bufferSizer.bufferSizeMs).toEqual(500);

  bufferSizer.recordOffset('c1', 6000);
  bufferSizer.recordOffset('c1', 6200);
  bufferSizer.recordOffset('c1', 6400);

  bufferSizer.recordMissable(false);
  bufferSizer.recordMissable(false);
  bufferSizer.recordMissable(false);
  bufferSizer.recordMissable(true);

  expect(bufferSizer.bufferSizeMs).toEqual(500);
  expect(
    bufferSizer.maybeAdjustBufferSize(
      adjustBufferSizeIntervalMs * 2,
      new LogContext('error'),
    ),
  ).toEqual(false);
  expect(bufferSizer.bufferSizeMs).toEqual(500);
  expect(
    bufferSizer.maybeAdjustBufferSize(
      adjustBufferSizeIntervalMs * 3,
      new LogContext('error'),
    ),
  ).toEqual(true);
  // 10% increase, because miss rate high, but max diff offset of 400 is less
  // than existing buffer size of 500
  expect(bufferSizer.bufferSizeMs).toEqual(550);

  bufferSizer.recordOffset('c1', 6000);
  bufferSizer.recordOffset('c1', 6200);
  bufferSizer.recordOffset('c1', 6800);

  // First record missable after buffer is adjusted is ignored
  bufferSizer.recordMissable(true);
  bufferSizer.recordMissable(false);
  bufferSizer.recordMissable(false);
  bufferSizer.recordMissable(false);

  expect(bufferSizer.bufferSizeMs).toEqual(550);
  expect(
    bufferSizer.maybeAdjustBufferSize(
      adjustBufferSizeIntervalMs * 3,
      new LogContext('error'),
    ),
  ).toEqual(false);
  expect(bufferSizer.bufferSizeMs).toEqual(550);
  expect(
    bufferSizer.maybeAdjustBufferSize(
      adjustBufferSizeIntervalMs * 4,
      new LogContext('error'),
    ),
  ).toEqual(false);
  // no change because first record missable after buffer is adjusted
  // is ignored, so miss percent is considered low but offsets are high
  expect(bufferSizer.bufferSizeMs).toEqual(550);
});
