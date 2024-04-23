import {LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import {BufferSizer} from './buffer-sizer.js';

type Case = {
  name: string;
  initialBufferSizeMs: number;
  minBufferSizeMs: number;
  maxBufferSizeMs: number;
  missables: {missed: boolean; bufferNeededMs: number; count: number}[];
  expectedBufferSizeMs: number;
};

describe('BufferSizer buffer adjustment', () => {
  const cases: Case[] = [
    {
      name: 'adjust up to 99.5% (under max)',
      initialBufferSizeMs: 250,
      minBufferSizeMs: 10,
      maxBufferSizeMs: 1000,
      missables: [
        {missed: true, bufferNeededMs: 700, count: 1},
        {missed: true, bufferNeededMs: 600, count: 1},
        {missed: true, bufferNeededMs: 500, count: 3},
        {missed: false, bufferNeededMs: 300, count: 400 - 5},
      ],
      expectedBufferSizeMs: 600,
    },
    {
      name: 'adjust up to 99.5% (under max, more missables)',
      initialBufferSizeMs: 250,
      minBufferSizeMs: 10,
      maxBufferSizeMs: 1000,
      missables: [
        {missed: true, bufferNeededMs: 700, count: 1},
        {missed: true, bufferNeededMs: 650, count: 1},
        {missed: true, bufferNeededMs: 600, count: 1},
        {missed: true, bufferNeededMs: 550, count: 1},
        {missed: true, bufferNeededMs: 500, count: 5},
        {missed: false, bufferNeededMs: 300, count: 800 - 9},
      ],
      expectedBufferSizeMs: 550,
    },
    {
      name: 'adjust up capped at max',
      initialBufferSizeMs: 250,
      minBufferSizeMs: 10,
      maxBufferSizeMs: 1000,
      missables: [
        {missed: true, bufferNeededMs: 1100, count: 5},
        {missed: false, bufferNeededMs: 300, count: 400 - 5},
      ],
      expectedBufferSizeMs: 1000,
    },
    {
      name: 'even if bufferNeededMs high doesnt adjust up if low miss rate',
      initialBufferSizeMs: 250,
      minBufferSizeMs: 10,
      maxBufferSizeMs: 1000,
      missables: [
        {missed: true, bufferNeededMs: 1100, count: 2},
        {missed: false, bufferNeededMs: 300, count: 300 - 2},
      ],
      expectedBufferSizeMs: 250,
    },
    {
      name: 'if miss rate is high, but bufferNeededMs is lower than current buffer, does not adjust up',
      initialBufferSizeMs: 250,
      minBufferSizeMs: 10,
      maxBufferSizeMs: 1000,
      missables: [
        {missed: true, bufferNeededMs: 300, count: 1},
        {missed: true, bufferNeededMs: 240, count: 1},
        {missed: true, bufferNeededMs: 200, count: 10},
        {missed: false, bufferNeededMs: 100, count: 400 - 12},
      ],
      expectedBufferSizeMs: 250,
    },
    {
      name: 'adjust down to 99.5% (above min)',
      initialBufferSizeMs: 250,
      minBufferSizeMs: 10,
      maxBufferSizeMs: 1000,
      missables: [
        {missed: true, bufferNeededMs: 1100, count: 1},
        {missed: false, bufferNeededMs: 50, count: 1},
        {missed: false, bufferNeededMs: 40, count: 400 - 2},
      ],
      expectedBufferSizeMs: 50,
    },
    {
      name: 'adjust down floored at min',
      initialBufferSizeMs: 250,
      minBufferSizeMs: 10,
      maxBufferSizeMs: 1000,
      missables: [
        {missed: true, bufferNeededMs: 1100, count: 1},
        {missed: false, bufferNeededMs: 5, count: 1},
        {missed: false, bufferNeededMs: 1, count: 400 - 2},
      ],
      expectedBufferSizeMs: 10,
    },
    {
      name: 'even if bufferNeededMs low doesnt adjust down if miss rate is not low enough',
      initialBufferSizeMs: 250,
      minBufferSizeMs: 10,
      maxBufferSizeMs: 1000,
      missables: [
        {missed: true, bufferNeededMs: 1100, count: 1},
        {missed: true, bufferNeededMs: 50, count: 1},
        {missed: false, bufferNeededMs: 40, count: 400 - 2},
      ],
      expectedBufferSizeMs: 250,
    },
    {
      name: 'if miss rate is low, but bufferNeededMs is higher than current buffer, does not adjust down',
      initialBufferSizeMs: 250,
      minBufferSizeMs: 10,
      maxBufferSizeMs: 1000,
      missables: [
        {missed: true, bufferNeededMs: 1100, count: 1},
        {missed: false, bufferNeededMs: 300, count: 1},
        {missed: false, bufferNeededMs: 40, count: 400 - 2},
      ],
      expectedBufferSizeMs: 250,
    },
    {
      name: 'negative buffer sizes',
      initialBufferSizeMs: 250,
      minBufferSizeMs: -1000,
      maxBufferSizeMs: 1000,
      missables: [
        {missed: true, bufferNeededMs: 1100, count: 1},
        {missed: false, bufferNeededMs: -500, count: 1},
        {missed: false, bufferNeededMs: -700, count: 400 - 2},
      ],
      expectedBufferSizeMs: -500,
    },
    {
      name: 'does not update if less than 200 misseables recorded',
      initialBufferSizeMs: 250,
      minBufferSizeMs: 10,
      maxBufferSizeMs: 1000,
      missables: [
        {missed: true, bufferNeededMs: 700, count: 1},
        {missed: true, bufferNeededMs: 600, count: 1},
        {missed: true, bufferNeededMs: 500, count: 10},
        {missed: false, bufferNeededMs: 300, count: 100 - 12},
      ],
      expectedBufferSizeMs: 250,
    },
  ];

  const startTime = 1680548665012;
  const adjustBufferSizeIntervalMs = 1000;
  for (const c of cases) {
    test(c.name, () => {
      const bufferSizer = new BufferSizer({
        initialBufferSizeMs: c.initialBufferSizeMs,
        minBufferSizeMs: c.minBufferSizeMs,
        maxBufferSizeMs: c.maxBufferSizeMs,
        adjustBufferSizeIntervalMs,
      });

      expect(bufferSizer.bufferSizeMs).toEqual(c.initialBufferSizeMs);
      recordMissables(
        bufferSizer,
        c.missables,
        startTime,
        startTime + adjustBufferSizeIntervalMs,
      );
      expect(bufferSizer.bufferSizeMs).toEqual(c.expectedBufferSizeMs);
    });
  }
});

test('sequence of recordMissable adjusts every adjustBufferSizeIntervalMs and stats are reset on adjustment and first after adjustment is ignored', () => {
  const startTime = 1680548665012;
  const adjustBufferSizeIntervalMs = 1000;
  const bufferSizer = new BufferSizer({
    initialBufferSizeMs: 250,
    minBufferSizeMs: 10,
    maxBufferSizeMs: 1000,
    adjustBufferSizeIntervalMs,
  });
  expect(bufferSizer.bufferSizeMs).toEqual(250);

  recordMissables(
    bufferSizer,
    [
      {missed: true, bufferNeededMs: 700, count: 1},
      {missed: true, bufferNeededMs: 600, count: 1},
      {missed: true, bufferNeededMs: 500, count: 10},
      // -1 is for the missable recorded below
      // at startTime + adjustBufferSizeIntervalMs
      {missed: false, bufferNeededMs: 400, count: 400 - 12 - 1},
    ],
    startTime,
  );

  expect(bufferSizer.bufferSizeMs).toEqual(250);

  recordMissables(
    bufferSizer,
    [{missed: false, bufferNeededMs: 300, count: 1}],
    startTime,
    startTime + adjustBufferSizeIntervalMs,
  );
  expect(bufferSizer.bufferSizeMs).toEqual(600);

  recordMissables(
    bufferSizer,
    [
      {missed: true, bufferNeededMs: 1500, count: 1}, // first after adjustment ignored
      {missed: true, bufferNeededMs: 1100, count: 1},
      {missed: false, bufferNeededMs: 50, count: 1},
      // -1 is for the missable recorded below
      // at startTime + adjustBufferSizeIntervalMs * 2
      {missed: false, bufferNeededMs: 40, count: 400 - 2 - 1},
    ],
    startTime + adjustBufferSizeIntervalMs,
  );
  expect(bufferSizer.bufferSizeMs).toEqual(600);
  recordMissables(
    bufferSizer,
    [{missed: false, bufferNeededMs: 40, count: 1}],
    startTime + adjustBufferSizeIntervalMs,
    startTime + adjustBufferSizeIntervalMs * 2,
  );
  expect(bufferSizer.bufferSizeMs).toEqual(50);
});

function recordMissables(
  bufferSizer: BufferSizer,
  missables: {missed: boolean; bufferNeededMs: number; count: number}[],
  startTime: number,
  timeOfLastRecord = startTime,
) {
  for (let i = 0; i < missables.length; i++) {
    const {missed, count, bufferNeededMs} = missables[i];
    for (let j = 0; j < count; j++) {
      const now =
        i === missables.length - 1 && j === count - 1
          ? timeOfLastRecord
          : startTime;
      bufferSizer.recordMissable(
        now,
        missed,
        bufferNeededMs,
        new LogContext('error'),
      );
    }
  }
}
