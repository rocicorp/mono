import {expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {Acker, LagReporter} from './change-source.ts';

test('acker', () => {
  const sink = {push: vi.fn()};

  let acks = 0;

  const expectAck = (expected: bigint) => {
    expect(sink.push).toBeCalledTimes(++acks);
    expect(sink.push.mock.calls[acks - 1][0]).toBe(expected);
  };

  const expectNoAck = () => {
    expect(sink.push).toBeCalledTimes(acks);
  };

  const acker = new Acker(sink);

  acker.onChange(['status', {ack: false}, {watermark: '0a'}]);
  expectAck(10n);

  acker.onChange(['begin', {tag: 'begin'}, {commitWatermark: '0b'}]);
  acker.ack('0b');
  expectAck(11n);

  acker.onChange(['status', {ack: false}, {watermark: '0c'}]);
  expectAck(12n);

  acker.onChange(['begin', {tag: 'begin'}, {commitWatermark: '0d'}]);

  // This should be dropped because we are awaiting 0d
  acker.onChange(['status', {ack: false}, {watermark: '0e'}]);
  expectNoAck();

  // Now we are awaiting 0f
  acker.onChange(['status', {ack: true}, {watermark: '0f'}]);
  acker.ack('0d');
  expectAck(13n);

  // Still not caught up, so dropped
  acker.onChange(['status', {ack: false}, {watermark: '0g'}]);
  expectNoAck();

  // Downstream is now caught up.
  acker.ack('0f');
  expectAck(15n);

  // Now that downstream is caught up, this should respond
  acker.onChange(['status', {ack: false}, {watermark: '0h'}]);
  expectAck(17n);
});

test('lag reporter retries missing reports', async () => {
  vi.useFakeTimers();
  vi.setSystemTime(1_000);

  const dbMock = vi.fn((strings: TemplateStringsArray) => {
    if (strings.join('').includes('current_setting')) {
      return [{pgVersion: 170000}];
    }

    return [
      {
        commitTimeMs: Date.now(),
        lsn: `0/${dbMock.mock.calls.length.toString(16)}`,
      },
    ];
  });
  const db = dbMock as unknown as PostgresDB;

  const reporter = new LagReporter(
    createSilentLogContext(),
    {appID: 'test', shardNum: 0},
    db,
    10,
  );

  try {
    await expect(reporter.initiateLagReport()).resolves.toEqual({
      nextSendTimeMs: 1_000,
    });
    expect(dbMock).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(9);
    expect(dbMock).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(1);
    expect(dbMock).toHaveBeenCalledTimes(3);

    await vi.advanceTimersByTimeAsync(10);
    expect(dbMock).toHaveBeenCalledTimes(4);
  } finally {
    reporter.stop();
    vi.useRealTimers();
  }
});
