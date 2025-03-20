import {afterEach, beforeEach, expect, test, vi} from 'vitest';
import {Acker} from './change-source.ts';

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

test('acker', () => {
  const sink = {push: vi.fn()};

  let acks = 0;

  const expectAck = (expected: bigint) => {
    expect(sink.push).toBeCalledTimes(++acks);
    expect(sink.push.mock.calls[acks - 1][0]).toBe(expected);
  };

  const acker = new Acker(sink);

  acker.keepalive();
  acker.ack('0b');
  expectAck(11n);

  // Should be a no-op (i.e. no '0/0' sent).
  vi.advanceTimersToNextTimer();
  acker.ack('0d');
  expectAck(13n);

  // Keepalive ('0/0') is sent if no ack is sent before the timer fires.
  acker.keepalive();
  vi.advanceTimersToNextTimer();
  expectAck(0n);
});
