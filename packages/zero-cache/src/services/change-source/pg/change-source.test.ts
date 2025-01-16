import type {LogicalReplicationService} from 'pg-logical-replication';
import {afterEach, beforeEach, expect, test, vi} from 'vitest';
import {Acker} from './change-source.js';

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

test('acker', () => {
  const service = {acknowledge: vi.fn()};

  let acks = 0;

  const expectAck = (expected: string) => {
    expect(service.acknowledge).toBeCalledTimes(++acks);
    expect(service.acknowledge.mock.calls[acks - 1][0]).toBe(expected);
  };

  const acker = new Acker(service as unknown as LogicalReplicationService);

  acker.keepalive();
  acker.ack('0b');
  expectAck('0/B');

  // Should be a no-op (i.e. no '0/0' sent).
  vi.advanceTimersToNextTimer();
  acker.ack('0d');
  expectAck('0/D');

  // Keepalive ('0/0') is sent if no ack is sent before the timer fires.
  acker.keepalive();
  vi.advanceTimersToNextTimer();
  expectAck('0/0');
});
