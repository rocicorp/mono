import type {LogicalReplicationService} from 'pg-logical-replication';
import {expect, test, vi} from 'vitest';
import {Acker} from './change-source.js';

test('acker', () => {
  const service = {acknowledge: vi.fn()};

  let acks = 0;

  const expectAck = (expected: string) => {
    expect(service.acknowledge).toBeCalledTimes(++acks);
    expect(service.acknowledge.mock.calls[acks - 1][0]).toBe(expected);
  };

  const acker = new Acker(
    service as unknown as LogicalReplicationService,
    '0a',
  );

  acker.onHeartbeat('0/1', true);
  expectAck('0/A');

  acker.onData('0/B');
  acker.onHeartbeat('0/C', true);
  expectAck('0/A'); // Outstanding data is unacked.

  acker.onAck('0b');
  expectAck('0/B'); // Now the data is acked.

  acker.onHeartbeat('0/D', true);
  expectAck('0/D'); // If the data is acked, heartbeats move acks forward.
});
