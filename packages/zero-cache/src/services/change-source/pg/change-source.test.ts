import {expect, test, vi} from 'vitest';
import {Acker} from './change-source.ts';

test('acker', () => {
  const sink = {push: vi.fn()};

  let acks = 0;

  const expectAck = (expected: bigint) => {
    expect(sink.push).toBeCalledTimes(++acks);
    expect(sink.push.mock.calls[acks - 1][0]).toBe(expected);
  };

  const acker = new Acker(sink);

  acker.ackIfDownstreamIsCaughtUp('0a');
  expectAck(10n);

  acker.expectDownstreamAck('0b');
  acker.ack('0b');
  expectAck(11n);

  acker.ackIfDownstreamIsCaughtUp('0c');
  expectAck(12n);

  acker.expectDownstreamAck('0d');

  // This should be dropped because we are awaiting 0d
  acker.ackIfDownstreamIsCaughtUp('0e');

  // Now we are awaiting 0f
  acker.expectDownstreamAck('0f');
  acker.ack('0d');
  expectAck(13n);

  // Still not caught up, so dropped
  acker.ackIfDownstreamIsCaughtUp('0g');

  // Downstream is now caught up.
  acker.ack('0f');
  expectAck(15n);

  // Now that downstream is caught up, this should respond
  acker.ackIfDownstreamIsCaughtUp('0h');
  expectAck(17n);
});
