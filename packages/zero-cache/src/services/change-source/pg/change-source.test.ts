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

  // Now we are awaiting 0f
  acker.onChange(['status', {ack: true}, {watermark: '0f'}]);
  acker.ack('0d');
  expectAck(13n);

  // Still not caught up, so dropped
  acker.onChange(['status', {ack: false}, {watermark: '0g'}]);

  // Downstream is now caught up.
  acker.ack('0f');
  expectAck(15n);

  // Now that downstream is caught up, this should respond
  acker.onChange(['status', {ack: false}, {watermark: '0h'}]);
  expectAck(17n);
});
