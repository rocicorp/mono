/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
