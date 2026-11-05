import {expect, test} from 'vitest';
import {ReconnectBackoff, STABLE_CONNECTION_MS} from './reconnect-backoff.ts';

const fixedRandom = () => 0.5;

function workingConnection(backoff: ReconnectBackoff, now = 0) {
  backoff.connected(now);
  backoff.receivedPoke();
  backoff.disconnected(now + STABLE_CONNECTION_MS);
}

test('a cold client keeps the initial retry interval', () => {
  const backoff = new ReconnectBackoff(5000);
  expect(backoff.nextDelay()).toBe(5000);
  expect(backoff.nextDelay()).toBe(5000);
});

test('useful stable service grants two jittered fast attempts then capped backoff', () => {
  const backoff = new ReconnectBackoff(5000);
  workingConnection(backoff);
  expect(
    Array.from({length: 8}, () =>
      backoff.nextDelay(undefined, false, fixedRandom),
    ),
  ).toEqual([125, 125, 3750, 7500, 15000, 22500, 22500, 22500]);
});

test.each([0, 0.999999])('jitter stays bounded (%s)', random => {
  const backoff = new ReconnectBackoff(5000);
  workingConnection(backoff);
  for (let i = 0; i < 2; i++) {
    const delay = backoff.nextDelay(undefined, false, () => random);
    expect(delay).toBeGreaterThanOrEqual(0);
    expect(delay).toBeLessThanOrEqual(250);
  }
  for (let i = 0; i < 50; i++) {
    const delay = backoff.nextDelay(undefined, false, () => random);
    expect(delay).toBeGreaterThanOrEqual(2500);
    expect(delay).toBeLessThanOrEqual(30_000);
  }
});

test.each(['handshake', 'short-useful', 'long-without-poke'])(
  '%s cannot reset an exhausted budget',
  kind => {
    const backoff = new ReconnectBackoff(5000);
    workingConnection(backoff);
    backoff.nextDelay();
    backoff.nextDelay();
    backoff.connected(100_000);
    if (kind === 'short-useful') backoff.receivedPoke();
    backoff.disconnected(
      100_000 + (kind === 'short-useful' ? 1 : STABLE_CONNECTION_MS),
    );
    expect(backoff.nextDelay(undefined, false, fixedRandom)).toBe(3750);
  },
);

test('another useful stable connection replenishes the allowance', () => {
  const backoff = new ReconnectBackoff(5000);
  workingConnection(backoff);
  for (let i = 0; i < 5; i++) backoff.nextDelay();
  workingConnection(backoff, 100_000);
  expect(backoff.nextDelay(undefined, false, fixedRandom)).toBe(125);
});

test('repeated handoff directives cannot retry quickly forever', () => {
  const backoff = new ReconnectBackoff(5000);
  for (let i = 0; i < 2; i++) {
    backoff.connected(i);
    backoff.disconnected(i);
    expect(backoff.nextDelay({maxBackoffMs: 0}, false, fixedRandom)).toBe(0);
  }
  expect(backoff.nextDelay({maxBackoffMs: 0}, false, fixedRandom)).toBe(3750);
});

test('overload bypasses the fast path and its minimum delay wins', () => {
  const backoff = new ReconnectBackoff(5000);
  workingConnection(backoff);
  expect(backoff.nextDelay(undefined, true, fixedRandom)).toBe(3750);
  expect(backoff.nextDelay({minBackoffMs: 60_000}, true, fixedRandom)).toBe(
    60_000,
  );
  expect(
    backoff.nextDelay(
      {minBackoffMs: 1000, maxBackoffMs: 2000},
      true,
      fixedRandom,
    ),
  ).toBe(2000);
  expect(backoff.nextDelay(undefined, false, fixedRandom)).toBe(125);
});
