import {expect, test} from 'vitest';
import type {
  ClientQueryRecord,
  CustomQueryRecord,
  InternalQueryRecord,
} from './schema/types.ts';
import {ttlClockFromNumber, type TTLClock} from './ttl-clock.ts';
import {expired} from './view-syncer.ts';

const NOW = ttlClockFromNumber(10_000);

function clientState(inactivatedAt: TTLClock | undefined, ttl = 1_000) {
  return {inactivatedAt, ttl, version: {stateVersion: '1a9', configVersion: 1}};
}

function clientQuery(
  clientState: ClientQueryRecord['clientState'],
): ClientQueryRecord {
  return {
    id: 'q',
    type: 'client',
    ast: {table: 'issues'},
    clientState,
  };
}

test('an internal query is never expired', () => {
  const internal: InternalQueryRecord = {
    id: 'internal',
    type: 'internal',
    ast: {table: 'clients'},
  };
  expect(expired(NOW, internal)).toBe(false);
});

test('a query with an active client is not expired', () => {
  expect(expired(NOW, clientQuery({a: clientState(undefined)}))).toBe(false);
  // Active for one client is active for the whole client group.
  expect(
    expired(
      NOW,
      clientQuery({
        a: clientState(undefined),
        b: clientState(ttlClockFromNumber(0)),
      }),
    ),
  ).toBe(false);
});

test('a query is expired only once every client TTL has elapsed', () => {
  // inactivatedAt 9_500 + ttl 1_000 = 10_500 > 10_000.
  expect(
    expired(NOW, clientQuery({a: clientState(ttlClockFromNumber(9_500))})),
  ).toBe(false);
  // inactivatedAt 8_000 + ttl 1_000 = 9_000 <= 10_000.
  expect(
    expired(NOW, clientQuery({a: clientState(ttlClockFromNumber(8_000))})),
  ).toBe(true);
  // The latest client expiration wins.
  expect(
    expired(
      NOW,
      clientQuery({
        a: clientState(ttlClockFromNumber(8_000)),
        b: clientState(ttlClockFromNumber(9_500)),
      }),
    ),
  ).toBe(false);
});

test('an ownerless query is expired', () => {
  // A `clear` desired-queries patch hard-deletes the client's state, which can
  // leave a query with no client state at all. Such a query has no owner and
  // therefore no TTL to wait out; reporting it as unexpired would leak the
  // query record and its pipeline forever.
  expect(expired(NOW, clientQuery({}))).toBe(true);

  const custom: CustomQueryRecord = {
    id: 'custom',
    type: 'custom',
    name: 'named',
    args: [],
    clientState: {},
  };
  expect(expired(NOW, custom)).toBe(true);
});
