import {expect, test} from 'vitest';
import {getInactiveQueries, type CVR} from './cvr.ts';
import type {ClientQueryRecord} from './schema/types.ts';
import {ttlClockFromNumber, type TTLClock} from './ttl-clock.ts';

type QueryDef = {
  hash: string;
  ttl: number;
  inactivatedAt: TTLClock | undefined;
};

function makeCVR(clients: Record<string, QueryDef[]>): CVR {
  const cvr: CVR = {
    clients: Object.fromEntries(
      Object.entries(clients).map(([clientID, queries]) => [
        clientID,
        {
          desiredQueryIDs: queries.map(({hash}) => hash),
          id: clientID,
        },
      ]),
    ),
    id: 'abc123',
    lastActive: Date.UTC(2024, 1, 20),
    ttlClock: ttlClockFromNumber(Date.UTC(2024, 1, 20)),
    queries: {},
    replicaVersion: '120',
    version: {
      stateVersion: '1aa',
    },
    clientSchema: null,
    profileID: null,
  };

  for (const [clientID, queries] of Object.entries(clients)) {
    for (const {hash, ttl, inactivatedAt} of queries) {
      cvr.queries[hash] ??= {
        ast: {
          table: 'issues',
        },
        type: 'client',
        clientState: {},
        id: hash,
        patchVersion: undefined,
        transformationHash: undefined,
        transformationVersion: undefined,
      };
      (cvr.queries[hash] as ClientQueryRecord).clientState[clientID] = {
        inactivatedAt,
        ttl,
        version: {
          configVersion: 1,
          stateVersion: '1a9',
        },
      };
    }
  }

  return cvr;
}

const minutes = (n: number) => n * 60 * 1000;

test.each([
  {
    clients: {
      clientX: [
        {hash: 'h1', ttl: 1000, inactivatedAt: ttlClockFromNumber(1000)},
        {hash: 'h2', ttl: 1000, inactivatedAt: ttlClockFromNumber(2000)},
        {hash: 'h3', ttl: 1000, inactivatedAt: ttlClockFromNumber(3000)},
      ],
    },
    expected: [
      {hash: 'h1', ttl: 1000, inactivatedAt: ttlClockFromNumber(1000)},
      {hash: 'h2', ttl: 1000, inactivatedAt: ttlClockFromNumber(2000)},
      {hash: 'h3', ttl: 1000, inactivatedAt: ttlClockFromNumber(3000)},
    ],
  },
  {
    clients: {
      clientX: [
        {hash: 'h1', ttl: 2000, inactivatedAt: ttlClockFromNumber(1000)},
        {hash: 'h2', ttl: 1000, inactivatedAt: ttlClockFromNumber(1000)},
        {hash: 'h3', ttl: 3000, inactivatedAt: ttlClockFromNumber(1000)},
      ],
    },
    expected: [
      {hash: 'h2', ttl: 1000, inactivatedAt: ttlClockFromNumber(1000)},
      {hash: 'h1', ttl: 2000, inactivatedAt: ttlClockFromNumber(1000)},
      {hash: 'h3', ttl: 3000, inactivatedAt: ttlClockFromNumber(1000)},
    ],
  },
  {
    clients: {
      clientX: [
        {hash: 'h1', ttl: -1, inactivatedAt: ttlClockFromNumber(1000)},
        {hash: 'h2', ttl: 2000, inactivatedAt: ttlClockFromNumber(1000)},
        {hash: 'h3', ttl: -1, inactivatedAt: ttlClockFromNumber(3000)},
      ],
    },
    expected: [
      {hash: 'h2', ttl: 2000, inactivatedAt: ttlClockFromNumber(1000)},
      {hash: 'h1', ttl: minutes(10), inactivatedAt: ttlClockFromNumber(1000)},
      {hash: 'h3', ttl: minutes(10), inactivatedAt: ttlClockFromNumber(3000)},
    ],
  },
  {
    clients: {
      clientX: [
        {hash: 'h1', ttl: 500, inactivatedAt: undefined},
        {hash: 'h2', ttl: -1, inactivatedAt: undefined},
        {hash: 'h3', ttl: 1000, inactivatedAt: ttlClockFromNumber(500)},
      ],
    },
    expected: [{hash: 'h3', ttl: 1000, inactivatedAt: ttlClockFromNumber(500)}],
  },
  {
    clients: {
      clientX: [
        {hash: 'h1', ttl: 1000, inactivatedAt: ttlClockFromNumber(1000)},
        {hash: 'h2', ttl: -1, inactivatedAt: ttlClockFromNumber(2000)},
        {hash: 'h3', ttl: -1, inactivatedAt: undefined},
      ],
    },
    expected: [
      {hash: 'h1', ttl: 1000, inactivatedAt: 1000},
      {hash: 'h2', ttl: minutes(10), inactivatedAt: ttlClockFromNumber(2000)},
    ],
  },

  // Multiple clients
  {
    clients: {
      clientX: [
        {hash: 'h1', ttl: 1000, inactivatedAt: ttlClockFromNumber(1000)},
        {hash: 'h2', ttl: 1000, inactivatedAt: ttlClockFromNumber(2000)},
      ],
      clientY: [
        {hash: 'h3', ttl: 1000, inactivatedAt: ttlClockFromNumber(3000)},
        {hash: 'h4', ttl: 1000, inactivatedAt: ttlClockFromNumber(4000)},
      ],
    },
    expected: [
      {hash: 'h1', ttl: 1000, inactivatedAt: ttlClockFromNumber(1000)},
      {hash: 'h2', ttl: 1000, inactivatedAt: ttlClockFromNumber(2000)},
      {hash: 'h3', ttl: 1000, inactivatedAt: ttlClockFromNumber(3000)},
      {hash: 'h4', ttl: 1000, inactivatedAt: ttlClockFromNumber(4000)},
    ],
  },

  // When multiple clients have the same query, the query that expires last should be used
  {
    clients: {
      clientX: [
        {hash: 'h1', ttl: 1000, inactivatedAt: ttlClockFromNumber(1000)},
        {hash: 'h2', ttl: 1000, inactivatedAt: ttlClockFromNumber(2000)},
        {hash: 'h3', ttl: 1000, inactivatedAt: ttlClockFromNumber(3000)},
      ],
      clientY: [
        {hash: 'h1', ttl: 1000, inactivatedAt: ttlClockFromNumber(6000)},
        {hash: 'h2', ttl: 1000, inactivatedAt: ttlClockFromNumber(5000)},
        {hash: 'h3', ttl: 1000, inactivatedAt: ttlClockFromNumber(4000)},
      ],
    },
    expected: [
      {hash: 'h3', ttl: 1000, inactivatedAt: ttlClockFromNumber(4000)},
      {hash: 'h2', ttl: 1000, inactivatedAt: ttlClockFromNumber(5000)},
      {hash: 'h1', ttl: 1000, inactivatedAt: ttlClockFromNumber(6000)},
    ],
  },

  {
    clients: {
      clientX: [
        {hash: 'h1', ttl: 1000, inactivatedAt: ttlClockFromNumber(1000)},
        {hash: 'h2', ttl: 1000, inactivatedAt: ttlClockFromNumber(2000)},
      ],
      clientY: [
        {hash: 'h1', ttl: 500, inactivatedAt: ttlClockFromNumber(1500)},
        {hash: 'h2', ttl: 1500, inactivatedAt: ttlClockFromNumber(1500)},
      ],
    },
    expected: [
      {hash: 'h1', ttl: 1000, inactivatedAt: ttlClockFromNumber(1000)},
      {hash: 'h2', ttl: 1000, inactivatedAt: ttlClockFromNumber(2000)},
    ],
  },

  {
    clients: {
      clientX: [
        {hash: 'h1', ttl: 2000, inactivatedAt: ttlClockFromNumber(1000)},
        {hash: 'h2', ttl: 1000, inactivatedAt: ttlClockFromNumber(3000)},
      ],
      clientY: [
        {hash: 'h1', ttl: 3000, inactivatedAt: ttlClockFromNumber(2000)},
        {hash: 'h2', ttl: -1, inactivatedAt: ttlClockFromNumber(4000)},
      ],
    },
    expected: [
      {hash: 'h1', ttl: 3000, inactivatedAt: ttlClockFromNumber(2000)},
      {hash: 'h2', ttl: minutes(10), inactivatedAt: ttlClockFromNumber(4000)},
    ],
  },
  {
    clients: {
      clientX: [
        {hash: 'h1', ttl: 1000, inactivatedAt: ttlClockFromNumber(1000)},
        {hash: 'h2', ttl: -1, inactivatedAt: ttlClockFromNumber(2000)},
      ],
      clientY: [
        {hash: 'h1', ttl: -1, inactivatedAt: ttlClockFromNumber(3000)},
        {hash: 'h2', ttl: 2000, inactivatedAt: ttlClockFromNumber(1500)},
      ],
    },
    expected: [
      {hash: 'h2', ttl: minutes(10), inactivatedAt: ttlClockFromNumber(2000)},
      {hash: 'h1', ttl: minutes(10), inactivatedAt: ttlClockFromNumber(3000)},
    ],
  },
  {
    clients: {
      clientX: [
        {hash: 'h1', ttl: 1000, inactivatedAt: undefined},
        {hash: 'h2', ttl: 2000, inactivatedAt: ttlClockFromNumber(1000)},
      ],
      clientY: [
        {hash: 'h1', ttl: -1, inactivatedAt: ttlClockFromNumber(2000)},
        {hash: 'h2', ttl: -1, inactivatedAt: undefined},
      ],
    },
    expected: [],
  },
])('getInactiveQueries %o', ({clients, expected}) => {
  const cvr = makeCVR(clients);
  expect(getInactiveQueries(cvr)).toEqual(expected);
});
