import {expect, test} from 'vitest';
import {getInactiveDesiredQueries, type CVR} from './cvr.ts';

type QueryDef = {
  hash: string;
  ttl: number | undefined;
  inactivatedAt: number | undefined;
};

function makeCVR(clientID: string, queries: QueryDef[]): CVR {
  return {
    clients: {
      [clientID]: {
        desiredQueryIDs: queries.map(({hash}) => hash),
        id: clientID,
      },
    },
    id: 'abc123',
    lastActive: Date.UTC(2024, 1, 19),
    queries: Object.fromEntries(
      queries.map(({hash, ttl, inactivatedAt}) => [
        hash,
        {
          ast: {
            table: 'issues',
          },
          desiredBy: {
            [clientID]: {
              inactivatedAt,
              ttl,
              version: {
                minorVersion: 1,
                stateVersion: '1a9',
              },
            },
          },
          id: hash,
          patchVersion: undefined,
          transformationHash: undefined,
          transformationVersion: undefined,
        },
      ]),
    ),
    replicaVersion: '120',
    version: {
      stateVersion: '1aa',
    },
  };
}

test.each([
  {
    queries: [
      {hash: 'h1', ttl: 1000, inactivatedAt: 1000},
      {hash: 'h2', ttl: 1000, inactivatedAt: 2000},
      {hash: 'h3', ttl: 1000, inactivatedAt: 3000},
    ],
    expected: ['h1', 'h2', 'h3'],
  },
  {
    queries: [
      {hash: 'h1', ttl: 2000, inactivatedAt: 1000},
      {hash: 'h2', ttl: 1000, inactivatedAt: 1000},
      {hash: 'h3', ttl: 3000, inactivatedAt: 1000},
    ],
    expected: ['h2', 'h1', 'h3'],
  },
  {
    queries: [
      {hash: 'h1', ttl: undefined, inactivatedAt: 1000},
      {hash: 'h2', ttl: 2000, inactivatedAt: 1000},
      {hash: 'h3', ttl: undefined, inactivatedAt: 3000},
    ],
    expected: ['h2', 'h1', 'h3'],
  },
  {
    queries: [
      {hash: 'h1', ttl: 500, inactivatedAt: undefined},
      {hash: 'h2', ttl: undefined, inactivatedAt: undefined},
      {hash: 'h3', ttl: 1000, inactivatedAt: 500},
    ],
    expected: ['h3'],
  },
  {
    queries: [
      {hash: 'h1', ttl: 1000, inactivatedAt: 1000},
      {hash: 'h2', ttl: undefined, inactivatedAt: 2000},
      {hash: 'h3', ttl: undefined, inactivatedAt: undefined},
    ],
    expected: ['h1', 'h2'],
  },
])('getInactiveQueries %o', ({queries, expected}) => {
  const clientID = 'clientX';
  const cvr = makeCVR(clientID, queries);
  expect(getInactiveDesiredQueries(cvr, clientID)).toEqual(expected);
});
