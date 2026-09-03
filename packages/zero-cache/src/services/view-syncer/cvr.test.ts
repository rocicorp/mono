import {expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {CVRStore} from './cvr-store.ts';
import {
  classifyQueriesForHydration,
  CVRQueryDrivenUpdater,
  getInactiveQueries,
  type CVR,
} from './cvr.ts';
import type {
  ClientQueryRecord,
  InternalQueryRecord,
  QueryRecord,
  RowRecord,
} from './schema/types.ts';
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

function clientQuery(
  id: string,
  states: ClientQueryRecord['clientState'],
): ClientQueryRecord {
  return {
    id,
    type: 'client',
    ast: {table: 'issues'},
    clientState: states,
  };
}

function clientState(inactivatedAt: TTLClock | undefined, ttl = 1_000) {
  return {
    inactivatedAt,
    ttl,
    version: {stateVersion: '1a9', configVersion: 1},
  };
}

test('classifyQueriesForHydration classifies active and internal queries as required', () => {
  const internal: InternalQueryRecord = {
    id: 'internal',
    type: 'internal',
    ast: {table: 'clients'},
  };
  const active = clientQuery('active', {
    a: clientState(undefined),
  });
  const partlyActive = clientQuery('partly-active', {
    a: clientState(ttlClockFromNumber(1_000)),
    b: clientState(undefined),
  });

  const {required, optional} = classifyQueriesForHydration([
    internal,
    active,
    partlyActive,
  ]);

  expect(required.map(q => q.id)).toEqual([
    'internal',
    'active',
    'partly-active',
  ]);
  expect(optional).toEqual([]);
});

test('classifyQueriesForHydration orders inactive queries by descending effective expiration', () => {
  const queries: QueryRecord[] = [
    clientQuery('ownerless', {}),
    clientQuery('early', {
      a: clientState(ttlClockFromNumber(1_000), 1_000),
    }),
    clientQuery('latest-from-second-client', {
      a: clientState(ttlClockFromNumber(1_000), 1_000),
      b: clientState(ttlClockFromNumber(4_000), 1_000),
    }),
    clientQuery('middle', {
      a: clientState(ttlClockFromNumber(2_000), 1_000),
      b: clientState(ttlClockFromNumber(1_000), 1_000),
    }),
  ];

  const {required, optional} = classifyQueriesForHydration(queries);

  expect(required).toEqual([]);
  expect(optional.map(q => q.id)).toEqual([
    'latest-from-second-client',
    'middle',
    'early',
    'ownerless',
  ]);
});

function makeQueryUpdater(query: QueryRecord, rows: RowRecord[] = []) {
  const cvr = makeCVR({});
  cvr.queries[query.id] = query;
  const store = {
    getRowRecords: vi.fn(() =>
      Promise.resolve(new Map(rows.map(row => [row.id, row]))),
    ),
    markQueryAsDeleted: vi.fn(),
    putRowRecord: vi.fn(),
    updateQuery: vi.fn(),
  } as unknown as CVRStore;
  const updater = new CVRQueryDrivenUpdater(
    store,
    cvr,
    cvr.version.stateVersion,
    cvr.replicaVersion!,
  );
  updater.ensureNewVersion();
  return {store, updater};
}

function gottenInactiveQuery(id = 'inactive'): ClientQueryRecord {
  return {
    ...clientQuery(id, {
      client: clientState(ttlClockFromNumber(1_000)),
    }),
    patchVersion: {stateVersion: '1aa'},
    transformationHash: 'transform',
    transformationVersion: {stateVersion: '1aa'},
  };
}

test('removeTrackedQueries removes a gotten inactive query and its row references', async () => {
  const query = gottenInactiveQuery();
  const row: RowRecord = {
    id: {schema: '', table: 'issues', rowKey: {id: '1'}},
    rowVersion: '01',
    patchVersion: {stateVersion: '1aa'},
    refCounts: {[query.id]: 1},
  };
  const {store, updater} = makeQueryUpdater(query, [row]);
  updater.trackQueries(
    createSilentLogContext(),
    [{id: query.id, transformationHash: 'transform'}],
    [],
  );

  expect(updater.removeTrackedQueries([query.id])).toEqual([
    {
      patch: {type: 'query', op: 'del', id: query.id},
      toVersion: {stateVersion: '1aa', configVersion: 1},
    },
  ]);
  expect(store.markQueryAsDeleted).toHaveBeenCalledWith(
    {stateVersion: '1aa', configVersion: 1},
    {type: 'query', op: 'del', id: query.id},
  );
  expect(await updater.deleteUnreferencedRows()).toEqual([
    {
      patch: {type: 'row', op: 'del', id: row.id},
      toVersion: {stateVersion: '1aa', configVersion: 1},
    },
  ]);
  expect(store.putRowRecord).toHaveBeenCalledWith({
    ...row,
    patchVersion: {stateVersion: '1aa', configVersion: 1},
    refCounts: null,
  });
});

test('removeTrackedQueries rejects active, internal, and untracked queries', () => {
  const lc = createSilentLogContext();
  const active = {
    ...gottenInactiveQuery('active'),
    clientState: {client: clientState(undefined)},
  };
  const activeUpdater = makeQueryUpdater(active).updater;
  activeUpdater.trackQueries(
    lc,
    [{id: active.id, transformationHash: 'transform'}],
    [],
  );
  expect(() => activeUpdater.removeTrackedQueries([active.id])).toThrow(
    'Query active is active',
  );

  const internal: InternalQueryRecord = {
    id: 'internal',
    type: 'internal',
    ast: {table: 'clients'},
    transformationHash: 'transform',
  };
  const internalUpdater = makeQueryUpdater(internal).updater;
  internalUpdater.trackQueries(
    lc,
    [{id: internal.id, transformationHash: 'transform'}],
    [],
  );
  expect(() => internalUpdater.removeTrackedQueries([internal.id])).toThrow(
    'reserved for internal use',
  );

  const inactive = gottenInactiveQuery();
  const untrackedUpdater = makeQueryUpdater(inactive).updater;
  untrackedUpdater.trackQueries(lc, [], []);
  expect(() => untrackedUpdater.removeTrackedQueries([inactive.id])).toThrow(
    'was not tracked as executed',
  );
});
