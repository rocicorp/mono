import {expect, test, vi} from 'vitest';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {rowIDString} from '../../types/row-key.ts';
import type {CVRStore} from './cvr-store.ts';
import {
  classifyQueriesForHydration,
  CVRQueryDrivenUpdater,
  getInactiveQueries,
  type CVR,
  type RowUpdate,
} from './cvr.ts';
import type {
  ClientQueryRecord,
  InternalQueryRecord,
  QueryRecord,
  RowID,
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

function makeQueryUpdater(
  queries: QueryRecord | QueryRecord[],
  rows: RowRecord[] = [],
) {
  const cvr = makeCVR({});
  for (const query of Array.isArray(queries) ? queries : [queries]) {
    cvr.queries[query.id] = query;
  }
  const rowRecords = new CustomKeyMap<RowID, RowRecord>(rowIDString);
  for (const row of rows) {
    rowRecords.set(row.id, row);
  }
  const puts: RowRecord[] = [];
  const store = {
    getRowRecords: vi.fn(() => Promise.resolve(rowRecords)),
    markQueryAsDeleted: vi.fn(),
    putRowRecord: vi.fn((row: RowRecord) => puts.push(row)),
    delRowRecord: vi.fn(),
    updateQuery: vi.fn(),
  } as unknown as CVRStore;
  const updater = new CVRQueryDrivenUpdater(
    store,
    cvr,
    cvr.version.stateVersion,
    cvr.replicaVersion!,
  );
  updater.ensureNewVersion();
  return {store, updater, puts};
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

test("removeTrackedQueries drops only the removed query's row references", async () => {
  // A row shared by a query that stays and a query that is budget-evicted must
  // survive with the surviving query's refCount intact.
  const kept = {
    ...gottenInactiveQuery('kept'),
    clientState: {client: clientState(undefined)},
  };
  const evicted = gottenInactiveQuery('evicted');
  const shared: RowRecord = {
    id: {schema: '', table: 'issues', rowKey: {id: '1'}},
    rowVersion: '01',
    patchVersion: {stateVersion: '1aa'},
    refCounts: {kept: 1, evicted: 1},
  };
  const onlyEvicted: RowRecord = {
    id: {schema: '', table: 'issues', rowKey: {id: '2'}},
    rowVersion: '01',
    patchVersion: {stateVersion: '1aa'},
    refCounts: {evicted: 1},
  };

  const {updater, puts} = makeQueryUpdater(
    [kept, evicted],
    [shared, onlyEvicted],
  );
  updater.trackQueries(
    createSilentLogContext(),
    [
      {id: 'kept', transformationHash: 'transform'},
      {id: 'evicted', transformationHash: 'transform'},
    ],
    [],
  );

  // 'kept' hydrates and re-reports the shared row; 'evicted' never starts.
  const received = new CustomKeyMap<RowID, RowUpdate>(rowIDString);
  received.set(shared.id, {
    refCounts: {kept: 1},
    version: '01',
    contents: {id: '1'},
  });
  await updater.received(createSilentLogContext(), received);

  updater.removeTrackedQueries(['evicted']);

  expect(await updater.deleteUnreferencedRows()).toEqual([
    {
      patch: {type: 'row', op: 'del', id: onlyEvicted.id},
      toVersion: {stateVersion: '1aa', configVersion: 1},
    },
  ]);

  const byTable = new Map(puts.map(row => [row.id.rowKey.id, row.refCounts]));
  expect(byTable.get('1')).toEqual({kept: 1});
  expect(byTable.get('2')).toBeNull();
});

test('removeTrackedQueries keeps the query in row reconciliation', () => {
  // The evicted query must stay in the removed-or-executed set so that
  // deleteUnreferencedRows() drops its references. Un-tracking it here would
  // strip its refCounts from rows already flushed by received() with no way to
  // put them back.
  const query = gottenInactiveQuery();
  const row: RowRecord = {
    id: {schema: '', table: 'issues', rowKey: {id: '1'}},
    rowVersion: '01',
    patchVersion: {stateVersion: '1aa'},
    refCounts: {[query.id]: 1},
  };
  const {updater} = makeQueryUpdater(query, [row]);
  updater.trackQueries(
    createSilentLogContext(),
    [{id: query.id, transformationHash: 'transform'}],
    [],
  );
  updater.removeTrackedQueries([query.id]);

  // Removing it twice is a bug: the query is gone from the CVR snapshot.
  expect(() => updater.removeTrackedQueries([query.id])).toThrow();
});

test('removeTrackedQueries requires a final CVR version', () => {
  const query = gottenInactiveQuery();
  const cvr = makeCVR({});
  cvr.queries[query.id] = query;
  const store = {
    getRowRecords: vi.fn(() =>
      Promise.resolve(new CustomKeyMap<RowID, RowRecord>(rowIDString)),
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
  // No ensureNewVersion() and no transformation change, so the version is
  // unchanged: a 'del' patch emitted here would land on a stale poke version.
  updater.trackQueries(
    createSilentLogContext(),
    [{id: query.id, transformationHash: 'transform'}],
    [],
  );
  expect(() => updater.removeTrackedQueries([query.id])).toThrow(
    'A final CVR version must be set before removing tracked queries',
  );
});
