import {expect, test} from 'vitest';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {rowIDString} from '../../types/row-key.ts';
import type {PatchToVersion} from './client-handler.ts';
import type {CVRStore} from './cvr-store.ts';
import {
  CVRQueryDrivenUpdater,
  getInactiveQueries,
  type CVR,
  type CVRSnapshot,
  type RowUpdate,
} from './cvr.ts';
import type {
  ClientQueryRecord,
  CVRVersion,
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

const lc = createSilentLogContext();
const patchVersion: CVRVersion = {stateVersion: '1a0'};
const updatedVersion: CVRVersion = {stateVersion: '1aa'};

const ROW_ID1: RowID = {
  schema: 'public',
  table: 'issues',
  rowKey: {id: '1'},
};

const ROW_ID2: RowID = {
  schema: 'public',
  table: 'issues',
  rowKey: {id: '2'},
};

function makeQueryDrivenCVR(): CVRSnapshot {
  return {
    id: 'abc123',
    version: {stateVersion: '1a9'},
    lastActive: Date.UTC(2024, 1, 20),
    ttlClock: ttlClockFromNumber(Date.UTC(2024, 1, 20)),
    replicaVersion: '120',
    clients: {
      fooClient: {
        id: 'fooClient',
        desiredQueryIDs: ['oneHash'],
      },
    },
    queries: {
      oneHash: {
        ast: {table: 'issues'},
        type: 'client',
        clientState: {
          fooClient: {
            inactivatedAt: undefined,
            ttl: 1000,
            version: {stateVersion: '1a9', configVersion: 1},
          },
        },
        id: 'oneHash',
        patchVersion,
        transformationHash: 'serverOneHash',
        transformationVersion: patchVersion,
      },
    },
    clientSchema: null,
    profileID: null,
  };
}

function makeRowRecord(
  id: RowID,
  rowVersion: string,
  refCounts: RowRecord['refCounts'] = {oneHash: 1},
  recordPatchVersion: CVRVersion = patchVersion,
): RowRecord {
  return {
    id,
    rowVersion,
    patchVersion: recordPatchVersion,
    refCounts,
  };
}

function makeStore(rowRecords: RowRecord[]) {
  const rows = new CustomKeyMap<RowID, RowRecord>(rowIDString);
  for (const row of rowRecords) {
    rows.set(row.id, row);
  }

  const putRows: RowRecord[] = [];
  const deletedRows: RowID[] = [];
  const clearedRows: RowID[] = [];
  const store = {
    getRowRecords: () => Promise.resolve(rows),
    putRowRecord: (row: RowRecord) => {
      putRows.push(row);
    },
    clearRowRecordUpdate: (id: RowID) => {
      clearedRows.push(id);
    },
    delRowRecord: (id: RowID) => {
      deletedRows.push(id);
    },
    updateQuery: () => {},
  } as unknown as CVRStore;

  return {clearedRows, deletedRows, putRows, store};
}

function makeTrackedUpdater(store: CVRStore): CVRQueryDrivenUpdater {
  const updater = new CVRQueryDrivenUpdater(
    store,
    makeQueryDrivenCVR(),
    updatedVersion.stateVersion,
    '120',
  );
  updater.trackQueries(
    lc,
    [{id: 'oneHash', transformationHash: 'serverOneHash'}],
    [],
  );
  return updater;
}

test('received skips row persistence for unchanged records while emitting row patches', async () => {
  const existing = makeRowRecord(ROW_ID1, '03');
  const {deletedRows, putRows, store} = makeStore([existing]);
  const updater = makeTrackedUpdater(store);

  const update: RowUpdate = {
    version: '03',
    contents: {id: 'same-row-version'},
    refCounts: {oneHash: 1},
  };

  expect(await updater.received(lc, new Map([[ROW_ID1, update]]))).toEqual([
    {
      toVersion: patchVersion,
      patch: {
        type: 'row',
        op: 'put',
        id: ROW_ID1,
        contents: {id: 'same-row-version'},
      },
    },
  ] satisfies PatchToVersion[]);
  expect(putRows).toEqual([]);
  expect(deletedRows).toEqual([]);
});

test('received persists changed and deleted row records with the correct patches', async () => {
  const existing1 = makeRowRecord(ROW_ID1, '03');
  const existing2 = makeRowRecord(ROW_ID2, '03');
  const {clearedRows, deletedRows, putRows, store} = makeStore([
    existing1,
    existing2,
  ]);
  const updater = makeTrackedUpdater(store);

  expect(
    await updater.received(
      lc,
      new Map([
        [
          ROW_ID1,
          {
            version: '04',
            contents: {id: 'changed'},
            refCounts: {oneHash: 1},
          },
        ],
      ]),
    ),
  ).toEqual([
    {
      toVersion: updatedVersion,
      patch: {
        type: 'row',
        op: 'put',
        id: ROW_ID1,
        contents: {id: 'changed'},
      },
    },
  ] satisfies PatchToVersion[]);
  expect(putRows).toEqual([
    {
      ...existing1,
      rowVersion: '04',
      patchVersion: updatedVersion,
    },
  ]);

  expect(
    await updater.received(
      lc,
      new Map([
        [
          ROW_ID2,
          {
            refCounts: {oneHash: 0},
          },
        ],
      ]),
    ),
  ).toEqual([
    {
      toVersion: updatedVersion,
      patch: {
        type: 'row',
        op: 'del',
        id: ROW_ID2,
      },
    },
  ] satisfies PatchToVersion[]);
  expect(putRows).toEqual([
    {
      ...existing1,
      rowVersion: '04',
      patchVersion: updatedVersion,
    },
    {
      ...existing2,
      patchVersion: updatedVersion,
      refCounts: null,
    },
  ]);
  expect(deletedRows).toEqual([]);
  expect(clearedRows).toEqual([]);
});

test('received cancels a queued row update when the final row record is unchanged', async () => {
  const existing = makeRowRecord(ROW_ID1, '03');
  const {clearedRows, putRows, store} = makeStore([existing]);
  const updater = makeTrackedUpdater(store);

  expect(
    await updater.received(
      lc,
      new Map([
        [
          ROW_ID1,
          {
            version: '04',
            contents: {id: 'changed'},
            refCounts: {oneHash: 1},
          },
        ],
      ]),
    ),
  ).toEqual([
    {
      toVersion: updatedVersion,
      patch: {
        type: 'row',
        op: 'put',
        id: ROW_ID1,
        contents: {id: 'changed'},
      },
    },
  ] satisfies PatchToVersion[]);
  expect(putRows).toEqual([
    {
      ...existing,
      rowVersion: '04',
      patchVersion: updatedVersion,
    },
  ]);

  expect(
    await updater.received(
      lc,
      new Map([
        [
          ROW_ID1,
          {
            version: '03',
            contents: {id: 'original'},
            refCounts: {oneHash: 0},
          },
        ],
      ]),
    ),
  ).toEqual([]);
  expect(clearedRows).toEqual([ROW_ID1]);
});
