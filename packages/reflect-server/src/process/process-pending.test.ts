import {
  describe,
  test,
  expect,
  beforeEach,
  afterEach,
  jest,
} from '@jest/globals';
import type {WriteTransaction} from 'replicache';
import type {PokeBody} from 'reflect-protocol';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import {
  ClientRecordMap,
  getClientRecord,
  putClientRecord,
} from '../../src/types/client-record.js';
import type {ClientID, ClientMap} from '../../src/types/client-state.js';
import {getUserValue, UserValue} from '../../src/types/user-value.js';
import {getVersion, putVersion} from '../../src/types/version.js';
import type {Version} from 'reflect-protocol';
import {
  client,
  clientRecord,
  createSilentLogContext,
  Mocket,
  mockMathRandom,
  pendingMutation,
} from '../util/test-utils.js';
import {processPending} from '../process/process-pending.js';
import type {PendingMutation} from '../types/mutation.js';
import {
  getConnectedClients,
  putConnectedClients,
} from '../types/connected-clients.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

mockMathRandom();

const START_TIME = 1000;
beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(START_TIME);
});

afterEach(() => {
  jest.restoreAllMocks();
});

describe('processPending', () => {
  type Case = {
    name: string;
    version: Version;
    clientRecords: ClientRecordMap;
    clients: ClientMap;
    storedConnectedClients: ClientID[];
    pendingMutations: PendingMutation[];
    maxProcessedMutationTimestamp: number;
    expectedError?: string;
    expectedVersion: Version;
    expectedPokes?: Map<ClientID, PokeBody>;
    expectedUserValues?: Map<string, UserValue>;
    expectedClientRecords?: ClientRecordMap;
    expectedPendingMutations?: PendingMutation[];
    expectNothingToProcess?: boolean;
    expectedMaxProcessedMutationTimestamp?: number;
  };

  const s1 = new Mocket();
  const s2 = new Mocket();
  const s3 = new Mocket();

  const cases: Case[] = [
    {
      name: 'no pending mutations connects or disconnects',
      version: 1,
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      clients: new Map(),
      storedConnectedClients: [],
      pendingMutations: [],
      maxProcessedMutationTimestamp: 500,
      expectedVersion: 1,
      expectedPokes: new Map(),
      expectedUserValues: new Map(),
      expectNothingToProcess: true,
      expectedClientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
    },
    {
      name: 'no pending mutations, but connect pending',
      version: 3,
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 3)],
        ['c2', clientRecord('cg1', 1)],
      ]),
      clients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1', s2, 0),
      ]),
      storedConnectedClients: ['c1'],
      pendingMutations: [],
      maxProcessedMutationTimestamp: 500,
      expectedVersion: 3,
      // newly connected client is fast forwarded
      expectedPokes: new Map([
        [
          'c2',
          {
            pokes: [
              {baseCookie: 1, cookie: 3, lastMutationIDChanges: {}, patch: []},
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map(),
      expectNothingToProcess: false,
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 3)],
        ['c2', clientRecord('cg1', 3)],
      ]),
    },
    {
      name: 'no pending mutations, but disconnect pending',
      version: 1,
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1)],
        ['c2', clientRecord('cg1', 1)],
      ]),
      clients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
      storedConnectedClients: ['c1', 'c2'],
      pendingMutations: [],
      maxProcessedMutationTimestamp: 500,
      // version updated by disconnectHandler
      expectedVersion: 2,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
              {baseCookie: 1, cookie: 2, lastMutationIDChanges: {}, patch: []},
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map(),
      expectNothingToProcess: false,
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 2)],
        ['c2', clientRecord('cg1', 1)],
      ]),
    },
    {
      name: 'one client, one mutation, all processed',
      version: 1,
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      clients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
      storedConnectedClients: ['c1'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamp: 750,
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 700,
      expectedVersion: 2,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
              {
                baseCookie: 1,
                cookie: 2,
                lastMutationIDChanges: {c1: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 1,
                  },
                ],
                timestamp: 750,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 1, version: 2, deleted: false}],
      ]),
      expectedClientRecords: new Map([['c1', clientRecord('cg1', 2, 2, 2)]]),
      expectedMaxProcessedMutationTimestamp: 750,
    },
    {
      name: 'three clients, two client groups, three mutations, all processed',
      version: 1,
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1)],
        ['c2', clientRecord('cg1', 1)],
        ['c3', clientRecord('cg2', 1)],
      ]),
      clients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1', s2, 0),
        client('c3', 'u3', 'cg2', s3, 0),
      ]),
      storedConnectedClients: ['c1', 'c2', 'c3'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamp: 700,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 2,
          timestamp: 720,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c3',
          clientGroupID: 'cg2',
          id: 2,
          timestamp: 740,
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 700,
      expectedVersion: 4,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
              {
                baseCookie: 1,
                cookie: 2,
                lastMutationIDChanges: {c1: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 1,
                  },
                ],
                timestamp: 700,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 720,
              },
              {
                baseCookie: 3,
                cookie: 4,
                lastMutationIDChanges: {},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 3,
                  },
                ],
                timestamp: 740,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
        [
          'c2',
          {
            pokes: [
              {
                baseCookie: 1,
                cookie: 2,
                lastMutationIDChanges: {c1: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 1,
                  },
                ],
                timestamp: 700,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 720,
              },
              {
                baseCookie: 3,
                cookie: 4,
                lastMutationIDChanges: {},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 3,
                  },
                ],
                timestamp: 740,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
        [
          'c3',
          {
            pokes: [
              {
                baseCookie: 1,
                cookie: 2,
                lastMutationIDChanges: {},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 1,
                  },
                ],
                timestamp: 700,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 720,
              },
              {
                baseCookie: 3,
                cookie: 4,
                lastMutationIDChanges: {c3: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 3,
                  },
                ],
                timestamp: 740,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 3, version: 4, deleted: false}],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 4, 2, 2)],
        ['c2', clientRecord('cg1', 4, 2, 3)],
        ['c3', clientRecord('cg2', 4, 2, 4)],
      ]),
      expectedMaxProcessedMutationTimestamp: 740,
    },
    {
      name: 'two clients, two client groups, four mutations all w timestamps, two processed',
      version: 1,
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1)],
        ['c2', clientRecord('cg1', 1)],
      ]),
      clients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1', s2, 0),
      ]),
      storedConnectedClients: ['c1', 'c2'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamp: 790,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 2,
          timestamp: 800,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 3,
          timestamp: 801,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 3,
          timestamp: 810,
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 700,
      expectedVersion: 3,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
              {
                baseCookie: 1,
                cookie: 2,
                lastMutationIDChanges: {c1: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 1,
                  },
                ],
                timestamp: 790,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 800,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
        [
          'c2',
          {
            pokes: [
              {
                baseCookie: 1,
                cookie: 2,
                lastMutationIDChanges: {c1: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 1,
                  },
                ],
                timestamp: 790,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 800,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 2, version: 3, deleted: false}],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 3, 2, 2)],
        ['c2', clientRecord('cg1', 3, 2, 3)],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 3,
          timestamp: 801,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 3,
          timestamp: 810,
          name: 'inc',
        }),
      ],
      expectedMaxProcessedMutationTimestamp: 800,
    },
    {
      name: 'two clients, two client groups, four mutations some with undefined timestamps, two processed',
      version: 1,
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1)],
        ['c2', clientRecord('cg1', 1)],
      ]),
      clients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1', s2, 0),
      ]),
      storedConnectedClients: ['c1', 'c2'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamp: 790,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 2,
          timestamp: undefined,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 3,
          timestamp: 801,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 3,
          timestamp: undefined,
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 700,
      expectedVersion: 3,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
              {
                baseCookie: 1,
                cookie: 2,
                lastMutationIDChanges: {c1: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 1,
                  },
                ],
                timestamp: 790,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: undefined,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
        [
          'c2',
          {
            pokes: [
              {
                baseCookie: 1,
                cookie: 2,
                lastMutationIDChanges: {c1: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 1,
                  },
                ],
                timestamp: 790,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c2: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: undefined,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 2, version: 3, deleted: false}],
      ]),
      expectedClientRecords: new Map([
        ['c1', clientRecord('cg1', 3, 2, 2)],
        ['c2', clientRecord('cg1', 3, 2, 3)],
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 3,
          timestamp: 801,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c2',
          clientGroupID: 'cg1',
          id: 3,
          timestamp: undefined,
          name: 'inc',
        }),
      ],
      expectedMaxProcessedMutationTimestamp: 790,
    },
    {
      name: 'one client, one mutation, all processed, passed maxProcessedMutationTimestamp is greater than processed',
      version: 1,
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      clients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
      storedConnectedClients: ['c1'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamp: 750,
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 800,
      expectedVersion: 2,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
              {
                baseCookie: 1,
                cookie: 2,
                lastMutationIDChanges: {c1: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 1,
                  },
                ],
                timestamp: 750,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 1, version: 2, deleted: false}],
      ]),
      expectedClientRecords: new Map([['c1', clientRecord('cg1', 2, 2, 2)]]),
      expectedMaxProcessedMutationTimestamp: 800,
    },
    {
      name: 'one client, two mutations, all processed, maxProcessedMutationTimestamp returned is not last mutation',
      version: 1,
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      clients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
      storedConnectedClients: ['c1'],
      pendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamp: 750,
          name: 'inc',
        }),
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 3,
          timestamp: 720,
          name: 'inc',
        }),
      ],
      maxProcessedMutationTimestamp: 700,
      expectedVersion: 3,
      expectedPokes: new Map([
        [
          'c1',
          {
            pokes: [
              {
                baseCookie: 1,
                cookie: 2,
                lastMutationIDChanges: {c1: 2},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 1,
                  },
                ],
                timestamp: 750,
              },
              {
                baseCookie: 2,
                cookie: 3,
                lastMutationIDChanges: {c1: 3},
                patch: [
                  {
                    op: 'put',
                    key: 'count',
                    value: 2,
                  },
                ],
                timestamp: 720,
              },
            ],
            requestID: '4fxcm49g2j9',
          },
        ],
      ]),
      expectedUserValues: new Map([
        ['count', {value: 2, version: 3, deleted: false}],
      ]),
      expectedClientRecords: new Map([['c1', clientRecord('cg1', 3, 3, 3)]]),
      expectedMaxProcessedMutationTimestamp: 750,
    },
  ];

  const mutators = new Map(
    Object.entries({
      inc: async (tx: WriteTransaction) => {
        let count = ((await tx.get('count')) as number) ?? 0;
        count++;
        await tx.put('count', count);
      },
    }),
  );

  for (const c of cases) {
    test(c.name, async () => {
      const durable = await getMiniflareDurableObjectStorage(id);
      await durable.deleteAll();
      const storage = new DurableStorage(durable);
      await putVersion(c.version, storage);
      for (const [clientID, record] of c.clientRecords) {
        await putClientRecord(clientID, record, storage);
      }
      await putConnectedClients(new Set(c.storedConnectedClients), storage);
      for (const [, clientState] of c.clients) {
        (clientState.socket as Mocket).log.length = 0;
      }
      const p = processPending(
        createSilentLogContext(),
        storage,
        c.clients,
        c.pendingMutations,
        mutators,
        () => Promise.resolve(),
        c.maxProcessedMutationTimestamp,
      );
      if (c.expectedError) {
        let expectedE;
        try {
          await p;
        } catch (e) {
          expectedE = String(e);
        }
        expect(expectedE).toEqual(c.expectedError);
        return;
      }

      expect(await p).toEqual({
        maxProcessedMutationTimestamp:
          c.expectedMaxProcessedMutationTimestamp ??
          c.maxProcessedMutationTimestamp,
        nothingToProcess: c.expectNothingToProcess ?? false,
      });
      expect(c.pendingMutations).toEqual(c.expectedPendingMutations ?? []);
      expect(await getConnectedClients(storage)).toEqual(
        new Set(c.clients.keys()),
      );

      expect(c.expectedError).toBeUndefined;
      expect(await getVersion(storage)).toEqual(c.expectedVersion);
      for (const [clientID, clientState] of c.clients) {
        const mocket = clientState.socket as Mocket;
        const expectedPoke = c.expectedPokes?.get(clientID);
        if (!expectedPoke) {
          expect(mocket.log.length).toEqual(0);
        } else {
          expect(mocket.log[0]).toEqual([
            'send',
            JSON.stringify(['poke', expectedPoke]),
          ]);
        }
      }
      for (const [expKey, expValue] of c.expectedUserValues ?? new Map()) {
        expect(await getUserValue(expKey, storage)).toEqual(expValue);
      }
      for (const [expClientID, expRecord] of c.expectedClientRecords ??
        new Map()) {
        expect(await getClientRecord(expClientID, storage)).toEqual(expRecord);
      }
    });
  }
});
