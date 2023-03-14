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
import type {ClientMap} from '../../src/types/client-state.js';
import {getUserValue, UserValue} from '../../src/types/user-value.js';
import {getVersion, putVersion} from '../../src/types/version.js';
import type {Version} from 'reflect-protocol';
import {
  client,
  clientRecord,
  createSilentLogContext,
  fail,
  Mocket,
  mockMathRandom,
  pendingMutation,
} from '../util/test-utils.js';
import {processPending} from '../process/process-pending.js';
import type {PendingMutation} from '../types/mutation.js';

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
    pendingMutations: PendingMutation[];
    maxProcessedMutationTimestamp: number;
    expectedError?: string;
    expectedClients: ClientMap;
    expectedVersion: Version;
    expectedPokes?: Map<Mocket, PokeBody>;
    expectedUserValues?: Map<string, UserValue>;
    expectedClientRecords?: ClientRecordMap;
    expectedPendingMutations?: PendingMutation[];
    expectedMaxProcessedMutationTimestamp?: number;
  };

  const s1 = new Mocket();
  const s2 = new Mocket();
  const s3 = new Mocket();

  const cases: Case[] = [
    {
      name: 'none pending',
      version: 1,
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      clients: new Map(),
      pendingMutations: [],
      maxProcessedMutationTimestamp: 500,
      expectedClients: new Map(),
      expectedVersion: 1,
      expectedPokes: new Map(),
      expectedUserValues: new Map(),
      expectedClientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
    },
    {
      name: 'one client, one mutation, all processed',
      version: 1,
      clientRecords: new Map([['c1', clientRecord('cg1', 1)]]),
      clients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
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
      expectedClients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
      expectedVersion: 2,
      expectedPokes: new Map([
        [
          s1,
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
      expectedClients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1', s2, 0),
        client('c3', 'u3', 'cg2', s3, 0),
      ]),
      expectedVersion: 4,
      expectedPokes: new Map<Mocket, PokeBody>([
        [
          s1,
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
          s2,
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
          s3,
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
      expectedClients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1', s2, 0),
      ]),
      expectedVersion: 3,
      expectedPokes: new Map<Mocket, PokeBody>([
        [
          s1,
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
          s2,
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
      expectedClients: new Map([
        client('c1', 'u1', 'cg1', s1, 0),
        client('c2', 'u2', 'cg1', s2, 0),
      ]),
      expectedVersion: 3,
      expectedPokes: new Map<Mocket, PokeBody>([
        [
          s1,
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
          s2,
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
      expectedClients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
      expectedVersion: 2,
      expectedPokes: new Map([
        [
          s1,
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
      expectedClients: new Map([client('c1', 'u1', 'cg1', s1, 0)]),
      expectedVersion: 3,
      expectedPokes: new Map([
        [
          s1,
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
        try {
          await p;
          fail('should have thrown');
        } catch (e) {
          expect(String(e)).toEqual(c.expectedError);
        }
        return;
      }

      expect(await p).toEqual(
        c.expectedMaxProcessedMutationTimestamp ??
          c.maxProcessedMutationTimestamp,
      );
      expect(c.clients).toEqual(c.expectedClients);
      expect(c.pendingMutations).toEqual(c.expectedPendingMutations ?? []);

      expect(c.expectedError).toBeUndefined;
      expect(await getVersion(storage)).toEqual(c.expectedVersion);
      for (const [mocket, clientPoke] of c.expectedPokes ?? []) {
        expect(mocket.log[0]).toEqual([
          'send',
          JSON.stringify(['poke', clientPoke]),
        ]);
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
