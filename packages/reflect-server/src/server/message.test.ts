import {
  test,
  describe,
  expect,
  afterEach,
  beforeEach,
  jest,
} from '@jest/globals';
import type {
  ClientID,
  ClientGroupID,
  ClientMap,
} from '../../src/types/client-state.js';
import {
  client,
  createSilentLogContext,
  Mocket,
  mutation,
  pendingMutation,
} from '../util/test-utils.js';
import {handleMessage} from '../../src/server/message.js';
import {assert} from 'shared';
import {randomID} from '../util/rand.js';
import {ErrorKind} from 'reflect-protocol';
import {DurableStorage} from '../storage/durable-storage.js';
import type {PendingMutation} from '../types/mutation.js';

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.restoreAllMocks();
});

describe('handleMessage', () => {
  type Case = {
    name: string;
    data: string;
    clients?: ClientMap;
    clientID?: ClientID;
    clientGroupID?: ClientGroupID;
    expectedErrorKind?: ErrorKind;
    expectedErrorMessage?: string;
    expectedPendingMutations?: PendingMutation[];
    expectSocketClosed?: boolean;
  };

  const cases: Case[] = [
    {
      name: 'empty',
      data: '',
      expectedErrorKind: ErrorKind.InvalidMessage,
      expectedErrorMessage: 'SyntaxError: Unexpected end of JSON input',
    },
    {
      name: 'invalid push',
      data: '[]',
      expectedErrorKind: ErrorKind.InvalidMessage,
      expectedErrorMessage:
        'StructError: Expected the value to satisfy a union of `tuple | tuple | tuple`, but received: ',
    },
    {
      name: 'valid push',
      data: JSON.stringify([
        'push',
        {
          clientGroupID: 'cg1',
          mutations: [mutation('c1', 1, 10), mutation('c1', 2, 20)],
          pushVersion: 1,
          schemaVersion: '',
          timestamp: 42,
          requestID: randomID(),
        },
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 1,
          timestamp: 10,
        }),
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamp: 20,
        }),
      ],
    },
    {
      name: 'push missing requestID',
      data: JSON.stringify([
        'push',
        {
          clientID: 'c1',
          mutations: [mutation('c1', 1, 10), mutation('c1', 2, 20)],
          pushVersion: 1,
          schemaVersion: '',
          timestamp: 42,
        },
      ]),
      // This error message is not great
      expectedErrorKind: ErrorKind.InvalidMessage,
      expectedErrorMessage:
        'StructError: Expected the value to satisfy a union of `tuple | tuple | tuple`, but received: push,[object Object]',
    },
    {
      name: 'missing client push',
      data: JSON.stringify([
        'push',
        {
          clientGroupID: 'cg1',
          mutations: [mutation('c1', 1), mutation('c1', 2)],
          pushVersion: 1,
          schemaVersion: '',
          timestamp: 42,
          requestID: randomID(),
        },
      ]),
      clients: new Map(),
      clientID: 'c1',
      expectedErrorKind: ErrorKind.ClientNotFound,
      expectedErrorMessage: 'c1',
      expectSocketClosed: true,
    },
    {
      name: 'missing client ping',
      data: JSON.stringify(['ping', {}]),
      clients: new Map(),
      clientID: 'c1',
      expectedErrorKind: ErrorKind.ClientNotFound,
      expectedErrorMessage: 'c1',
      expectSocketClosed: true,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const s1 = new Mocket();
      const clientID = c.clientID !== undefined ? c.clientID : 'c1';
      const clientGroupID =
        c.clientGroupID !== undefined ? c.clientGroupID : 'cg1';
      const clients: ClientMap =
        c.clients || new Map([client(clientID, 'u1', clientGroupID, s1, 0)]);

      const {roomDO} = getMiniflareBindings();
      const storage = new DurableStorage(
        await getMiniflareDurableObjectStorage(roomDO.newUniqueId()),
      );

      const pendingMutations: PendingMutation[] = [];
      await handleMessage(
        createSilentLogContext(),
        storage,
        clients,
        pendingMutations,
        clientID,
        c.data,
        s1,
        () => undefined,
      );

      if (c.expectSocketClosed) {
        expect(s1.log.length).toBeGreaterThan(0);
        expect(s1.log[s1.log.length - 1][0]).toEqual('close');
      }

      if (c.expectedErrorKind !== undefined) {
        expect(s1.log.length).toEqual(c.expectSocketClosed ? 2 : 1);

        expect(s1.log[0]).toEqual([
          'send',
          JSON.stringify([
            'error',
            c.expectedErrorKind,
            c.expectedErrorMessage,
          ]),
        ]);
        if (c.expectSocketClosed) {
          expect(s1.log[1]).toEqual(['close']);
        }
      }

      if (c.expectedPendingMutations) {
        const client = clients.get(clientID);
        assert(client);
        expect(pendingMutations).toEqual(c.expectedPendingMutations);
      }
    });
  }
});
