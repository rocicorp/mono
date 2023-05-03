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
import {randomID} from '../util/rand.js';
import type {ErrorKind} from 'reflect-protocol';
import {DurableStorage} from '../storage/durable-storage.js';
import type {PendingMutation} from '../types/mutation.js';

const START_TIME = 1683000000000;
beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(START_TIME);
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
    expectLastActivityUpdated?: boolean;
  };

  const cases: Case[] = [
    {
      name: 'empty',
      data: '',
      expectedErrorKind: 'InvalidMessage',
      expectedErrorMessage: 'SyntaxError: Unexpected end of JSON input',
    },
    {
      name: 'invalid message',
      data: '[]',
      expectedErrorKind: 'InvalidMessage',
      expectedErrorMessage: 'TypeError: Invalid union value',
    },
    {
      name: 'valid push',
      data: JSON.stringify([
        'push',
        {
          clientGroupID: 'cg1',
          mutations: [
            mutation('c1', 1, START_TIME + 10),
            mutation('c1', 2, START_TIME + 20),
          ],
          pushVersion: 1,
          schemaVersion: '',
          timestamp: START_TIME + 42,
          requestID: randomID(),
        },
      ]),
      expectedPendingMutations: [
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 1,
          timestamps: {
            normalizedTimestamp: START_TIME + 10,
            originTimestamp: START_TIME + 10,
            serverReceivedTimestamp: START_TIME,
          },
        }),
        pendingMutation({
          clientID: 'c1',
          clientGroupID: 'cg1',
          id: 2,
          timestamps: {
            normalizedTimestamp: START_TIME + 20,
            originTimestamp: START_TIME + 20,
            serverReceivedTimestamp: START_TIME,
          },
        }),
      ],
      expectLastActivityUpdated: true,
    },
    {
      name: 'push missing requestID',
      data: JSON.stringify([
        'push',
        {
          clientID: 'c1',
          mutations: [
            mutation('c1', 1, START_TIME + 10),
            mutation('c1', 2, START_TIME + 20),
          ],
          pushVersion: 1,
          schemaVersion: '',
          timestamp: START_TIME + 42,
        },
      ]),
      // This error message is not great
      expectedErrorKind: 'InvalidMessage',
      expectedErrorMessage: 'TypeError: Invalid union value',
    },
    {
      name: 'missing client push',
      data: JSON.stringify([
        'push',
        {
          clientGroupID: 'cg1',
          mutations: [
            mutation('c1', 1, START_TIME + 10),
            mutation('c1', 2, START_TIME + 20),
          ],
          pushVersion: 1,
          schemaVersion: '',
          timestamp: START_TIME + 42,
          requestID: randomID(),
        },
      ]),
      clients: new Map(),
      clientID: 'c1',
      expectedErrorKind: 'ClientNotFound',
      expectedErrorMessage: 'c1',
      expectSocketClosed: true,
    },
    {
      name: 'valid ping',
      data: JSON.stringify(['ping', {}]),
      expectLastActivityUpdated: true,
    },
    {
      name: 'missing client ping',
      data: JSON.stringify(['ping', {}]),
      clients: new Map(),
      clientID: 'c1',
      expectedErrorKind: 'ClientNotFound',
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
      const prevLastActivityTimestamp = START_TIME - 5000;
      const clients: ClientMap =
        c.clients ||
        new Map([
          client(
            clientID,
            'u1',
            clientGroupID,
            s1,
            0,
            false,
            prevLastActivityTimestamp,
          ),
        ]);

      const {roomDO} = getMiniflareBindings();
      const storage = new DurableStorage(
        await getMiniflareDurableObjectStorage(roomDO.newUniqueId()),
      );

      const pendingMutations: PendingMutation[] = [];
      const clientPreHandleMessage = structuredClone(clients.get(clientID));
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

      if (c.expectLastActivityUpdated) {
        expect({
          ...clients.get(clientID),
          socket: undefined,
        }).toEqual({
          ...clientPreHandleMessage,
          socket: undefined,
          lastActivityTimestamp: START_TIME,
        });
      } else {
        const client = clients.get(clientID);
        if (!client) {
          expect(clientPreHandleMessage).toBeUndefined();
        } else {
          expect({...client, socket: undefined}).toEqual({
            ...clientPreHandleMessage,
            socket: undefined,
          });
        }
      }

      if (c.expectedPendingMutations) {
        expect(pendingMutations).toEqual(c.expectedPendingMutations);
      }
    });
  }
});
