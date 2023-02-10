import {test, describe, expect} from '@jest/globals';
import type {Mutation} from '../../src/protocol/push.js';
import type {ClientID, ClientMap} from '../../src/types/client-state.js';
import {
  client,
  createSilentLogContext,
  Mocket,
  mutation,
} from '../util/test-utils.js';
import {handleMessage} from '../../src/server/message.js';
import {assert} from '../util/asserts.js';
import {randomID} from '../util/rand.js';
import {ErrorKind} from '../protocol/error.js';

describe('handleMessage', () => {
  type Case = {
    name: string;
    data: string;
    clients?: ClientMap;
    clientID?: ClientID;
    expectedErrorKind?: ErrorKind;
    expectedErrorMessage?: string;
    expectedPendingMutations?: Mutation[];
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
        'StructError: Expected the value to satisfy a union of `tuple | tuple`, but received: ',
    },
    {
      name: 'valid push',
      data: JSON.stringify([
        'push',
        {
          clientID: 'c1',
          mutations: [mutation(1), mutation(2)],
          pushVersion: 1,
          schemaVersion: '',
          timestamp: 42,
          requestID: randomID(),
        },
      ]),
      expectedPendingMutations: [
        mutation(1, undefined, undefined, 2),
        mutation(2, undefined, undefined, 2),
      ],
    },
    {
      name: 'push missing requestID',
      data: JSON.stringify([
        'push',
        {
          clientID: 'c1',
          mutations: [mutation(1), mutation(2)],
          pushVersion: 1,
          schemaVersion: '',
          timestamp: 42,
        },
      ]),
      // This error message is not great
      expectedErrorKind: ErrorKind.InvalidMessage,
      expectedErrorMessage:
        'StructError: Expected the value to satisfy a union of `tuple | tuple`, but received: push,[object Object]',
    },
    {
      name: 'missing client push',
      data: JSON.stringify([
        'push',
        {
          clientID: 'c1',
          mutations: [mutation(1), mutation(2)],
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
    test(c.name, () => {
      const s1 = new Mocket();
      const clientID = c.clientID !== undefined ? c.clientID : 'c1';
      const clients: ClientMap =
        c.clients || new Map([client(clientID, 'u1', s1)]);

      handleMessage(
        createSilentLogContext(),
        clients,
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
        expect(client.pending).toEqual(c.expectedPendingMutations);
      }
    });
  }
});
