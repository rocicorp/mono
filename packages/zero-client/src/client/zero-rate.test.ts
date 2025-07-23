import {beforeEach, expect, test, vi} from 'vitest';
import type {PushRequest} from '../../../replicache/src/sync/push.ts';
import * as ErrorKind from '../../../zero-protocol/src/error-kind-enum.ts';
import type {Mutation} from '../../../zero-protocol/src/push.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import * as ConnectionState from './connection-state-enum.ts';
import {MockSocket, tickAFewTimes, zeroForTest} from './test-utils.ts';

const startTime = 1678829450000;

beforeEach(() => {
  vi.useFakeTimers({now: startTime});
  vi.stubGlobal('WebSocket', MockSocket as unknown as typeof WebSocket);

  return () => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  };
});

test('connection stays alive on rate limit error', async () => {
  const z = zeroForTest();
  await z.triggerConnected();

  const mockSocket = await z.socket;

  const pushReq: PushRequest = {
    profileID: 'p1',
    clientGroupID: await z.clientGroupID,
    pushVersion: 2,
    schemaVersion: '1',
    mutations: [
      {
        clientID: 'c1',
        id: 1,
        name: 'mut1',
        args: [{d: 1}],
        timestamp: 1,
      },
    ],
  };
  // reset mock socket messages to clear `initConnection` message
  mockSocket.messages.length = 0;
  await z.pusher(pushReq, 'test-request-id');
  await z.triggerError(ErrorKind.MutationRateLimited, 'Rate limit exceeded');

  expect(mockSocket.messages).to.have.lengthOf(1);
  expect(mockSocket.closed).toBe(false);
});

test('a mutation after a rate limit error causes limited mutations to be resent', async () => {
  const z = zeroForTest({
    schema: createSchema({
      tables: [
        table('issue')
          .columns({
            id: string(),
            value: number(),
          })
          .primaryKey('id'),
      ],
    }),
  });
  await z.triggerConnected();
  const mockSocket = await z.socket;
  // reset mock socket messages to clear `initConnection` message
  mockSocket.messages.length = 0;

  await z.mutate.issue.insert({id: 'a', value: 1});
  await z.triggerError(ErrorKind.MutationRateLimited, 'Rate limit exceeded');

  await tickAFewTimes(vi, 0);
  expect(mockSocket.messages).to.have.lengthOf(1);
  expect(mockSocket.closed).toBe(false);
  expect(z.connectionState).eq(ConnectionState.Connected);

  // reset messages
  mockSocket.messages.length = 0;

  // now send another mutation
  await z.mutate.issue.insert({id: 'b', value: 2});
  await z.triggerError(ErrorKind.MutationRateLimited, 'Rate limit exceeded');
  await tickAFewTimes(vi, 0);

  // two mutations should be sent in separate push messages
  expect(mockSocket.messages).to.have.lengthOf(2);
  expect(
    mockSocket.messages.flatMap(m =>
      JSON.parse(m)[1].mutations.map((m: Mutation) => m.id),
    ),
  ).toEqual([1, 2]);
});

test('previously confirmed mutations are not resent after a rate limit error', async () => {
  const z = zeroForTest({
    schema: createSchema({
      tables: [
        table('issue')
          .columns({
            id: string(),
            value: number(),
          })
          .primaryKey('id'),
      ],
    }),
  });
  await z.triggerConnected();
  const mockSocket = await z.socket;
  // reset mock socket messages to clear `initConnection` message
  mockSocket.messages.length = 0;

  await z.mutate.issue.insert({id: 'a', value: 1});
  await tickAFewTimes(vi);
  // confirm the mutation
  await z.triggerPokeStart({
    pokeID: '1',
    baseCookie: null,
    schemaVersions: {minSupportedVersion: 1, maxSupportedVersion: 1},
  });
  await z.triggerPokePart({
    pokeID: '1',
    lastMutationIDChanges: {[z.clientID]: 1},
  });
  await z.triggerPokeEnd({pokeID: '1', cookie: '1'});
  await tickAFewTimes(vi);

  // reset messages
  mockSocket.messages.length = 0;

  // now send another mutation but rate limit it
  await z.mutate.issue.insert({id: 'b', value: 2});
  await z.triggerError(ErrorKind.MutationRateLimited, 'Rate limit exceeded');
  await tickAFewTimes(vi);

  // Only the new mutation should have been sent. The first was confirmed by a poke response.
  expect(
    mockSocket.messages.flatMap(m =>
      JSON.parse(m)[1].mutations.map((m: Mutation) => m.id),
    ),
  ).toEqual([2]);
  mockSocket.messages.length = 0;

  // Send another mutation. This and the last rate limited mutation should be sent
  await z.mutate.issue.insert({id: 'c', value: 3});
  await z.triggerError(ErrorKind.MutationRateLimited, 'Rate limit exceeded');
  await tickAFewTimes(vi);

  expect(
    mockSocket.messages.flatMap(m =>
      JSON.parse(m)[1].mutations.map((m: Mutation) => m.id),
    ),
  ).toEqual([2, 3]);
});
