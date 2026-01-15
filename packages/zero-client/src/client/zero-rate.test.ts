import {beforeEach, expect, test, vi} from 'vitest';
import type {PushRequest} from '../../../replicache/src/sync/push.ts';
import * as ErrorKind from '../../../zero-protocol/src/error-kind-enum.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import type {Mutation} from '../../../zero-protocol/src/push.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {defineMutatorsWithType} from '../../../zql/src/mutate/mutator-registry.ts';
import {defineMutatorWithType} from '../../../zql/src/mutate/mutator.ts';
import {ConnectionStatus} from './connection-status.ts';
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
    pushVersion: 1,
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
  await z.triggerError({
    kind: ErrorKind.MutationRateLimited,
    message: 'Rate limit exceeded',
    origin: ErrorOrigin.Server,
  });

  expect(mockSocket.messages).toHaveLength(1);
  expect(mockSocket.closed).toBe(false);
});

test('a mutation after a rate limit error causes limited mutations to be resent', async () => {
  const schema = createSchema({
    tables: [
      table('issue')
        .columns({
          id: string(),
          value: number(),
        })
        .primaryKey('id'),
    ],
  });
  const mutators = defineMutatorsWithType<typeof schema>()({
    issue: {
      insert: defineMutatorWithType<typeof schema>()<{
        id: string;
        value: number;
      }>(async ({tx, args}) => {
        await tx.mutate.issue.insert(args);
      }),
    },
  });
  const z = zeroForTest({
    schema,
    mutators,
  });
  await z.triggerConnected();
  const mockSocket = await z.socket;
  // reset mock socket messages to clear `initConnection` message
  mockSocket.messages.length = 0;

  await z.mutate(mutators.issue.insert({id: 'a', value: 1})).client;
  await z.triggerError({
    kind: ErrorKind.MutationRateLimited,
    message: 'Rate limit exceeded',
    origin: ErrorOrigin.Server,
  });

  await tickAFewTimes(vi, 0);
  expect(mockSocket.messages).toHaveLength(1);
  expect(mockSocket.closed).toBe(false);
  expect(z.connectionStatus).eq(ConnectionStatus.Connected);

  // reset messages
  mockSocket.messages.length = 0;

  // now send another mutation
  await z.mutate(mutators.issue.insert({id: 'b', value: 2})).client;
  await z.triggerError({
    kind: ErrorKind.MutationRateLimited,
    message: 'Rate limit exceeded',
    origin: ErrorOrigin.Server,
  });
  await tickAFewTimes(vi, 0);

  // two mutations should be sent in separate push messages
  expect(mockSocket.messages).toHaveLength(2);
  expect(
    mockSocket.messages.flatMap(m =>
      JSON.parse(m)[1].mutations.map((m: Mutation) => m.id),
    ),
  ).toEqual([1, 2]);
});

test('previously confirmed mutations are not resent after a rate limit error', async () => {
  const schema = createSchema({
    tables: [
      table('issue')
        .columns({
          id: string(),
          value: number(),
        })
        .primaryKey('id'),
    ],
  });
  const mutators = defineMutatorsWithType<typeof schema>()({
    issue: {
      insert: defineMutatorWithType<typeof schema>()<{
        id: string;
        value: number;
      }>(async ({tx, args}) => {
        await tx.mutate.issue.insert(args);
      }),
    },
  });
  const z = zeroForTest({
    schema,
    mutators,
  });
  await z.triggerConnected();
  const mockSocket = await z.socket;
  // reset mock socket messages to clear `initConnection` message
  mockSocket.messages.length = 0;

  await z.mutate(mutators.issue.insert({id: 'a', value: 1})).client;
  await tickAFewTimes(vi);
  // confirm the mutation
  await z.triggerPokeStart({
    pokeID: '1',
    baseCookie: null,
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
  await z.mutate(mutators.issue.insert({id: 'b', value: 2})).client;
  await z.triggerError({
    kind: ErrorKind.MutationRateLimited,
    message: 'Rate limit exceeded',
    origin: ErrorOrigin.Server,
  });
  await tickAFewTimes(vi);

  // Only the new mutation should have been sent. The first was confirmed by a poke response.
  expect(
    mockSocket.messages.flatMap(m =>
      JSON.parse(m)[1].mutations.map((m: Mutation) => m.id),
    ),
  ).toEqual([2]);
  mockSocket.messages.length = 0;

  // Send another mutation. This and the last rate limited mutation should be sent
  await z.mutate(mutators.issue.insert({id: 'c', value: 3})).client;
  await z.triggerError({
    kind: ErrorKind.MutationRateLimited,
    message: 'Rate limit exceeded',
    origin: ErrorOrigin.Server,
  });
  await tickAFewTimes(vi);

  expect(
    mockSocket.messages.flatMap(m =>
      JSON.parse(m)[1].mutations.map((m: Mutation) => m.id),
    ),
  ).toEqual([2, 3]);
});
