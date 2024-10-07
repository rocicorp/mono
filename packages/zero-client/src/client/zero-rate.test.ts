import type {PushRequestV1} from 'replicache/src/mod.js';
import * as sinon from 'sinon';
import {afterEach, beforeEach, expect, test} from 'vitest';
import {ErrorKind} from 'zero-protocol/src/error.js';
import type {Mutation} from '../../../zero-protocol/src/push.js';
import {MockSocket, tickAFewTimes, zeroForTest} from './test-utils.js';
import {ConnectionState} from './zero.js';

let clock: sinon.SinonFakeTimers;
const startTime = 1678829450000;

beforeEach(() => {
  clock = sinon.useFakeTimers();
  clock.setSystemTime(startTime);
  sinon.replace(
    globalThis,
    'WebSocket',
    MockSocket as unknown as typeof WebSocket,
  );
});

afterEach(() => {
  sinon.restore();
});

test('connection stays alive on rate limit error', async () => {
  const z = zeroForTest();
  await z.triggerConnected();

  const mockSocket = await z.socket;

  const pushReq: PushRequestV1 = {
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
  await z.triggerError(ErrorKind.MutationRateLimited, 'Rate limit exceeded');

  expect(mockSocket.messages).to.have.lengthOf(1);
  expect(mockSocket.closed).toBe(false);
});

test('a mutation after a rate limit error causes limited mutations to be resent', async () => {
  const z = zeroForTest({
    schema: {
      version: 1,
      tables: {
        issue: {
          columns: {
            id: {type: 'string'},
            value: {type: 'number'},
          },
          primaryKey: ['id'],
          tableName: 'issues',
          relationships: {},
        },
      },
    },
  });
  await z.triggerConnected();
  const mockSocket = await z.socket;
  // reset mock socket messages to clear `initConnection` message
  mockSocket.messages.length = 0;

  await z.mutate.issue.create({id: 'a', value: 1});
  await z.triggerError(ErrorKind.MutationRateLimited, 'Rate limit exceeded');

  expect(mockSocket.messages).to.have.lengthOf(1);
  expect(mockSocket.closed).toBe(false);
  expect(z.connectionState).eq(ConnectionState.Connected);

  // reset messages
  mockSocket.messages.length = 0;

  // now send another mutation
  await z.mutate.issue.create({id: 'b', value: 2});
  await z.triggerError(ErrorKind.MutationRateLimited, 'Rate limit exceeded');
  await tickAFewTimes(clock, 0);

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
    schema: {
      version: 1,
      tables: {
        issue: {
          columns: {
            id: {type: 'string'},
            value: {type: 'number'},
          },
          primaryKey: ['id'],
          tableName: 'issues',
          relationships: {},
        },
      },
    },
  });
  await z.triggerConnected();
  const mockSocket = await z.socket;
  // reset mock socket messages to clear `initConnection` message
  mockSocket.messages.length = 0;

  await z.mutate.issue.create({id: 'a', value: 1});
  await tickAFewTimes(clock);
  // confirm the mutation
  await z.triggerPokeStart({
    pokeID: '1',
    baseCookie: null,
    cookie: '1',
  });
  await z.triggerPokePart({
    pokeID: '1',
    lastMutationIDChanges: {[z.clientID]: 1},
  });
  await z.triggerPokeEnd({pokeID: '1'});
  await tickAFewTimes(clock);

  // reset messages
  mockSocket.messages.length = 0;

  // now send another mutation but rate limit it
  await z.mutate.issue.create({id: 'b', value: 2});
  await z.triggerError(ErrorKind.MutationRateLimited, 'Rate limit exceeded');
  await tickAFewTimes(clock);

  // Only the new mutation should have been sent. The first was confirmed by a poke response.
  expect(
    mockSocket.messages.flatMap(m =>
      JSON.parse(m)[1].mutations.map((m: Mutation) => m.id),
    ),
  ).toEqual([2]);
  mockSocket.messages.length = 0;

  // Send another mutation. This and the last rate limited mutation should be sent
  await z.mutate.issue.create({id: 'c', value: 3});
  await z.triggerError(ErrorKind.MutationRateLimited, 'Rate limit exceeded');
  await tickAFewTimes(clock);

  expect(
    mockSocket.messages.flatMap(m =>
      JSON.parse(m)[1].mutations.map((m: Mutation) => m.id),
    ),
  ).toEqual([2, 3]);
});
