import {afterEach, beforeEach, jest} from '@jest/globals';
import {Context, LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import type {Mutation, NullableVersion} from 'reflect-protocol';
import type {AuthData} from 'reflect-shared/src/types.js';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import type {ClientRecord} from '../../src/types/client-record.js';
import type {
  ClientGroupID,
  ClientID,
  ClientState,
  Socket,
} from '../../src/types/client-state.js';
import type {Storage} from '../storage/storage.js';
import type {PendingMutation} from '../types/mutation.js';
import {userValueKey} from '../types/user-value.js';

afterEach(() => {
  jest.restoreAllMocks();
});

export function pendingMutationsEntry(
  id: ClientGroupID,
  ...mutations: Mutation[]
): [ClientGroupID, Mutation[]] {
  return [id, mutations];
}

export function client(
  id: ClientID,
  userID: string,
  clientGroupID: ClientGroupID,
  socket: Socket = new Mocket(),
  clockBehindByMs?: number | undefined,
  debugPerf = false,
  sentInitialPresence = true,
): [ClientID, ClientState] {
  return [
    id,
    {
      socket,
      auth: {userID},
      clientGroupID,
      clockOffsetMs: clockBehindByMs,
      debugPerf,
      sentInitialPresence,
    },
  ];
}

export function mutation(
  clientID: ClientID,
  id: number,
  timestamp = 1,
  name = 'foo',
  args: ReadonlyJSONValue = [],
): Mutation {
  return {
    clientID,
    id,
    name,
    args,
    timestamp,
  };
}

export function pendingMutation(opts: {
  clientID: ClientID;
  clientGroupID: ClientGroupID;
  id: number;
  timestamps:
    | {
        normalizedTimestamp: number;
        originTimestamp: number;
        serverReceivedTimestamp: number;
      }
    | number
    | undefined;
  pusherClientIDs?: Set<ClientID>;
  name?: string;
  args?: ReadonlyJSONValue;
  auth?: AuthData | undefined;
}): PendingMutation {
  const {
    clientID,
    clientGroupID,
    id,
    timestamps,
    pusherClientIDs = new Set([clientID]),
    name = 'foo',
    args = [],
    auth = {userID: 'testUser1'},
  } = opts;
  return {
    clientID,
    clientGroupID,
    id,
    name,
    args,
    timestamps:
      typeof timestamps === 'number'
        ? {
            normalizedTimestamp: timestamps,
            originTimestamp: timestamps,
            serverReceivedTimestamp: timestamps,
          }
        : timestamps,
    pusherClientIDs,
    auth,
  };
}

export class Mocket extends EventTarget implements Socket {
  log: unknown[][] = [];
  readyState = 1;
  onclose = undefined;
  onmessage = undefined;

  readonly url: string | null = null;
  readonly protocol: string | null = null;
  readonly extensions: string | null = null;

  accept(): void {
    // noop
  }

  send(data: string): void {
    this.log.push(['send', data]);
  }

  close(code?: number, reason?: string): void;
  close(...args: unknown[]): void {
    this.log.push(['close', ...args]);
  }
}

export function clientRecord({
  clientGroupID,
  baseCookie = null,
  lastMutationID = 1,
  lastMutationIDVersion = 1,
  lastSeen = 1000,
  userID = 'testUser1',
}: {
  clientGroupID: ClientGroupID;
  baseCookie?: NullableVersion | undefined;
  lastMutationID?: number | undefined;
  lastMutationIDVersion?: NullableVersion | undefined;
  lastSeen?: number | undefined;
  userID?: string | undefined;
}): ClientRecord {
  return {
    clientGroupID,
    baseCookie,
    lastMutationID,
    lastMutationIDVersion,
    lastSeen,
    userID,
  };
}

export function userValue(
  value: ReadonlyJSONValue,
  version = 1,
  deleted = false,
) {
  return {
    value,
    version,
    deleted,
  };
}

export function fail(s: string): never {
  throw new Error(s);
}

export class TestLogSink implements LogSink {
  messages: [LogLevel, Context | undefined, unknown[]][] = [];

  log(level: LogLevel, context: Context | undefined, ...args: unknown[]): void {
    this.messages.push([level, context, args]);
  }
}

export class SilentLogSink implements LogSink {
  log(_l: LogLevel, _c: Context | undefined, ..._args: unknown[]): void {
    return;
  }
}

export function createSilentLogContext() {
  return new LogContext('error', undefined, new SilentLogSink());
}

export function mockMathRandom() {
  const {random} = Math;

  beforeEach(() => {
    // If we need more entropy use a PRNG.
    Math.random = () => 0.1234;
  });

  afterEach(() => {
    Math.random = random;
  });
}

export function mockWebSocketPair(): [Mocket, Mocket] {
  const client = new Mocket();
  const server = new Mocket();
  jest
    .spyOn(
      globalThis,
      // @ts-expect-error TS does not know about WebSocketPair
      'WebSocketPair',
    )
    // @ts-expect-error TS does not know about WebSocketPair
    .mockReturnValue({0: client, 1: server});

  return [client, server];
}
export async function setUserEntries(
  cache: Storage,
  version: number,
  entries: Record<string, ReadonlyJSONValue>,
) {
  for (const [k, value] of Object.entries(entries)) {
    await cache.put(userValueKey(k), {
      value,
      deleted: false,
      version,
    });
  }
}
