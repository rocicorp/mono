import {afterEach, beforeEach, jest} from '@jest/globals';
import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import type {Mutation, NullableVersion} from 'reflect-protocol';
import type {ReadonlyJSONValue} from 'shared/json.js';
import type {ClientRecord} from '../../src/types/client-record.js';
import type {
  ClientGroupID,
  ClientID,
  ClientState,
  Socket,
} from '../../src/types/client-state.js';
import type {PendingMutation} from '../types/mutation.js';

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
): [ClientID, ClientState] {
  return [
    id,
    {
      socket,
      userData: {userID},
      clientGroupID,
      clockOffsetMs: clockBehindByMs,
      debugPerf,
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
}): PendingMutation {
  const {
    clientID,
    clientGroupID,
    id,
    timestamps,
    pusherClientIDs = new Set([clientID]),
    name = 'foo',
    args = [],
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

export function clientRecord(
  clientGroupID: ClientGroupID,
  baseCookie: NullableVersion = null,
  lastMutationID = 1,
  lastMutationIDVersion: NullableVersion = 1,
): ClientRecord {
  return {
    clientGroupID,
    baseCookie,
    lastMutationID,
    lastMutationIDVersion,
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
  messages: [LogLevel, ...unknown[]][] = [];

  log(level: LogLevel, ...args: unknown[]): void {
    this.messages.push([level, ...args]);
  }
}

export class SilentLogSink implements LogSink {
  log(_level: LogLevel, ..._args: unknown[]): void {
    return;
  }
}

export function createSilentLogContext() {
  return new LogContext('error', new SilentLogSink());
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
