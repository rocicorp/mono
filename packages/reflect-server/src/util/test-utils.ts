import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import type {ClientRecord} from 'src/types/client-record.js';
import type {JSONType} from '../../src/protocol/json.js';
import type {Mutation} from '../../src/protocol/push.js';
import type {
  ClientGroupID,
  ClientID,
  ClientState,
  Socket,
} from '../../src/types/client-state.js';
import type {NullableVersion} from '../../src/types/version.js';

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
  clockBehindByMs = 1,
): [ClientID, ClientState] {
  return [
    id,
    {
      socket,
      userData: {userID},
      clientGroupID,
      clockBehindByMs,
    },
  ];
}

export function mutation(
  clientID: ClientID,
  id: number,
  name = 'foo',
  args: JSONType = [],
  timestamp = 1,
): Mutation {
  return {
    clientID,
    id,
    name,
    args,
    timestamp,
  };
}

export class Mocket extends EventTarget implements Socket {
  log: string[][] = [];
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

  close(): void {
    this.log.push(['close']);
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

export function userValue(value: JSONType, version = 1, deleted = false) {
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
