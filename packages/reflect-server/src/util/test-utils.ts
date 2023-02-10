import { LogContext, LogLevel, LogSink } from "@rocicorp/logger";
import type { JSONType } from "../../src/protocol/json.js";
import type { Mutation } from "../../src/protocol/push.js";
import type { ClientMutation } from "../../src/types/client-mutation.js";
import type {
  ClientID,
  ClientState,
  Socket,
} from "../../src/types/client-state.js";
import type { NullableVersion } from "../../src/types/version.js";

export function client(
  id: ClientID,
  userID: string,
  socket: Socket = new Mocket(),
  clockBehindByMs = 1,
  ...mutations: Mutation[]
): [ClientID, ClientState] {
  return [
    id,
    {
      clockBehindByMs,
      pending: mutations,
      socket,
      userData: { userID },
    },
  ];
}

export function mutation(
  id: number,
  name = "foo",
  args: JSONType = [],
  timestamp = 1
): Mutation {
  return {
    id,
    name,
    args,
    timestamp,
  };
}

export function clientMutation(
  clientID: ClientID,
  id: number,
  name = "foo",
  args: JSONType = [],
  timestamp = 1
): ClientMutation {
  return {
    clientID,
    ...mutation(id, name, args, timestamp),
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
    this.log.push(["send", data]);
  }

  close(): void {
    this.log.push(["close"]);
  }
}

export function clientRecord(
  baseCookie: NullableVersion = null,
  lastMutationID = 1
) {
  return {
    baseCookie,
    lastMutationID,
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
  return new LogContext("error", new SilentLogSink());
}
