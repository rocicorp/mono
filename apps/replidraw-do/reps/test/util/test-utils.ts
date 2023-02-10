import { JSONType } from "../../src/protocol/json";
import { Mutation } from "../../src/protocol/push";
import { ClientMutation } from "../../src/types/client-mutation";
import { ClientID, ClientState, Socket } from "../../src/types/client-state";
import { NullableVersion } from "../../src/types/version";

export function client(
  id: ClientID,
  socket: Socket = new Mocket(),
  clockBehindByMs = 1,
  ...mutations: Mutation[]
): [ClientID, ClientState] {
  return [id, { clockBehindByMs, pending: mutations, socket }] as [
    string,
    ClientState
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
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  accept(): void {}
  log: string[][] = [];
  send(data: string): void {
    this.log.push(["send", data]);
  }
  close(): void {
    this.log.push(["close"]);
  }
  onclose: undefined;
  onmessage: undefined;
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
