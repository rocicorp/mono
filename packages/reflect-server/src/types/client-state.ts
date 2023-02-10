import type { UserData } from "../server/auth.js";
import type { Mutation } from "../protocol/push.js";

export type ClientID = string;

export type ClientMap = Map<ClientID, ClientState>;

export interface Socket extends EventTarget<WebSocketEventMap> {
  accept(): void;
  send(message: ArrayBuffer | string): void;
  close(code?: number, reason?: string): void;
  readonly readyState: number;
}

export type ClientState = {
  socket: Socket;
  userData: UserData;
  // A list of mutations awaiting application from this client. Sorted by
  // lastMutationID and de-duplicated. The timestamps in these mutations
  // are in the server's timeframe. Note that they will generally increase
  // with respect to mutationID but that is not guaranteed.
  pending: Mutation[];

  // How long is the client's timestamp behind the local timestamp?
  // This is initialized in the first push message from the client, not
  // connect, which is why we need the |undefined here. We need to do that
  // because socket setup overhead is substantial and we will get a value
  // that is far too high if we use connection.
  clockBehindByMs: number | undefined;
};
