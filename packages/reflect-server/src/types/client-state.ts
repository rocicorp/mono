import type {AuthData} from 'reflect-types';

export type ClientID = string;
export type ClientGroupID = string;

export type ClientMap = Map<ClientID, ClientState>;

export interface Socket extends EventTarget<WebSocketEventMap> {
  accept(): void;
  send(message: ArrayBuffer | string): void;
  close(code?: number, reason?: string): void;
  readonly readyState: number;
}

export type ClientState = {
  socket: Socket;
  auth: AuthData;
  clientGroupID: ClientGroupID;
  // How long is the client's timestamp behind the local timestamp?
  // This is initialized in the first push message from the client, not
  // connect, which is why we need the |undefined here. We need to do that
  // because socket setup overhead is substantial and we will get a value
  // that is far too high if we use connection.
  clockOffsetMs: number | undefined;
  // Should extra performance debug information be sent to this client?
  debugPerf: boolean;
};
