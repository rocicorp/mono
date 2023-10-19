import type {AuthData} from 'reflect-shared';

export type ClientID = string;
export type ClientGroupID = string;

export type ClientMap = Map<ClientID, ClientState>;

export interface ConnectionCountTracker {
  onConnectionCountChange(currentCount: number): void;
}

export class ConnectionCountTrackingClientMap
  extends Map<ClientID, ClientState>
  implements ClientMap
{
  #countTracker: ConnectionCountTracker;

  constructor(countTracker: ConnectionCountTracker) {
    super();
    this.#countTracker = countTracker;
  }

  #trackCount() {
    this.#countTracker.onConnectionCountChange(this.size);
  }

  clear(): void {
    super.clear();
    this.#trackCount();
  }

  delete(key: ClientID): boolean {
    if (super.delete(key)) {
      this.#trackCount();
      return true;
    }
    return false;
  }

  set(key: ClientID, value: ClientState): this {
    super.set(key, value);
    this.#trackCount();
    return this;
  }
}

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
  sentInitialPresence: boolean;
  // How long is the client's timestamp behind the local timestamp?
  // This is initialized in the first push message from the client, not
  // connect, which is why we need the |undefined here. We need to do that
  // because socket setup overhead is substantial and we will get a value
  // that is far too high if we use connection.
  clockOffsetMs: number | undefined;
  // Should extra performance debug information be sent to this client?
  debugPerf: boolean;
};
