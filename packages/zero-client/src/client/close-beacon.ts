import type {LogContext} from '@rocicorp/logger';
import type {
  CloseBeacon,
  CloseBeaconQueryParams,
} from 'reflect-protocol/src/close-beacon.js';
import {getConfig} from 'reflect-shared/src/config.js';
import {CLOSE_BEACON_PATH} from 'reflect-shared/src/paths.js';

type ReflectLike = {
  roomID: string;
  clientID: string;
  userID: string;
};

export class CloseBeaconManager {
  readonly #lc: LogContext;
  readonly #server: string | null;
  readonly #reflect: ReflectLike;
  readonly #auth: () => string | undefined;
  readonly #lastMutationID: () => number;
  readonly #signal: AbortSignal;
  #sent = false;

  constructor(
    reflect: ReflectLike,
    lc: LogContext,
    server: string | null,
    auth: () => string | undefined,
    lastMutationID: () => number,
    signal: AbortSignal,
    window: Window | undefined,
  ) {
    this.#reflect = reflect;
    this.#lc = lc;
    this.#server = server;
    this.#auth = auth;
    this.#lastMutationID = lastMutationID;
    this.#signal = signal;

    this.#initForPageHide(window);
  }

  #initForPageHide(window: Window | undefined) {
    if (getConfig('closeBeacon')) {
      window?.addEventListener(
        'pagehide',
        e => {
          // When pagehide fires and persisted is true it means the page is
          // going into the BFCache. If that happens the page might get restored
          // and this client will be operational again. If that happens we do
          // not want to remove the client from the server.
          if (e.persisted) {
            return;
          }
          this.send('Pagehide');
        },
        {signal: this.#signal},
      );
    }
  }

  send(reason: 'Pagehide' | 'ReflectClosed'): void {
    if (!getConfig('closeBeacon')) {
      return;
    }

    const lc = this.#lc.withContext('close-beacon', reason);

    if (this.#sent) {
      lc.debug?.('Not sending close beacon because already sent');
      return;
    }
    this.#sent = true;

    if (this.#server === null) {
      this.#lc.debug?.(
        `Not sending close beacon for ${reason} because server is null`,
      );
      return;
    }

    const lastMutationID = this.#lastMutationID();
    const auth = this.#auth();

    const url = new URL(CLOSE_BEACON_PATH, this.#server);
    const params: CloseBeaconQueryParams = {
      roomID: this.#reflect.roomID,
      userID: this.#reflect.userID,
      clientID: this.#reflect.clientID,
    };
    url.search = new URLSearchParams(params).toString();
    const body: CloseBeacon = {
      lastMutationID,
    };

    lc.debug?.('Sending close beacon', params, body);

    const headers: Record<string, string> = {
      'content-type': 'application/json',
    };
    if (auth) {
      headers['authorization'] = `Bearer ${auth}`;
    }
    fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
      keepalive: true,
    }).catch(e => {
      lc.info?.('Failed to send close beacon', e);
    });
  }
}
