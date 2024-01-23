import type {LogContext} from '@rocicorp/logger';
import type {
  DisconnectBeacon,
  DisconnectBeaconQueryParams,
} from 'reflect-protocol/src/disconnect-beacon.js';
import {getConfig} from 'reflect-shared/src/config.js';
import {DISCONNECT_BEACON_PATH} from 'reflect-shared/src/paths.js';

type ReflectLike = {
  roomID: string;
  clientID: string;
  userID: string;
};

export class DisconnectBeaconManager {
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
    if (getConfig('disconnectBeacon')) {
      window?.addEventListener(
        'pagehide',
        e => {
          // When store in BFCache we don't want to send a disconnect beacon.
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
    if (!getConfig('disconnectBeacon')) {
      return;
    }

    const lc = this.#lc.withContext('disconnect-beacon', reason);

    if (this.#sent) {
      lc.debug?.('Not sending disconnect beacon because already sent');
      return;
    }
    this.#sent = true;

    if (this.#server === null) {
      this.#lc.debug?.(
        `Not sending disconnect beacon for ${reason} because server is null`,
      );
      return;
    }

    const lastMutationID = this.#lastMutationID();
    const auth = this.#auth();

    const url = new URL(DISCONNECT_BEACON_PATH, this.#server);
    const params: DisconnectBeaconQueryParams = {
      roomID: this.#reflect.roomID,
      userID: this.#reflect.userID,
      clientID: this.#reflect.clientID,
    };
    url.search = new URLSearchParams(params).toString();
    const body: DisconnectBeacon = {
      lastMutationID,
    };

    lc.debug?.('Sending disconnect beacon', params, body);

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
      lc.info?.('Failed to send disconnect beacon', e);
    });
  }
}
