import type {LogContext} from '@rocicorp/logger';
import type {
  DisconnectBeacon,
  DisconnectBeaconQueryParams,
} from 'reflect-protocol/src/disconnect-beacon.js';
import {getConfig} from 'reflect-shared/src/config.js';
import {DISCONNECT_BEACON_PATH} from 'reflect-shared/src/paths.js';

export function sendDisconnectBeacon(
  lc: LogContext,
  server: string | null,
  roomID: string,
  userID: string,
  clientID: string,
  auth: string | undefined,
  lastMutationID: number,
  reason: 'Pagehide' | 'ReflectClosed',
): void {
  if (!getConfig('disconnectBeacon')) {
    return;
  }

  if (server === null) {
    lc.debug?.(
      `Not sending disconnect beacon for ${reason} because server is null`,
    );
    return;
  }

  lc = lc.withContext('disconnect-beacon', reason);
  lc.debug?.('Sending disconnect beacon', {server, clientID, lastMutationID});

  const url = new URL(DISCONNECT_BEACON_PATH, server);
  const params: DisconnectBeaconQueryParams = {
    roomID,
    userID,
    clientID,
  };
  url.search = new URLSearchParams(params).toString();
  const body: DisconnectBeacon = {
    lastMutationID,
  };

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

export function initDisconnectBeaconForPageHide(
  lc: LogContext,
  window: Window | undefined,
  signal: AbortSignal,
  server: string | null,
  roomID: string,
  userID: string,
  clientID: string,
  auth: () => string | undefined,
  lastMutationID: () => number,
): void {
  if (getConfig('disconnectBeacon')) {
    window?.addEventListener(
      'pagehide',
      e => {
        // When store in BFCache we don't want to send a disconnect beacon.
        if (e.persisted) {
          return;
        }
        sendDisconnectBeacon(
          lc,
          server,
          roomID,
          userID,
          clientID,
          auth(),
          lastMutationID(),
          'Pagehide',
        );
      },
      {signal},
    );
  }
}
