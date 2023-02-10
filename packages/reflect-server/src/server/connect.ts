import {
  ClientRecord,
  getClientRecord,
  putClientRecord,
} from '../types/client-record.js';
import type {
  ClientID,
  ClientMap,
  ClientState,
  Socket,
} from '../types/client-state.js';
import type {LogContext} from '@rocicorp/logger';
import type {ConnectedMessage} from '../protocol/connected.js';
import type {UserData} from './auth.js';
import {USER_DATA_HEADER_NAME} from './auth.js';
import {decodeHeaderValue} from '../util/headers.js';
import {addConnectedClient} from '../types/connected-clients.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import {compareVersions, getVersion} from '../types/version.js';

export type MessageHandler = (
  clientID: ClientID,
  data: string,
  ws: Socket,
) => void;

export type CloseHandler = (clientID: ClientID, ws: Socket) => void;

/**
 * Handles the connect message from a client, registering the client state in memory and updating the persistent client-record.
 * @param ws socket connection to requesting client
 * @param url raw URL of connect request
 * @param clients currently running clients
 * @param onMessage message handler for this connection
 * @param onClose callback for when connection closes
 * @returns
 */
export async function handleConnection(
  lc: LogContext,
  ws: Socket,
  storage: DurableStorage,
  url: URL,
  headers: Headers,
  clients: ClientMap,
  onMessage: MessageHandler,
  onClose: CloseHandler,
) {
  lc.info?.('roomDO: handling connect', url.toString());
  const sendError = (error: string) => {
    lc.error?.('roomDO: invalid connection request', error);
    ws.send(JSON.stringify(['error', error]));
    ws.close();
  };

  const {result, error} = getConnectRequest(url, headers);
  if (error !== null) {
    sendError(error);
    return;
  }

  const {clientID, baseCookie, lmid, wsid, userData} = result;
  lc = lc.addContext('client', clientID).addContext('wsid', wsid);
  lc.info?.('parsed request', {
    ...result,
    userData: 'redacted',
  });

  const existingRecord = await getClientRecord(clientID, storage);
  lc.debug?.('Existing client record', existingRecord);
  const existingLastMutationID = existingRecord?.lastMutationID ?? 0;

  // These checks catch a large class of dev mistakes where the room is
  // re-created without clearing browser state. This can happen in a
  // variety of ways, e.g. it can happen when re-using the same roomID
  // across runs of `wrangler dev`.
  if (lmid > existingLastMutationID) {
    lc.info?.(
      'Unexpected lmid when connecting. Got',
      lmid,
      'expected lastMutationID',
      existingLastMutationID,
    );
    sendError(`Unexpected lmid. ${maybeOldClientStateMessage}`);
    return;
  }

  const version = (await getVersion(storage)) ?? null;
  if (compareVersions(baseCookie, version) > 0) {
    lc.info?.(
      'Unexpected baseCookie when connecting. Got',
      baseCookie,
      'current version is',
      version,
    );
    sendError(`Unexpected baseCookie. ${maybeOldClientStateMessage}`);
    return;
  }

  const record: ClientRecord = {
    baseCookie,
    lastMutationID: existingLastMutationID,
  };
  await putClientRecord(clientID, record, storage);
  lc.debug?.('Put client record', record);
  await addConnectedClient(clientID, storage);

  const existing = clients.get(clientID);
  if (existing) {
    lc.info?.('Closing old socket');
    existing.socket.close();
  }

  ws.addEventListener('message', event =>
    onMessage(clientID, event.data.toString(), ws),
  );
  ws.addEventListener('close', e => {
    lc.info?.('WebSocket CloseEvent for client', clientID, {
      reason: e.reason,
      code: e.code,
      wasClean: e.wasClean,
    });
    onClose(clientID, ws);
  });
  ws.addEventListener('error', e => {
    lc.error?.(
      'WebSocket ErrorEvent for client',
      clientID,
      {
        filename: e.filename,
        message: e.message,
        lineno: e.lineno,
        colno: e.colno,
      },
      e.error,
    );
  });

  const client: ClientState = {
    socket: ws,
    userData,
    clockBehindByMs: undefined,
    pending: [],
  };
  lc.debug?.('Setting client map entry', clientID, client);
  clients.set(clientID, client);

  const connectedMessage: ConnectedMessage = ['connected', {wsid}];
  ws.send(JSON.stringify(connectedMessage));
}

export const maybeOldClientStateMessage =
  'Possibly the room was re-created without also clearing browser state? Try clearing browser state and trying again.';

export function getConnectRequest(url: URL, headers: Headers) {
  function getParam(name: string, required: true): string;
  function getParam(name: string, required: boolean): string | null;
  function getParam(name: string, required: boolean) {
    const value = url.searchParams.get(name);
    if (value === '' || value === null) {
      if (required) {
        throw new Error(`invalid querystring - missing ${name}`);
      }
      return null;
    }
    return value;
  }

  function getIntegerParam(name: string, required: true): number;
  function getIntegerParam(name: string, required: boolean): number | null;
  function getIntegerParam(name: string, required: boolean) {
    const value = getParam(name, required);
    if (value === null) {
      return null;
    }
    const int = parseInt(value);
    if (isNaN(int)) {
      throw new Error(
        `invalid querystring parameter ${name}, url: ${url}, got: ${value}`,
      );
    }
    return int;
  }

  const getUserData = (headers: Headers): UserData => {
    const encodedValue = headers.get(USER_DATA_HEADER_NAME);
    if (!encodedValue) {
      throw new Error('missing user-data');
    }
    let jsonValue;
    try {
      jsonValue = JSON.parse(decodeHeaderValue(encodedValue));
    } catch (e) {
      throw new Error('invalid user-data - failed to decode/parse');
    }
    if (
      !jsonValue ||
      typeof jsonValue.userID !== 'string' ||
      jsonValue.userID === ''
    ) {
      throw new Error('invalid user-data - missing userID');
    }
    return jsonValue;
  };

  try {
    const clientID = getParam('clientID', true);
    const baseCookie = getIntegerParam('baseCookie', false);
    const timestamp = getIntegerParam('ts', true);
    const lmid = getIntegerParam('lmid', true);
    const wsid = getParam('wsid', true);

    const userData = getUserData(headers);
    return {
      result: {
        clientID,
        userData,
        baseCookie,
        timestamp,
        lmid,
        wsid,
      },
      error: null,
    };
  } catch (e) {
    return {
      result: null,
      error: String(e),
    };
  }
}
