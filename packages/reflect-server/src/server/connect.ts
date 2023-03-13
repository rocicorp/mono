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
import type {ConnectedMessage} from 'reflect-protocol';
import type {UserData} from './auth.js';
import {USER_DATA_HEADER_NAME} from './auth.js';
import {decodeHeaderValue} from '../util/headers.js';
import {addConnectedClient} from '../types/connected-clients.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import {compareVersions, getVersion} from '../types/version.js';
import type {NullableVersion, Version} from 'reflect-protocol';
import {send, closeWithError} from '../util/socket.js';
import {assert} from '../util/asserts.js';
import {ErrorKind} from 'reflect-protocol';

export type MessageHandler = (
  lc: LogContext,
  clientID: ClientID,
  data: string,
  ws: Socket,
) => void;

export type CloseHandler = (
  lc: LogContext,
  clientID: ClientID,
  ws: Socket,
) => void;

export const maybeOldClientStateMessage =
  'Possibly the room was re-created without also clearing browser state? Try clearing browser state and trying again.';

/**
 * Handles the connect message from a client, registering the client state in
 * memory and updating the persistent client-record.
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
): Promise<void> {
  lc.info?.('roomDO: handling connect', url.toString());
  const closeWithErrorLocal = (msg: string) => {
    closeWithError(lc, ws, ErrorKind.InvalidConnectionRequest, msg);
  };

  const {result, error} = getConnectRequest(url, headers);
  if (error !== null) {
    closeWithErrorLocal(error);
    return;
  }

  const {clientID, baseCookie, lmid, wsid, userData} = result;
  lc = lc.addContext('client', clientID).addContext('wsid', wsid);
  lc.info?.('parsed request', {
    ...result,
    userData: 'redacted',
  });

  const {
    clientID: requestClientID,
    baseCookie: requestBaseCookie,
    clientGroupID: requestClientGroupID,
  } = result;
  const existingRecord = await getClientRecord(requestClientID, storage);
  lc.debug?.('Existing client record', existingRecord);

  if (existingRecord && requestClientGroupID !== existingRecord.clientGroupID) {
    lc.info?.(
      'Unexpected client group id ',
      requestClientGroupID,
      ' received when connecting with a client id ',
      requestClientID,
      ' with existing client group id ',
      existingRecord.clientGroupID,
    );
    closeWithErrorLocal('Unexpected clientGroupID.');
    return;
  }

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
    closeWithErrorLocal(`Unexpected lmid. ${maybeOldClientStateMessage}`);
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
    closeWithErrorLocal(`Unexpected baseCookie. ${maybeOldClientStateMessage}`);
    return;
  }

  const existingRecordLastMutationIDVersion: Version | null =
    existingRecord?.lastMutationIDVersion ?? null;

  const record: ClientRecord = {
    clientGroupID: requestClientGroupID,
    baseCookie: requestBaseCookie,
    lastMutationID: existingLastMutationID,
    lastMutationIDVersion: existingRecordLastMutationIDVersion,
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
    onMessage(lc, clientID, event.data.toString(), ws),
  );
  ws.addEventListener('close', e => {
    lc.info?.('WebSocket CloseEvent for client', clientID, {
      reason: e.reason,
      code: e.code,
      wasClean: e.wasClean,
    });
    onClose(lc, clientID, ws);
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
    clientGroupID: requestClientGroupID,
  };
  lc.debug?.('Setting client map entry', clientID, client);
  clients.set(clientID, client);

  const connectedMessage: ConnectedMessage = ['connected', {wsid}];
  send(ws, connectedMessage);
}

export function getConnectRequest(
  url: URL,
  headers: Headers,
):
  | {
      result: {
        clientID: string;
        clientGroupID: string;
        userData: UserData;
        baseCookie: NullableVersion;
        timestamp: number;
        lmid: number;
        wsid: string;
      };
      error: null;
    }
  | {
      result: null;
      error: string;
    } {
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
        `invalid querystring parameter ${name}, got: ${value}, url: ${url}`,
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
    const clientGroupID = getParam('clientGroupID', true);
    const baseCookie = getIntegerParam('baseCookie', false);
    const timestamp = getIntegerParam('ts', true);
    const lmid = getIntegerParam('lmid', true);
    const wsid = getParam('wsid', true);

    const userData = getUserData(headers);
    return {
      result: {
        clientID,
        clientGroupID,
        userData,
        baseCookie,
        timestamp,
        lmid,
        wsid,
      },
      error: null,
    };
  } catch (e) {
    assert(e instanceof Error);

    return {
      result: null,
      error: e.message,
    };
  }
}
