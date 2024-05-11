import type {LogContext} from '@rocicorp/logger';
import type {
  ConnectedMessage,
  ErrorKind,
  NullableVersion,
  Version,
} from 'reflect-protocol';
import type {AuthData} from 'reflect-shared/src/types.js';
import {assert} from 'shared/src/asserts.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import {
  ClientRecord,
  IncludeDeleted,
  getClientRecord,
  putClientRecord,
} from '../types/client-record.js';
import type {
  ClientID,
  ClientMap,
  ClientState,
  Socket,
} from '../types/client-state.js';
import {compareVersions, getVersion} from '../types/version.js';
import {decodeHeaderValue} from 'shared/src/headers.js';
import {closeWithError, send} from 'shared/src/cf/socket.js';
import {AUTH_DATA_HEADER_NAME} from './internal-headers.js';

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
  lc.info?.('handling connect', url.toString());
  const closeWithErrorLocal = (ek: ErrorKind, msg: string) => {
    closeWithError(lc, ws, ek, msg);
  };

  const {result, error} = getConnectRequest(url, headers);
  if (error !== null) {
    closeWithErrorLocal('InvalidConnectionRequest', error);
    return;
  }

  const {clientID, baseCookie, lmid, wsid, auth} = result;
  lc.info?.('parsed request', {
    ...result,
    auth: 'redacted',
  });

  const {
    clientID: requestClientID,
    baseCookie: requestBaseCookie,
    clientGroupID: requestClientGroupID,
  } = result;
  const existingRecord = await getClientRecord(
    requestClientID,
    IncludeDeleted.Include,
    storage,
  );
  lc.debug?.('Existing client record', existingRecord);

  if (existingRecord) {
    if (existingRecord.deleted) {
      lc.info?.(
        'Client with clientID',
        requestClientID,
        'is deleted and cannot reconnect.',
      );
      closeWithErrorLocal(
        'InvalidConnectionRequestClientDeleted',
        'Client is deleted',
      );
      return;
    }

    if (requestClientGroupID !== existingRecord.clientGroupID) {
      lc.info?.(
        'Unexpected client group id ',
        requestClientGroupID,
        ' received when connecting with a client id ',
        requestClientID,
        ' with existing client group id ',
        existingRecord.clientGroupID,
      );
      closeWithErrorLocal(
        'InvalidConnectionRequest',
        'Unexpected clientGroupID.',
      );
      return;
    }

    if (
      // Old records may not have userID.
      existingRecord.userID !== undefined &&
      existingRecord.userID !== auth.userID
    ) {
      lc.info?.(
        'Unexpected userID received',
        auth.userID,
        'when connecting to client with clientID',
        requestClientID,
        'which was created by userID',
        existingRecord.userID,
      );
      closeWithErrorLocal('InvalidConnectionRequest', 'Unexpected userID');
      return;
    }
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
    closeWithErrorLocal(
      'InvalidConnectionRequestLastMutationID',
      `Unexpected lmid.`,
    );
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
    closeWithErrorLocal(
      'InvalidConnectionRequestBaseCookie',
      `Unexpected baseCookie.`,
    );
    return;
  }

  const existingRecordLastMutationIDVersion: Version | null =
    existingRecord?.lastMutationIDVersion ?? null;

  const record: ClientRecord = {
    clientGroupID: requestClientGroupID,
    baseCookie: requestBaseCookie,
    lastMutationID: existingLastMutationID,
    lastMutationIDVersion: existingRecordLastMutationIDVersion,
    lastSeen: Date.now(),
    userID: auth.userID,
  };
  await putClientRecord(clientID, record, storage);
  lc.debug?.('Put client record', record);

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
    auth,
    clockOffsetMs: undefined,
    clientGroupID: requestClientGroupID,
    sentInitialPresence: false,
    debugPerf: result.debugPerf,
  };
  lc.debug?.('Setting client map entry', clientID, client);
  clients.set(clientID, client);

  const connectedMessage: ConnectedMessage = [
    'connected',
    {wsid, timestamp: Date.now()},
  ];
  lc.debug?.('sending connect message', url.toString());
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
        auth: AuthData;
        baseCookie: NullableVersion;
        timestamp: number;
        lmid: number;
        wsid: string;
        debugPerf: boolean;
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

  function getBooleanParam(name: string): boolean {
    const value = getParam(name, false);
    if (value === null) {
      return false;
    }
    return value === 'true';
  }

  const getAuthData = (headers: Headers): AuthData => {
    const encodedValue = headers.get(AUTH_DATA_HEADER_NAME);
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
    const wsid = getParam('wsid', false) ?? '';
    const debugPerf = getBooleanParam('debugPerf');

    const auth = getAuthData(headers);
    return {
      result: {
        clientID,
        clientGroupID,
        auth,
        baseCookie,
        timestamp,
        lmid,
        wsid,
        debugPerf,
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
