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
import {
  NullableVersion,
  Version,
  compareVersions,
  getVersion,
} from '../types/version.js';

export const maybeOldClientStateMessage =
  'Possibly the room was re-created without also clearing browser state? Try clearing browser state and trying again.';

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
  const sendError = (error: string) => {
    lc.info?.('invalid connection request', error);
    ws.send(JSON.stringify(['error', error]));
    ws.close();
  };

  const req = getConnectRequest(url, headers);
  const {result: parsedConnectRequest} = req;
  if (parsedConnectRequest === null) {
    const {error} = req;
    sendError(error);
    return;
  }

  lc = lc.addContext('client', parsedConnectRequest.clientID);
  lc.info?.('parsed request', {
    ...parsedConnectRequest,
    userData: 'redacted',
  });

  const {
    clientID: requestClientID,
    baseCookie: requestBaseCookie,
    clientGroupID: requestClientGroupID,
  } = parsedConnectRequest;
  const existingRecord = await getClientRecord(requestClientID, storage);
  lc.debug?.('Existing client record', existingRecord);

  if (existingRecord) {
    if (requestClientGroupID !== existingRecord.clientGroupID) {
      lc.info?.(
        'Unexpected client group id ',
        requestClientGroupID,
        ' received when connecting with a client id ',
        requestClientID,
        ' with existing client group id ',
        existingRecord.clientGroupID,
      );
      sendError('Unexpected clientGroupID.');
      return;
    }
  }

  const existingLastMutationID = existingRecord?.lastMutationID ?? 0;
  // These checks catch a large class of dev mistakes where the room is
  // re-created without clearing browser state. This can happen in a
  // variety of ways, e.g. it can happen when re-using the same roomID
  // across runs of `wrangler dev`.
  if (parsedConnectRequest.lmid > existingLastMutationID) {
    lc.info?.(
      'Unexpected lmid when connecting. Got',
      parsedConnectRequest.lmid,
      'expected lastMutationID',
      existingLastMutationID,
    );
    sendError(`Unexpected lmid. ${maybeOldClientStateMessage}`);
    return;
  }

  const version = (await getVersion(storage)) ?? null;
  if (compareVersions(requestBaseCookie, version) > 0) {
    lc.info?.(
      'Unexpected baseCookie when connecting. Got',
      requestBaseCookie,
      'current version is',
      version,
    );
    sendError(`Unexpected baseCookie. ${maybeOldClientStateMessage}`);
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
  await putClientRecord(requestClientID, record, storage);
  lc.debug?.('Put client record', record);
  await addConnectedClient(requestClientID, storage);

  const existing = clients.get(requestClientID);
  if (existing) {
    lc.debug?.('Closing old socket');
    existing.socket.close();
  }

  ws.addEventListener('message', event =>
    onMessage(requestClientID, event.data.toString(), ws),
  );
  ws.addEventListener('close', e => {
    lc.info?.('WebSocket CloseEvent for client', requestClientID, {
      reason: e.reason,
      code: e.code,
      wasClean: e.wasClean,
    });
    onClose(requestClientID, ws);
  });
  ws.addEventListener('error', e => {
    lc.error?.(
      'WebSocket ErrorEvent for client',
      requestClientID,
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
    userData: parsedConnectRequest.userData,
    clockBehindByMs: undefined,
    clientGroupID: requestClientGroupID,
  };
  lc.debug?.('Setting client map entry', requestClientID, client);
  clients.set(requestClientID, client);
  const connectedMessage: ConnectedMessage = ['connected', {}];
  ws.send(JSON.stringify(connectedMessage));
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
      };
      error: null;
    }
  | {
      result: null;
      error: string;
    } {
  const getParam = (name: string, required: boolean) => {
    const value = url.searchParams.get(name);
    if (value === '' || value === null) {
      if (required) {
        throw new Error(`invalid querystring - missing ${name}`);
      }
      return null;
    }
    return value;
  };
  const getIntegerParam = (name: string, required: boolean) => {
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
  };

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
    if (!jsonValue.userID) {
      throw new Error('invalid user-data - missing userID');
    }
    return jsonValue;
  };

  try {
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const clientID = getParam('clientID', true)!;
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const clientGroupID = getParam('clientGroupID', true)!;
    const baseCookie = getIntegerParam('baseCookie', false);
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const timestamp = getIntegerParam('ts', true)!;
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const lmid = getIntegerParam('lmid', true)!;

    const userData = getUserData(headers);
    return {
      result: {
        clientID,
        clientGroupID,
        userData,
        baseCookie,
        timestamp,
        lmid,
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
