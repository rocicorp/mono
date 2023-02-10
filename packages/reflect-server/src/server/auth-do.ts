import {encodeHeaderValue} from '../util/headers.js';
import {randomID} from '../util/rand.js';
import {LogSink, LogContext, LogLevel} from '@rocicorp/logger';
import {version} from '../util/version.js';
import {AuthHandler, UserData, USER_DATA_HEADER_NAME} from './auth.js';
import {dispatch, paths} from './dispatch.js';
import {
  closeRoom,
  createRoom,
  createRoomRecordForLegacyRoom,
  deleteRoom,
  deleteRoomRecord,
  objectIDByRoomID,
  roomRecordByRoomID,
  roomRecords,
  RoomStatus,
} from './rooms.js';
import {RWLock} from '@rocicorp/lock';
import {
  ConnectionsResponse,
  connectionsResponseSchema,
  InvalidateForRoomRequest,
  InvalidateForUserRequest,
} from '../protocol/api/auth.js';
import {assert} from 'superstruct';
import {createAuthAPIHeaders} from './auth-api-headers.js';
import {DurableStorage} from '../storage/durable-storage.js';
import type {CreateRoomRequest} from '../protocol/api/room.js';
import {
  newWebSocketPair as defaultNewWebSocketPair,
  sendError,
  NewWebSocketPair,
} from '../util/socket.js';
import {
  requireAuthAPIKey,
  Handler,
  Router,
  asJSON,
  withRoomID,
  get,
  post,
  BaseContext,
  WithRoomID,
} from './router.js';

export interface AuthDOOptions {
  roomDO: DurableObjectNamespace;
  state: DurableObjectState;
  authHandler: AuthHandler;
  authApiKey: string;
  logSink: LogSink;
  logLevel: LogLevel;
  // newWebSocketPair is a seam we use for testing. I cannot figure out
  // how to get jest to mock a module.
  newWebSocketPair?: NewWebSocketPair | undefined;
}
export type ConnectionKey = {
  userID: string;
  roomID: string;
  clientID: string;
};
export type ConnectionRecord = {
  connectTimestamp: number;
};

export const AUTH_ROUTES = {
  roomStatusByRoomID: '/api/room/v0/room/:roomID/status',
  roomRecords: '/api/room/v0/rooms',
  closeRoom: '/api/room/v0/room/:roomID/close',
  deleteRoom: '/api/room/v0/room/:roomID/delete',
  migrateRoom: '/api/room/v0/room/:roomID/migrate/1',
  forgetRoom: '/api/room/v0/room/:roomID/DANGER/forget',
};

export class BaseAuthDO implements DurableObject {
  private readonly _router: Router;
  private readonly _roomDO: DurableObjectNamespace;
  private readonly _state: DurableObjectState;
  // _durableStorage is a type-aware wrapper around _state.storage. It
  // always disables the input gate. The output gate is configured in the
  // constructor below. Anything that needs to read *values* out of
  // storage should probably use _durableStorage, and not _state.storage
  // directly.
  private readonly _durableStorage: DurableStorage;
  private readonly _authHandler: AuthHandler;
  private readonly _authApiKey: string;
  private readonly _lc: LogContext;

  // _authLock ensures that at most one auth api call is processed at a time.
  // For safety, if something requires both the auth lock and the room record
  // lock, the auth lock MUST be acquired first.
  private readonly _authLock: RWLock;
  // _roomRecordLock ensure that at most one write operation is in
  // progress on a RoomRecord at a time. For safety, if something requires
  // both the auth lock and the room record lock, the auth lock MUST be
  // acquired first.
  private readonly _roomRecordLock: RWLock;
  private readonly _newWebSocketPair: NewWebSocketPair;

  constructor(options: AuthDOOptions) {
    const {
      roomDO,
      state,
      authHandler,
      authApiKey,
      logSink,
      logLevel,
      newWebSocketPair = defaultNewWebSocketPair,
    } = options;
    this._router = new Router();
    this._newWebSocketPair = newWebSocketPair;
    this._roomDO = roomDO;
    this._state = state;
    this._durableStorage = new DurableStorage(
      state.storage,
      false, // don't allow unconfirmed
    );
    this._authHandler = authHandler;
    this._authApiKey = authApiKey;
    this._lc = new LogContext(logLevel, logSink)
      .addContext('AuthDO')
      .addContext('doID', state.id.toString());
    this._authLock = new RWLock();
    this._roomRecordLock = new RWLock();
    this._initRoutes();
    this._lc.info?.('Starting server');
    this._lc.info?.('Version:', version);
  }

  async fetch(request: Request): Promise<Response> {
    // Match route against pattern /:name/*action
    const lc = this._lc.addContext('req', randomID());
    lc.debug?.('Handling request:', request.url);
    try {
      // Try newfangled routes first.
      let resp = await this._router.dispatch(request, {lc});
      // If not handled, use dispatch routes.
      // TODO: change dispatch to return 404 in this case once everything is converted.
      if (resp === undefined) {
        resp = await dispatch(request, lc, this._authApiKey, this);
      }
      lc.debug?.(`Returning response: ${resp.status} ${resp.statusText}`);
      return resp;
    } catch (e) {
      lc.error?.('Unhandled exception in fetch', e);
      return new Response(
        e instanceof Error ? e.message : 'Unexpected error.',
        {
          status: 500,
        },
      );
    }
  }

  private _requireAPIKey = <Context extends BaseContext, Resp>(
    next: Handler<Context, Resp>,
  ) => requireAuthAPIKey(() => this._authApiKey, next);

  private _roomStatusByRoomID = get(
    this._requireAPIKey(
      withRoomID(
        asJSON(async (_, ctx: BaseContext & WithRoomID) => {
          const roomRecord = await this._roomRecordLock.withRead(() =>
            roomRecordByRoomID(this._durableStorage, ctx.roomID),
          );
          if (roomRecord === undefined) {
            return {status: RoomStatus.Unknown};
          }
          return {status: roomRecord.status};
        }),
      ),
    ),
  );

  private _allRoomRecords = get(
    this._requireAPIKey(
      asJSON(async () => {
        const roomIDToRecords = await this._roomRecordLock.withRead(() =>
          roomRecords(this._durableStorage),
        );
        return Array.from(roomIDToRecords);
      }),
    ),
  );

  createRoom(
    lc: LogContext,
    request: Request,
    validatedBody: CreateRoomRequest,
  ) {
    return this._roomRecordLock.withWrite(() =>
      createRoom(
        lc,
        this._roomDO,
        this._durableStorage,
        request,
        validatedBody,
      ),
    );
  }

  // A call to closeRoom should be followed by a call to authInvalidateForRoom
  // to ensure users are logged out.
  private _closeRoom = post(
    this._requireAPIKey(
      withRoomID((_, ctx) =>
        this._roomRecordLock.withWrite(() =>
          closeRoom(ctx.lc, this._durableStorage, ctx.roomID),
        ),
      ),
    ),
  );

  // A room must first be closed before it can be deleted. Once deleted, a room
  // will return 410 Gone for all requests.
  private _deleteRoom = post(
    this._requireAPIKey(
      withRoomID((req, ctx) =>
        this._roomRecordLock.withWrite(() =>
          deleteRoom(
            ctx.lc,
            this._roomDO,
            this._durableStorage,
            ctx.roomID,
            req,
          ),
        ),
      ),
    ),
  );

  // This is a DANGEROUS call: it removes the RoomRecord for the given
  // room, potentially orphaning the roomDO. It doesn't log users out
  // or delete the room's data, it just forgets about the room.
  // It is useful if you are testing migration, or if you are developing
  // in reflect-server.
  private _forgetRoom = post(
    this._requireAPIKey(
      withRoomID((_, ctx) =>
        this._roomRecordLock.withWrite(() =>
          deleteRoomRecord(ctx.lc, this._durableStorage, ctx.roomID),
        ),
      ),
    ),
  );

  // This call creates a RoomRecord for a room that was created via the
  // old mechanism of deriving room objectID from the roomID via
  // idFromString(). It overwrites any existing RoomRecord for the room. It
  // does not check that the room actually exists.
  private _migrateRoom = post(
    this._requireAPIKey(
      withRoomID((_, ctx) =>
        this._roomRecordLock.withWrite(() =>
          createRoomRecordForLegacyRoom(
            ctx.lc,
            this._roomDO,
            this._durableStorage,
            ctx.roomID,
          ),
        ),
      ),
    ),
  );

  private _initRoutes() {
    this._router.register(
      AUTH_ROUTES.roomStatusByRoomID,
      this._roomStatusByRoomID,
    );
    this._router.register(AUTH_ROUTES.roomRecords, this._allRoomRecords);
    this._router.register(AUTH_ROUTES.closeRoom, this._closeRoom);
    this._router.register(AUTH_ROUTES.deleteRoom, this._deleteRoom);
    this._router.register(AUTH_ROUTES.migrateRoom, this._migrateRoom);
    this._router.register(AUTH_ROUTES.forgetRoom, this._forgetRoom);
  }

  // eslint-disable-next-line require-await
  async connect(lc: LogContext, request: Request): Promise<Response> {
    lc.info?.('authDO received websocket connection request:', request.url);
    const url = new URL(request.url);
    if (url.pathname !== '/connect') {
      lc.error?.('authDO returning 400 bc path is not /connect:', request.url);
      return new Response('unknown route', {
        status: 400,
      });
    }

    if (request.headers.get('Upgrade') !== 'websocket') {
      lc.error?.(
        'authDO returning 400 bc missing Upgrade header:',
        request.url,
      );
      return new Response('expected websocket', {status: 400});
    }

    const encodedAuth = request.headers.get('Sec-WebSocket-Protocol');
    if (!encodedAuth) {
      lc.error?.('authDO auth not found in Sec-WebSocket-Protocol header.');
      return createUnauthorizedResponse('auth required');
    }

    // From this point forward we want to return errors over the websocket so
    // the client can see them.
    //
    // To report an error in the HTTP upgrade request we accept the upgrade
    // request and send the error over the websocket. This is because the
    // status code and body are not visible to the client in the HTTP upgrade.

    // This is a bit dodgy since adversaries who send unauthorized or bad
    // requests cause us to allocate websockets. But we don't have an
    // alternative to piping errors down to the client at the moment.
    //
    // TODO consider using socket close codes in the 4xxx range for the
    //   signaling instead of messages.
    //
    // TODO should probably unify the way this works with how roomDO connect()
    //   does it.

    const closeWithError = (error: string) => {
      const pair = this._newWebSocketPair();
      const ws = pair[1];
      lc.error?.('accepting connection to send error', url.toString());
      ws.accept();

      lc.error?.('invalid connection request', error);
      sendError(ws, error);
      ws.close();

      // MDN tells me that the message will be delivered even if we call close
      // immediately after send:
      //   https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close
      // However the relevant section of the RFC says this behavior is non-normative?
      //   https://www.rfc-editor.org/rfc/rfc6455.html#section-1.4
      // In any case, it seems to work just fine to send the message and
      // close before even returning the response.

      const responseHeaders = new Headers();
      responseHeaders.set('Sec-WebSocket-Protocol', encodedAuth);
      return new Response(null, {
        status: 101,
        headers: responseHeaders,
        webSocket: pair[0],
      });
    };

    // TODO apparently many of these checks are not tested :(
    const clientID = url.searchParams.get('clientID');
    if (!clientID) {
      return closeWithError('400: clientID parameter required');
    }

    const roomID = url.searchParams.get('roomID');
    if (roomID === null || roomID === '') {
      return closeWithError('400: roomID parameter required');
    }

    lc = lc.addContext('client', clientID).addContext('room', roomID);

    let decodedAuth: string | undefined;
    try {
      decodedAuth = decodeURIComponent(encodedAuth);
    } catch (e) {
      return closeWithError('400: malformed auth');
    }
    const auth = decodedAuth;
    return this._authLock.withRead(async () => {
      let userData: UserData | undefined;
      try {
        userData = await this._authHandler(auth, roomID);
      } catch (e) {
        return closeWithError('401: authHandler rejected');
      }
      if (!userData || !userData.userID) {
        if (!userData) {
          lc.info?.('userData returned by authHandler is falsey.');
        } else if (!userData.userID) {
          lc.info?.('userData returned by authHandler has no userID.');
        }
        return closeWithError('401: no userData');
      }

      // Find the room's objectID so we can connect to it. Do this BEFORE
      // writing the connection record, in case it doesn't exist or is
      // closed/deleted.
      const roomRecord = await this._roomRecordLock.withRead(() =>
        roomRecordByRoomID(this._durableStorage, roomID),
      );

      // If the room doesn't exist, or is closed, we need to give the client some
      // visibility into this. If we just return a 404 here without accepting the
      // connection the client doesn't have any access to the return code or body.
      // So we accept the connection and send an error message to the client, then
      // close the connection. We trust it will be logged by onSocketError in the
      // client.
      if (roomRecord === undefined || roomRecord.status !== RoomStatus.Open) {
        const errorMsg = roomRecord ? 'room is not open' : 'room not found';
        if (request.headers.get('Upgrade') !== 'websocket') {
          return new Response('expected websocket', {status: 400});
        }
        const pair = this._newWebSocketPair();
        const ws = pair[1];
        const url = new URL(request.url);
        lc.info?.('accepting connection ', url.toString());
        ws.accept();

        // MDN tells me that the message will be delivered even if we call close
        // immediately after send:
        //   https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close
        // However the relevant section of the RFC says this behavior is non-normative?
        //   https://www.rfc-editor.org/rfc/rfc6455.html#section-1.4
        // In any case, it seems to work just fine to send the message and
        // close before even returning the response.
        sendError(ws, errorMsg);
        ws.close();

        const responseHeaders = new Headers();
        responseHeaders.set('Sec-WebSocket-Protocol', encodedAuth);
        return new Response(null, {
          status: 101,
          headers: responseHeaders,
          webSocket: pair[0],
        });
      }

      const roomObjectID = this._roomDO.idFromString(roomRecord.objectIDString);

      // Record the connection in DO storage
      const connectionKey = connectionKeyToString({
        userID: userData.userID,
        roomID,
        clientID,
      });
      const connectionRecord: ConnectionRecord = {
        connectTimestamp: Date.now(),
      };
      await this._state.storage.put(connectionKey, connectionRecord);

      // Forward the request to the Room Durable Object...
      const stub = this._roomDO.get(roomObjectID);
      const requestToDO = new Request(request);
      requestToDO.headers.set(
        USER_DATA_HEADER_NAME,
        encodeHeaderValue(JSON.stringify(userData)),
      );
      const responseFromDO = await stub.fetch(requestToDO);
      const responseHeaders = new Headers(responseFromDO.headers);
      // While Sec-WebSocket-Protocol is just being used as a mechanism for
      // sending `auth` since custom headers are not supported by the browser
      // WebSocket API, the Sec-WebSocket-Protocol semantics must be followed.
      // Send a Sec-WebSocket-Protocol response header with a value
      // matching the Sec-WebSocket-Protocol request header, to indicate
      // support for the protocol, otherwise the client will close the connection.
      responseHeaders.set('Sec-WebSocket-Protocol', encodedAuth);

      const response = new Response(responseFromDO.body, {
        status: responseFromDO.status,
        statusText: responseFromDO.statusText,
        webSocket: responseFromDO.webSocket,
        headers: responseHeaders,
      });
      return response;
    });
  }

  authInvalidateForUser(
    lc: LogContext,
    request: Request,
    {userID}: InvalidateForUserRequest,
  ): Promise<Response> {
    lc.debug?.(`authInvalidateForUser ${userID} waiting for lock.`);
    return this._authLock.withWrite(async () => {
      lc.debug?.('got lock.');
      const connectionKeys = (
        await this._state.storage.list({
          prefix: getConnectionKeyStringUserPrefix(userID),
        })
      ).keys();
      // The requests to the Room DOs must be completed inside the write lock
      // to avoid races with new connect requests for this user.
      return this._forwardInvalidateRequest(
        lc,
        'authInvalidateForUser',
        request,
        [...connectionKeys],
      );
    });
  }

  authInvalidateForRoom(
    lc: LogContext,
    request: Request,
    {roomID}: InvalidateForRoomRequest,
  ): Promise<Response> {
    lc.debug?.(`authInvalidateForRoom ${roomID} waiting for lock.`);
    return this._authLock.withWrite(async () => {
      lc.debug?.('got lock.');
      lc.debug?.(`Sending authInvalidateForRoom request to ${roomID}`);
      // The request to the Room DO must be completed inside the write lock
      // to avoid races with connect requests for this room.
      const roomObjectID = await this._roomRecordLock.withRead(() =>
        objectIDByRoomID(this._durableStorage, this._roomDO, roomID),
      );
      if (roomObjectID === undefined) {
        return new Response('room not found', {
          status: 404,
        });
      }
      const stub = this._roomDO.get(roomObjectID);
      const response = await stub.fetch(request);
      if (!response.ok) {
        lc.debug?.(
          `Received error response from ${roomID}. ${
            response.status
          } ${await response.clone().text()}`,
        );
      }
      return response;
    });
  }

  authInvalidateAll(lc: LogContext, request: Request): Promise<Response> {
    lc.debug?.(`authInvalidateAll waiting for lock.`);
    return this._authLock.withWrite(async () => {
      lc.debug?.('got lock.');
      const connectionKeys = (
        await this._state.storage.list({
          prefix: CONNECTION_KEY_PREFIX,
        })
      ).keys();
      // The request to the Room DOs must be completed inside the write lock
      // to avoid races with connect requests.
      return this._forwardInvalidateRequest(lc, 'authInvalidateAll', request, [
        ...connectionKeys,
      ]);
    });
  }

  async authRevalidateConnections(lc: LogContext): Promise<Response> {
    lc.info?.(`Starting auth revalidation.`);
    const authApiKey = this._authApiKey;
    if (authApiKey === undefined) {
      lc.info?.(
        'Returning Unauthorized because REFLECT_AUTH_API_KEY is not defined in env.',
      );
      return new Response('Unauthorized', {
        status: 401,
      });
    }
    const connectionRecords = await this._state.storage.list({
      prefix: CONNECTION_KEY_PREFIX,
    });
    const connectionKeyStringsByRoomID = new Map<string, Set<string>>();
    for (const keyString of connectionRecords.keys()) {
      const connectionKey = connectionKeyFromString(keyString);
      if (!connectionKey) {
        lc.error?.('Failed to parse connection key', keyString);
        continue;
      }
      const {roomID} = connectionKey;
      let keyStringSet = connectionKeyStringsByRoomID.get(roomID);
      if (!keyStringSet) {
        keyStringSet = new Set();
        connectionKeyStringsByRoomID.set(roomID, keyStringSet);
      }
      keyStringSet.add(keyString);
    }
    lc.info?.(
      `Revalidating ${connectionRecords.size} ConnectionRecords across ${connectionKeyStringsByRoomID.size} rooms.`,
    );
    let deleteCount = 0;
    for (const [
      roomID,
      connectionKeyStringsForRoomID,
    ] of connectionKeyStringsByRoomID) {
      lc.debug?.(`revalidating connections for ${roomID} waiting for lock.`);
      await this._authLock.withWrite(async () => {
        lc.debug?.('got lock.');
        const roomObjectID = await this._roomRecordLock.withRead(() =>
          objectIDByRoomID(this._durableStorage, this._roomDO, roomID),
        );
        if (roomObjectID === undefined) {
          lc.error?.(`Can't find room ${roomID}, skipping`);
          return;
        }
        const stub = this._roomDO.get(roomObjectID);
        const response = await stub.fetch(
          new Request(
            `https://unused-reflect-room-do.dev${paths.authConnections}`,
            {
              headers: createAuthAPIHeaders(authApiKey),
            },
          ),
        );
        let connectionsResponse: ConnectionsResponse | undefined;
        try {
          const responseJSON = await response.json();
          assert(responseJSON, connectionsResponseSchema);
          connectionsResponse = responseJSON;
        } catch (e) {
          lc.error?.(`Bad ${paths.authConnections} response from roomDO`, e);
        }
        if (connectionsResponse) {
          const openConnectionKeyStrings = new Set(
            connectionsResponse.map(({userID, clientID}) =>
              connectionKeyToString({
                roomID,
                userID,
                clientID,
              }),
            ),
          );
          const keysToDelete: string[] = [
            ...connectionKeyStringsForRoomID,
          ].filter(keyString => !openConnectionKeyStrings.has(keyString));
          try {
            deleteCount += await this._state.storage.delete(keysToDelete);
          } catch (e) {
            lc.info?.('Failed to delete connections for roomID', roomID);
          }
        }
      });
    }
    lc.info?.(
      `Revalidated ${connectionRecords.size} ConnectionRecords, deleted ${deleteCount} ConnectionRecords.`,
    );
    return new Response('Complete', {status: 200});
  }

  private async _forwardInvalidateRequest(
    lc: LogContext,
    invalidateRequestName: string,
    request: Request,
    connectionKeyStrings: string[],
  ): Promise<Response> {
    const connectionKeys = connectionKeyStrings.map(keyString => {
      const connectionKey = connectionKeyFromString(keyString);
      if (!connectionKey) {
        lc.error?.('Failed to parse connection key', keyString);
      }
      return connectionKey;
    });
    const roomIDSet = new Set<string>();
    for (const connectionKey of connectionKeys) {
      if (connectionKey) {
        roomIDSet.add(connectionKey.roomID);
      }
    }

    const roomIDs = [...roomIDSet];
    const responsePromises: Promise<Response>[] = [];
    lc.debug?.(
      `Sending ${invalidateRequestName} requests to ${roomIDs.length} rooms`,
    );
    // Send requests to room DOs in parallel
    const errorResponses = [];
    for (const roomID of roomIDs) {
      const roomObjectID = await this._roomRecordLock.withRead(() =>
        objectIDByRoomID(this._durableStorage, this._roomDO, roomID),
      );

      if (roomObjectID === undefined) {
        const msg = `No objectID for ${roomID}, skipping`;
        lc.error?.(msg);
        errorResponses.push(new Response(msg, {status: 500}));
        continue;
      }

      const stub = this._roomDO.get(roomObjectID);
      responsePromises.push(stub.fetch(request));
    }
    for (let i = 0; i < responsePromises.length; i++) {
      const response = await responsePromises[i];
      if (!response.ok) {
        errorResponses.push(response);
        lc.error?.(
          `Received error response from ${roomIDs[i]}. ${response.status} ${
            // TODO(arv): This should be `text()` and not `text`
            await response.text
          }`,
        );
      }
    }
    if (errorResponses.length === 0) {
      return new Response('Success', {
        status: 200,
      });
    }
    return errorResponses[0];
  }
}

const CONNECTION_KEY_PREFIX = 'connection/';

function connectionKeyToString(key: ConnectionKey): string {
  return `${CONNECTION_KEY_PREFIX}${encodeURIComponent(
    key.userID,
  )}/${encodeURIComponent(key.roomID)}/${encodeURIComponent(key.clientID)}/`;
}

function getConnectionKeyStringUserPrefix(userID: string): string {
  return `${CONNECTION_KEY_PREFIX}${encodeURIComponent(userID)}/`;
}

export function connectionKeyFromString(
  key: string,
): ConnectionKey | undefined {
  if (!key.startsWith(CONNECTION_KEY_PREFIX)) {
    return undefined;
  }
  const parts = key.split('/');
  if (parts.length !== 5 || parts[4] !== '') {
    return undefined;
  }
  return {
    userID: decodeURIComponent(parts[1]),
    roomID: decodeURIComponent(parts[2]),
    clientID: decodeURIComponent(parts[3]),
  };
}

function createUnauthorizedResponse(message = 'Unauthorized'): Response {
  return new Response(message, {
    status: 401,
  });
}
