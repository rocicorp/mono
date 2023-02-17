import {encodeHeaderValue} from '../util/headers.js';
import {LogSink, LogContext, LogLevel} from '@rocicorp/logger';
import {version} from '../util/version.js';
import {AuthHandler, UserData, USER_DATA_HEADER_NAME} from './auth.js';
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
  invalidateForRoomRequestSchema,
  invalidateForUserRequestSchema,
} from '../protocol/api/auth.js';
import * as superstruct from 'superstruct';
import {createAuthAPIHeaders} from './auth-api-headers.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {createRoomRequestSchema} from '../protocol/api/room.js';
import {closeWithError} from '../util/socket.js';
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
  withBody,
  withVersion,
  WithVersion,
} from './router.js';
import {addRequestIDFromHeadersOrRandomID} from './request-id.js';
import {createUnauthorizedResponse} from './create-unauthorized-response.js';
import {ErrorKind} from '../protocol/error.js';
import {ROOM_ROUTES} from './room-do.js';
import {pullRequestSchema} from '../protocol/pull.js';
import {
  CONNECT_URL_PATTERN,
  CREATE_ROOM_PATH,
  LEGACY_CONNECT_PATH,
  LEGACY_CREATE_ROOM_PATH,
  PULL_PATH,
} from './paths.js';

export interface AuthDOOptions {
  roomDO: DurableObjectNamespace;
  state: DurableObjectState;
  authHandler: AuthHandler;
  authApiKey: string;
  logSink: LogSink;
  logLevel: LogLevel;
}
export type ConnectionKey = {
  userID: string;
  roomID: string;
  clientID: string;
};
export type ConnectionRecord = {
  connectTimestamp: number;
};

export const AUTH_ROUTES_AUTHED_BY_API_KEY = {
  roomStatusByRoomID: '/api/room/v0/room/:roomID/status',
  roomRecords: '/api/room/v0/rooms',
  closeRoom: '/api/room/v0/room/:roomID/close',
  deleteRoom: '/api/room/v0/room/:roomID/delete',
  migrateRoom: '/api/room/v0/room/:roomID/migrate/1',
  forgetRoom: '/api/room/v0/room/:roomID/DANGER/forget',
  authInvalidateAll: '/api/auth/v0/invalidateAll',
  authInvalidateForUser: '/api/auth/v0/invalidateForUser',
  authInvalidateForRoom: '/api/auth/v0/invalidateForRoom',
  authRevalidateConnections: '/api/auth/v0/revalidateConnections',
  legacyCreateRoom: LEGACY_CREATE_ROOM_PATH,
  createRoom: CREATE_ROOM_PATH,
} as const;

export const AUTH_ROUTES_AUTHED_BY_AUTH_HANDLER = {
  legacyConnect: LEGACY_CONNECT_PATH,
  connect: CONNECT_URL_PATTERN,
  pull: PULL_PATH,
} as const;

export const AUTH_ROUTES = {
  ...AUTH_ROUTES_AUTHED_BY_API_KEY,
  ...AUTH_ROUTES_AUTHED_BY_AUTH_HANDLER,
} as const;

export class BaseAuthDO implements DurableObject {
  private readonly _router = new Router();
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
  private readonly _authLock = new RWLock();
  // _roomRecordLock ensure that at most one write operation is in
  // progress on a RoomRecord at a time. For safety, if something requires
  // both the auth lock and the room record lock, the auth lock MUST be
  // acquired first.
  private readonly _roomRecordLock = new RWLock();

  constructor(options: AuthDOOptions) {
    const {roomDO, state, authHandler, authApiKey, logSink, logLevel} = options;
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

    this._initRoutes();
    this._lc.info?.('Starting server');
    this._lc.info?.('Version:', version);
  }

  async fetch(request: Request): Promise<Response> {
    const lc = addRequestIDFromHeadersOrRandomID(this._lc, request);
    lc.debug?.('Handling request:', request.url);
    try {
      const resp = await this._router.dispatch(request, {lc});
      lc.debug?.(`Returning response: ${resp.status} ${resp.statusText}`);
      return resp;
    } catch (e) {
      lc.error?.('Unhandled exception in fetch', e);
      return new Response(
        e instanceof Error ? e.message : 'Unexpected error.',
        {status: 500},
      );
    }
  }

  private _requireAPIKey = <Context extends BaseContext, Resp>(
    next: Handler<Context, Resp>,
  ) => requireAuthAPIKey(() => this._authApiKey, next);

  private _roomStatusByRoomID = get(
    this._requireAPIKey(
      withRoomID(
        asJSON(async (ctx: BaseContext & WithRoomID) => {
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

  private _createRoom = post(
    this._requireAPIKey(
      withBody(createRoomRequestSchema, (ctx, req) => {
        const {lc, body} = ctx;
        return this._roomRecordLock.withWrite(() =>
          createRoom(lc, this._roomDO, this._durableStorage, req, body),
        );
      }),
    ),
  );

  // A call to closeRoom should be followed by a call to authInvalidateForRoom
  // to ensure users are logged out.
  private _closeRoom = post(
    this._requireAPIKey(
      withRoomID(ctx =>
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
      withRoomID((ctx, req) =>
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
      withRoomID(ctx =>
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
      withRoomID(ctx =>
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
    this._router.register(AUTH_ROUTES.legacyCreateRoom, this._createRoom);
    this._router.register(AUTH_ROUTES.createRoom, this._createRoom);
    this._router.register(AUTH_ROUTES.deleteRoom, this._deleteRoom);
    this._router.register(AUTH_ROUTES.migrateRoom, this._migrateRoom);
    this._router.register(AUTH_ROUTES.forgetRoom, this._forgetRoom);
    this._router.register(
      AUTH_ROUTES.authInvalidateAll,
      this._authInvalidateAll,
    );
    this._router.register(
      AUTH_ROUTES.authInvalidateForUser,
      this._authInvalidateForUser,
    );
    this._router.register(
      AUTH_ROUTES.authInvalidateForRoom,
      this._authInvalidateForRoom,
    );
    this._router.register(
      AUTH_ROUTES.authRevalidateConnections,
      this._authRevalidateConnections,
    );

    this._router.register(AUTH_ROUTES.legacyConnect, this._legacyConnect);
    this._router.register(AUTH_ROUTES.connect, this._connect);
    this._router.register(AUTH_ROUTES.pull, this._pull);
  }

  private _connect = get(
    withVersion((ctx: BaseContext & WithVersion, request) => {
      const {lc, version} = ctx;
      return this._connectImpl(lc, version, request);
    }),
  );

  private _legacyConnect = get((ctx, request) => {
    const {lc} = ctx;
    return this._connectImpl(lc, 0, request);
  });

  private _connectImpl(lc: LogContext, version: number, request: Request) {
    const {url} = request;
    lc.info?.('authDO received websocket connection request:', url);
    if (request.headers.get('Upgrade') !== 'websocket') {
      lc.error?.('authDO returning 400 bc missing Upgrade header:', url);
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

    const closeWithErrorLocal = (errorKind: ErrorKind, msg: string) =>
      createWSAndCloseWithError(lc, url, errorKind, msg, encodedAuth);

    const expectedVersion = 0;
    if (version !== expectedVersion) {
      lc.debug?.(
        'Version not supported. Expected',
        expectedVersion,
        'but got',
        version,
      );
      return closeWithErrorLocal(
        ErrorKind.VersionNotSupported,
        'unsupported version',
      );
    }

    const {searchParams} = new URL(url);
    // TODO apparently many of these checks are not tested :(
    const clientID = searchParams.get('clientID');
    if (!clientID) {
      return closeWithErrorLocal(
        ErrorKind.InvalidConnectionRequest,
        'clientID parameter required',
      );
    }

    const roomID = searchParams.get('roomID');
    if (!roomID) {
      return closeWithErrorLocal(
        ErrorKind.InvalidConnectionRequest,
        'roomID parameter required',
      );
    }

    lc = lc.addContext('client', clientID).addContext('room', roomID);

    let decodedAuth: string | undefined;
    try {
      decodedAuth = decodeURIComponent(encodedAuth);
    } catch (e) {
      return closeWithErrorLocal(
        ErrorKind.InvalidConnectionRequest,
        'malformed auth',
      );
    }
    const auth = decodedAuth;
    return this._authLock.withRead(async () => {
      let userData: UserData | undefined;
      try {
        userData = await this._authHandler(auth, roomID);
      } catch (e) {
        return closeWithErrorLocal(
          ErrorKind.Unauthorized,
          'authHandler rejected',
        );
      }
      if (!userData || !userData.userID) {
        if (!userData) {
          lc.info?.('userData returned by authHandler is not an object.');
        } else if (!userData.userID) {
          lc.info?.('userData returned by authHandler has no userID.');
        }
        return closeWithErrorLocal(ErrorKind.Unauthorized, 'no userData');
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
        const kind = roomRecord ? ErrorKind.RoomClosed : ErrorKind.RoomNotFound;
        return createWSAndCloseWithError(lc, url, kind, roomID, encodedAuth);
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

  private _pull = post(
    withBody(pullRequestSchema, async (ctx, req) => {
      const {
        lc,
        body: {roomID},
      } = ctx;

      const auth = req.headers.get('Authorization');
      if (!auth) {
        lc.info?.('auth not found in Authorization header.');
        return createUnauthorizedResponse('auth required');
      }

      let userData: UserData | undefined;
      try {
        userData = await this._authHandler(auth, roomID);
      } catch (e) {
        return createUnauthorizedResponse();
      }
      if (!userData || !userData.userID) {
        if (!userData) {
          lc.info?.('userData returned by authHandler is not an object.');
        } else if (!userData.userID) {
          lc.info?.('userData returned by authHandler has no userID.');
        }
        return createUnauthorizedResponse();
      }

      // Find the room's objectID so we can route the request to it.
      const roomRecord = await this._roomRecordLock.withRead(() =>
        roomRecordByRoomID(this._durableStorage, roomID),
      );
      if (roomRecord === undefined || roomRecord.status !== RoomStatus.Open) {
        const errorMsg = roomRecord ? 'room is not open' : 'room not found';
        return new Response(errorMsg, {
          status: 404,
        });
      }

      const roomObjectID = this._roomDO.idFromString(roomRecord.objectIDString);
      // Forward the request to the Room Durable Object...
      const stub = this._roomDO.get(roomObjectID);
      const requestToDO = new Request(req);
      requestToDO.headers.set(
        USER_DATA_HEADER_NAME,
        encodeHeaderValue(JSON.stringify(userData)),
      );
      const responseFromDO = await stub.fetch(requestToDO);
      return responseFromDO;
    }),
  );

  private _authInvalidateForRoom = post(
    this._requireAPIKey(
      withBody(invalidateForRoomRequestSchema, (ctx, req) => {
        const {lc, body} = ctx;
        const {roomID} = body;
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
            return new Response('room not found', {status: 404});
          }
          const stub = this._roomDO.get(roomObjectID);
          const response = await stub.fetch(req);
          if (!response.ok) {
            lc.debug?.(
              `Received error response from ${roomID}. ${
                response.status
              } ${await response.clone().text()}`,
            );
          }
          return response;
        });
      }),
    ),
  );

  private _authInvalidateForUser = post(
    this._requireAPIKey(
      withBody(invalidateForUserRequestSchema, (ctx, req) => {
        const {lc, body} = ctx;
        const {userID} = body;
        lc.debug?.(`_authInvalidateForUser waiting for lock.`);
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
            req,
            [...connectionKeys],
          );
        });
      }),
    ),
  );

  private _authInvalidateAll = post(
    this._requireAPIKey((ctx, req) => {
      const {lc} = ctx;
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
        return this._forwardInvalidateRequest(lc, 'authInvalidateAll', req, [
          ...connectionKeys,
        ]);
      });
    }),
  );

  private _authRevalidateConnections = post(
    this._requireAPIKey(async ctx => {
      const {lc} = ctx;
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
              `https://unused-reflect-room-do.dev${ROOM_ROUTES.authConnections}`,
              {
                method: 'POST',
                headers: createAuthAPIHeaders(this._authApiKey),
              },
            ),
          );
          let connectionsResponse: ConnectionsResponse | undefined;
          try {
            const responseJSON = await response.json();
            superstruct.assert(responseJSON, connectionsResponseSchema);
            connectionsResponse = responseJSON;
          } catch (e) {
            lc.error?.(
              `Bad ${ROOM_ROUTES.authConnections} response from roomDO`,
              e,
            );
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
    }),
  );

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
      const req = roomIDs.length === 1 ? request : request.clone();
      responsePromises.push(stub.fetch(req));
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
      return new Response('Success', {status: 200});
    }
    return errorResponses[0];
  }
}

const CONNECTION_KEY_PREFIX = 'connection/';

function createWSAndCloseWithError(
  lc: LogContext,
  url: string,
  kind: ErrorKind,
  msg: string,
  encodedAuth: string,
) {
  const pair = new WebSocketPair();
  const ws = pair[1];
  lc.error?.('accepting connection to send error', url);
  ws.accept();

  // MDN tells me that the message will be delivered even if we call close
  // immediately after send:
  //   https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close
  // However the relevant section of the RFC says this behavior is non-normative?
  //   https://www.rfc-editor.org/rfc/rfc6455.html#section-1.4
  // In any case, it seems to work just fine to send the message and
  // close before even returning the response.

  closeWithError(lc, ws, kind, msg);

  const responseHeaders = new Headers();
  responseHeaders.set('Sec-WebSocket-Protocol', encodedAuth);
  return new Response(null, {
    status: 101,
    headers: responseHeaders,
    webSocket: pair[0],
  });
}

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
