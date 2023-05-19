import {RWLock} from '@rocicorp/lock';
import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import type {ErrorKind} from 'reflect-protocol';
import {
  ConnectionsResponse,
  connectionsResponseSchema,
  createRoomRequestSchema,
  invalidateForRoomRequestSchema,
  invalidateForUserRequestSchema,
} from 'reflect-protocol';
import {assert} from 'shared/asserts.js';
import * as valita from 'shared/valita.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {encodeHeaderValue} from '../util/headers.js';
import {randomID} from '../util/rand.js';
import {closeWithError} from '../util/socket.js';
import {version} from '../util/version.js';
import {createAuthAPIHeaders} from './auth-api-headers.js';
import {AuthHandler, USER_DATA_HEADER_NAME, UserData} from './auth.js';
import {
  CONNECT_URL_PATTERN,
  CREATE_ROOM_PATH,
  LEGACY_CONNECT_PATH,
  LEGACY_CREATE_ROOM_PATH,
} from './paths.js';
import {addRequestIDFromHeadersOrRandomID} from './request-id.js';
import {ROOM_ROUTES} from './room-do.js';
import {
  RoomRecord,
  RoomStatus,
  closeRoom,
  createRoom,
  createRoomRecordForLegacyRoom,
  deleteRoom,
  deleteRoomRecord,
  internalCreateRoom,
  objectIDByRoomID,
  roomRecordByRoomID,
  roomRecords,
} from './rooms.js';
import {
  BaseContext,
  Handler,
  Router,
  WithRoomID,
  WithVersion,
  asJSON,
  get,
  post,
  requireAuthAPIKey,
  withBody,
  withRoomID,
  withVersion,
} from './router.js';
import {registerUnhandledRejectionHandler} from './unhandled-rejection-handler.js';

export interface AuthDOOptions {
  roomDO: DurableObjectNamespace;
  state: DurableObjectState;
  authHandler?: AuthHandler | undefined;
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
  private readonly _authHandler: AuthHandler | undefined;
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

  /**
   * @param ensureStorageSchemaMigratedWrapperForTests provides a seam for
   *     tests to wait for migrations to complete, and catch/assert about
   *     any errors thrown by the migrations
   */
  constructor(
    options: AuthDOOptions,
    ensureStorageSchemaMigratedWrapperForTests: (
      p: Promise<void>,
    ) => Promise<void> = p => p,
  ) {
    const {roomDO, state, authHandler, authApiKey, logSink, logLevel} = options;
    this._roomDO = roomDO;
    this._state = state;
    this._durableStorage = new DurableStorage(
      state.storage,
      false, // don't allow unconfirmed
    );
    this._authHandler = authHandler;
    this._authApiKey = authApiKey;
    const lc = new LogContext(logLevel, logSink).addContext('AuthDO');
    registerUnhandledRejectionHandler(lc);
    this._lc = lc.addContext('doID', state.id.toString());

    this._initRoutes();
    this._lc.info?.('Starting server');
    this._lc.info?.('Version:', version);
    void state.blockConcurrencyWhile(() =>
      ensureStorageSchemaMigratedWrapperForTests(
        ensureStorageSchemaMigrated(state.storage, this._lc, logSink),
      ),
    );
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
        const {
          lc,
          body: {roomID, jurisdiction},
        } = ctx;
        return this._roomRecordLock.withWrite(() =>
          createRoom(
            lc,
            this._roomDO,
            this._durableStorage,
            req,
            roomID,
            jurisdiction,
          ),
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

    if (this._authHandler && !encodedAuth) {
      lc.error?.('authDO auth not found in Sec-WebSocket-Protocol header.');
      return closeWithErrorLocal('InvalidConnectionRequest', 'auth required');
    }
    const expectedVersion = 1;
    if (version !== expectedVersion) {
      lc.debug?.(
        'Version not supported. Expected',
        expectedVersion,
        'but got',
        version,
      );
      return closeWithErrorLocal('VersionNotSupported', 'unsupported version');
    }
    const {searchParams} = new URL(url);
    // TODO apparently many of these checks are not tested :(
    const clientID = searchParams.get('clientID');
    if (!clientID) {
      return closeWithErrorLocal(
        'InvalidConnectionRequest',
        'clientID parameter required',
      );
    }

    const roomID = searchParams.get('roomID');
    if (!roomID) {
      return closeWithErrorLocal(
        'InvalidConnectionRequest',
        'roomID parameter required',
      );
    }

    const userID = searchParams.get('userID');
    if (!userID) {
      return closeWithErrorLocal(
        'InvalidConnectionRequest',
        'userID parameter required',
      );
    }

    const jurisdiction = searchParams.get('jurisdiction') ?? undefined;
    if (jurisdiction && jurisdiction !== 'eu') {
      return closeWithErrorLocal(
        'InvalidConnectionRequest',
        'invalid jurisdiction parameter',
      );
    }
    assert(jurisdiction === undefined || jurisdiction === 'eu');

    lc = lc.addContext('client', clientID).addContext('room', roomID);
    let decodedAuth: string | undefined;
    if (encodedAuth) {
      try {
        decodedAuth = decodeURIComponent(encodedAuth);
      } catch (e) {
        return closeWithErrorLocal(
          'InvalidConnectionRequest',
          'malformed auth',
        );
      }
    }
    return this._authLock.withRead(async () => {
      let userData: UserData = {
        userID,
      };

      if (this._authHandler) {
        const auth = decodedAuth;
        assert(auth);
        let authHandlerUserData: UserData | null;

        try {
          authHandlerUserData = await this._authHandler(auth, roomID);
        } catch (e) {
          return closeWithErrorLocal('Unauthorized', 'authHandler rejected');
        }
        if (!authHandlerUserData || !authHandlerUserData.userID) {
          if (!authHandlerUserData) {
            lc.info?.('userData returned by authHandler is not an object.');
          } else if (!authHandlerUserData.userID) {
            lc.info?.('userData returned by authHandler has no userID.');
          }
          return closeWithErrorLocal('Unauthorized', 'no userData');
        }
        if (authHandlerUserData.userID !== userID) {
          lc.info?.('userData returned by authHandler has a different userID.');
          return closeWithErrorLocal(
            'Unauthorized',
            'userID returned by authHandler does not match userID url parameter',
          );
        }
        userData = authHandlerUserData;
      }

      // Find the room's objectID so we can connect to it. Do this BEFORE
      // writing the connection record, in case it doesn't exist or is
      // closed/deleted.

      let roomRecord: RoomRecord | undefined =
        await this._roomRecordLock.withRead(
          // Check if room already exists.
          () => roomRecordByRoomID(this._durableStorage, roomID),
        );

      if (!roomRecord) {
        roomRecord = await this._roomRecordLock.withWrite(async () => {
          // checking again in case it was created while we were waiting for writeLock
          const rr = await roomRecordByRoomID(this._durableStorage, roomID);
          if (rr) {
            return rr;
          }
          lc.debug?.('room not found, trying to create it');

          const resp = await internalCreateRoom(
            lc,
            this._roomDO,
            this._durableStorage,
            roomID,
            jurisdiction,
          );
          if (!resp.ok) {
            return undefined;
          }
          return roomRecordByRoomID(this._durableStorage, roomID);
        });
      }

      // If the room is closed or we failed to implicitly create it, we need to
      // give the client some visibility into this. If we just return a 404 here
      // without accepting the connection the client doesn't have any access to
      // the return code or body. So we accept the connection and send an error
      // message to the client, then close the connection. We trust it will be
      // logged by onSocketError in the client.

      if (roomRecord === undefined || roomRecord.status !== RoomStatus.Open) {
        const kind = roomRecord ? 'RoomClosed' : 'RoomNotFound';
        return createWSAndCloseWithError(lc, url, kind, roomID, encodedAuth);
      }

      const roomObjectID = this._roomDO.idFromString(roomRecord.objectIDString);

      // Record the connection in DO storage
      await recordConnection(
        {
          userID: userData.userID,
          roomID,
          clientID,
        },
        this._state.storage,
        {
          connectTimestamp: Date.now(),
        },
      );

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
      if (encodedAuth !== null) {
        responseHeaders.set('Sec-WebSocket-Protocol', encodedAuth);
      }
      const response = new Response(responseFromDO.body, {
        status: responseFromDO.status,
        statusText: responseFromDO.statusText,
        webSocket: responseFromDO.webSocket,
        headers: responseHeaders,
      });
      return response;
    });
  }

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
      return this._authLock.withWrite(() => {
        lc.debug?.('got lock.');
        // The request to the Room DOs must be completed inside the write lock
        // to avoid races with connect requests.
        return this._forwardInvalidateRequest(
          lc,
          'authInvalidateAll',
          req,
          // Use async generator because the full list of connections
          // may exceed the DO's memory limits.
          getKeyStrings(this._state.storage, CONNECTION_KEY_PREFIX),
        );
      });
    }),
  );

  private _authRevalidateConnections = post(
    this._requireAPIKey(async ctx => {
      const {lc} = ctx;
      lc.info?.('Revalidating connections.');
      const connectionsByRoom = getConnectionsByRoom(this._state.storage, lc);
      let connectionCount = 0;
      let revalidatedCount = 0;
      let deleteCount = 0;
      for await (const {roomID, connectionKeys} of connectionsByRoom) {
        connectionCount += connectionKeys.length;
        lc.info?.(
          `Revalidating ${connectionKeys.length} connections for room ${roomID}.`,
        );
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
          let connectionsResponse: ConnectionsResponse;
          try {
            const responseJSON = valita.parse(
              await response.json(),
              connectionsResponseSchema,
            );
            connectionsResponse = responseJSON;
          } catch (e) {
            lc.error?.(
              `Bad ${ROOM_ROUTES.authConnections} response from roomDO ${roomID}`,
              e,
            );
            return;
          }
          const openConnectionKeyStrings = new Set(
            connectionsResponse.map(({userID, clientID}) =>
              connectionKeyToString({
                roomID,
                userID,
                clientID,
              }),
            ),
          );
          const toDelete: [ConnectionKey, string][] = connectionKeys
            .map((key): [ConnectionKey, string] => [
              key,
              connectionKeyToString(key),
            ])
            .filter(
              ([_, keyString]) => !openConnectionKeyStrings.has(keyString),
            );
          try {
            for (const [keyToDelete] of toDelete) {
              await deleteConnection(keyToDelete, this._state.storage);
            }
            await this._state.storage.sync();
          } catch (e) {
            lc.info?.('Failed to delete connections for roomID', roomID);
            return;
          }
          revalidatedCount += connectionKeys.length;
          deleteCount += toDelete.length;
          lc.info?.(
            `Revalidated ${connectionKeys.length} connections for room ${roomID}, deleted ${toDelete.length} connections.`,
          );
        });
      }
      lc.info?.(
        `Revalidated ${revalidatedCount} connections, deleted ${deleteCount} connections.  Failed to revalidate ${
          connectionCount - revalidatedCount
        } connections.`,
      );
      return new Response('Complete', {status: 200});
    }),
  );

  private async _forwardInvalidateRequest(
    lc: LogContext,
    invalidateRequestName: string,
    request: Request,
    connectionKeyStrings: Iterable<string> | AsyncGenerator<string>,
  ): Promise<Response> {
    const roomIDSet = new Set<string>();
    for await (const keyString of connectionKeyStrings) {
      const connectionKey = connectionKeyFromString(keyString);
      if (connectionKey) {
        roomIDSet.add(connectionKey.roomID);
      } else {
        lc.error?.('Failed to parse connection key', keyString);
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
          `Received error response from ${roomIDs[i]}. ${
            response.status
          } ${await response.clone().text()}`,
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
const CONNECTIONS_BY_ROOM_INDEX_PREFIX = 'connections_by_room/';

function createWSAndCloseWithError(
  lc: LogContext,
  url: string,
  kind: ErrorKind,
  msg: string,
  encodedAuth: string | null,
) {
  const pair = new WebSocketPair();
  const ws = pair[1];
  lc.info?.('accepting connection to send error', url);
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
  if (encodedAuth) {
    responseHeaders.set('Sec-WebSocket-Protocol', encodedAuth);
  }
  return new Response(null, {
    status: 101,
    headers: responseHeaders,
    webSocket: pair[0],
  });
}

function connectionKeyToString(key: ConnectionKey): string {
  return `${getConnectionKeyStringUserPrefix(key.userID)}${encodeURIComponent(
    key.roomID,
  )}/${encodeURIComponent(key.clientID)}/`;
}

function getConnectionKeyStringUserPrefix(userID: string): string {
  return `${CONNECTION_KEY_PREFIX}${encodeURIComponent(userID)}/`;
}

function connectionKeyToConnectionRoomIndexString(key: ConnectionKey): string {
  return `${getConnectionRoomIndexPrefix(key.roomID)}${connectionKeyToString(
    key,
  )}`;
}

function getConnectionRoomIndexPrefix(roomID: string): string {
  return `${CONNECTIONS_BY_ROOM_INDEX_PREFIX}${encodeURIComponent(roomID)}/`;
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

export function connectionKeyFromRoomIndexString(
  key: string,
): ConnectionKey | undefined {
  if (!key.startsWith(CONNECTIONS_BY_ROOM_INDEX_PREFIX)) {
    return undefined;
  }
  const indexOfFirstSlashAfterPrefix = key.indexOf(
    '/',
    CONNECTIONS_BY_ROOM_INDEX_PREFIX.length,
  );
  if (indexOfFirstSlashAfterPrefix === -1) {
    return undefined;
  }
  return connectionKeyFromString(
    key.substring(indexOfFirstSlashAfterPrefix + 1),
  );
}

async function getConnectionKeysForRoomID(
  roomID: string,
  storage: DurableObjectStorage,
): Promise<ConnectionKey[]> {
  const connectionKeys = [];
  for (const key of (
    await storage.list({
      prefix: getConnectionRoomIndexPrefix(roomID),
    })
  ).keys()) {
    const connectionKey = connectionKeyFromRoomIndexString(key);
    if (connectionKey) {
      connectionKeys.push(connectionKey);
    }
  }
  return connectionKeys;
}

/**
 * Provides a way to iterate over all stored connection keys grouped by
 * room id, in a way that will not exceed memory limits even if not all stored
 * connection keys can fit in memory at once.  It does assume that
 * all connection keys for a given room id can fit in memory.
 */
async function* getConnectionsByRoom(
  storage: DurableObjectStorage,
  lc: LogContext,
): AsyncGenerator<{
  roomID: string;
  connectionKeys: ConnectionKey[];
}> {
  let lastKey = '';
  while (true) {
    const nextRoomListResult = await storage.list({
      startAfter: lastKey,
      prefix: CONNECTIONS_BY_ROOM_INDEX_PREFIX,
      limit: 1,
    });
    if (nextRoomListResult.size === 0) {
      return;
    }
    const firstRoomIndexString: string = nextRoomListResult.keys().next().value;
    const connectionKey =
      connectionKeyFromRoomIndexString(firstRoomIndexString);
    if (!connectionKey) {
      lc.error?.(
        'Failed to parse connection room index key',
        firstRoomIndexString,
      );
      lastKey = firstRoomIndexString;
      continue;
    }
    const {roomID} = connectionKey;
    const connectionKeys = await getConnectionKeysForRoomID(roomID, storage);
    yield {roomID, connectionKeys};
    lastKey = connectionKeyToConnectionRoomIndexString(
      connectionKeys.length > 0
        ? connectionKeys[connectionKeys.length - 1]
        : connectionKey,
    );
  }
}

/**
 * Provides a way to iterate over keys with a prefix in a way that
 * will not exceed memory limits even if not all entries with keys
 * with the given prefix can fit in memory at once.  Assumes at least
 * 1000 entries can fit in memory at a time.
 */
async function* getKeyStrings(
  storage: DurableObjectStorage,
  prefix: string,
): AsyncGenerator<string> {
  let lastKey = '';
  let done = false;
  while (!done) {
    const listResult = await storage.list({
      startAfter: lastKey,
      prefix,
      limit: 1000,
    });
    for (const keyString of listResult.keys()) {
      yield keyString;
      lastKey = keyString;
    }
    done = listResult.size === 0;
  }
}

export async function recordConnection(
  connectionKey: ConnectionKey,
  storage: DurableObjectStorage,
  record: ConnectionRecord,
) {
  const connectionKeyString = connectionKeyToString(connectionKey);
  const connectionRoomIndexString =
    connectionKeyToConnectionRoomIndexString(connectionKey);
  // done in a single put to ensure atomicity
  await storage.put({
    [connectionKeyString]: record,
    [connectionRoomIndexString]: {},
  });
}

async function deleteConnection(
  connectionKey: ConnectionKey,
  storage: DurableObjectStorage,
) {
  const connectionKeyString = connectionKeyToString(connectionKey);
  const connectionRoomIndexString =
    connectionKeyToConnectionRoomIndexString(connectionKey);
  // done in a single delete to ensure atomicity
  await storage.delete([connectionKeyString, connectionRoomIndexString]);
}

export const STORAGE_SCHEMA_META_KEY = 'storage_schema_meta';
export const STORAGE_SCHEMA_VERSION = 1;
export const STORAGE_SCHEMA_MIN_SAFE_ROLLBACK_VERSION = 0;

export type StorageSchemaMeta = {
  version: number;
  maxVersion: number;
  minSafeRollbackVersion: number;
};

async function migrateStorageSchemaToVersion(
  storage: DurableObjectStorage,
  lc: LogContext,
  existingStorageSchemaMeta: StorageSchemaMeta,
  version: number,
  minSafeRollbackVersion: number,
  migrate: () => Promise<void>,
) {
  lc.info?.(
    `Migrating from storage schema version ${existingStorageSchemaMeta.version} to storage schema version ${version}.`,
  );
  assert(version >= existingStorageSchemaMeta.minSafeRollbackVersion);
  assert(version <= STORAGE_SCHEMA_VERSION);
  if (
    minSafeRollbackVersion > existingStorageSchemaMeta.minSafeRollbackVersion
  ) {
    const preUpdate = {
      ...existingStorageSchemaMeta,
      minSafeRollbackVersion,
    };
    await storage.put(STORAGE_SCHEMA_META_KEY, preUpdate);
    await storage.sync();
  }
  await migrate();
  const postUpdate = {
    ...existingStorageSchemaMeta,
    version,
    maxVersion: Math.max(version, existingStorageSchemaMeta.maxVersion),
    minSafeRollbackVersion: Math.max(
      minSafeRollbackVersion,
      existingStorageSchemaMeta.minSafeRollbackVersion,
    ),
  };
  await storage.put(STORAGE_SCHEMA_META_KEY, postUpdate);
  await storage.sync();
  lc.info?.(
    `Successfully migrated from storage schema version ${existingStorageSchemaMeta.version} to storage schema version ${version}.`,
  );
  return postUpdate;
}

async function ensureStorageSchemaMigrated(
  storage: DurableObjectStorage,
  lc: LogContext,
  logSink: LogSink,
) {
  lc = lc.addContext('schemaUpdateID', randomID());
  lc.info?.('Ensuring storage schema is up to date.');
  let storageSchemaMeta: StorageSchemaMeta = (await storage.get(
    STORAGE_SCHEMA_META_KEY,
  )) ?? {version: 0, maxVersion: 0, minSafeRollbackVersion: 0};
  if (storageSchemaMeta.minSafeRollbackVersion > STORAGE_SCHEMA_VERSION) {
    throw new Error(
      `Cannot safely migrate to schema version ${STORAGE_SCHEMA_VERSION}, schema is currently version ${storageSchemaMeta.version}, min safe rollback version is ${storageSchemaMeta.minSafeRollbackVersion}`,
    );
  }
  if (storageSchemaMeta.version > STORAGE_SCHEMA_VERSION) {
    storageSchemaMeta = await migrateStorageSchemaToVersion(
      storage,
      lc,
      storageSchemaMeta,
      STORAGE_SCHEMA_VERSION,
      STORAGE_SCHEMA_MIN_SAFE_ROLLBACK_VERSION,
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      async () => {
        /* noop */
      },
    );
    lc.info?.('Storage schema is already up to date.');
    return;
  }
  if (storageSchemaMeta.version === 0) {
    // Adds the "connections by room" index.
    storageSchemaMeta = await migrateStorageSchemaToVersion(
      storage,
      lc,
      storageSchemaMeta,
      1,
      0,
      async () => {
        // The code deploy triggering this migration will have restarted
        // all room dos causing all connections to be closed.
        // Instead of building the "connections by room" index from
        // the "connection" entries, simply delete all "connection" entries
        // and any existing "connections by room" index entries.
        for (const [prefix, desc] of [
          [CONNECTION_KEY_PREFIX, 'connection entries'],
          [
            CONNECTIONS_BY_ROOM_INDEX_PREFIX,
            'connections by room index entries',
          ],
        ]) {
          let deleteCount = 0;
          for await (const keyString of getKeyStrings(storage, prefix)) {
            if (deleteCount === 0) {
              lc.info?.('First delete of', desc, keyString);
            }
            await storage.delete(keyString);
            deleteCount++;
            // Every 10,000 deletes force sync of pending writes to disk
            // to ensure that if migration runs out of time and is killed
            // at least some forward progress has been sync'd to disk.
            // Also flush logs about this progress.
            if (deleteCount % 10000 === 0) {
              await storage.sync();
              lc.info?.('Deleted', deleteCount, desc, 'so far.', keyString);
              await logSink.flush?.();
            }
          }
          lc.info?.('Deleted', deleteCount, desc, 'in total.');
        }
      },
    );
  }
  lc.info?.('Storage schema update complete.');
  await logSink.flush?.();
}
