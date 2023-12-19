import {Lock, RWLock} from '@rocicorp/lock';
import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import type {ErrorKind} from 'reflect-protocol';
import {
  ConnectionsResponse,
  connectionsResponseSchema,
  createRoomRequestSchema,
} from 'reflect-protocol';
import type {TailErrorKind} from 'reflect-protocol/src/tail.js';
import type {AuthData, Env} from 'reflect-shared';
import {version} from 'reflect-shared';
import {getConfig} from 'reflect-shared/src/config.js';
import {assert} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import {timed} from 'shared/src/timed.js';
import * as valita from 'shared/src/valita.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {encodeHeaderValue, getBearerToken} from '../util/headers.js';
import {populateLogContextFromRequest} from '../util/log-context-common.js';
import {sleep} from '../util/sleep.js';
import {
  createWSAndCloseWithError,
  createWSAndCloseWithTailError,
} from '../util/socket.js';
import {AlarmManager, TimeoutID} from './alarms.js';
import {roomNotFoundAPIError} from './api-errors.js';
import {initAuthDOSchema} from './auth-do-schema.js';
import type {AuthHandler} from './auth.js';
import {ErrorWithForwardedResponse, makeErrorResponse} from './errors.js';
import {getRequiredSearchParams} from './get-required-search-params.js';
import {requireUpgradeHeader} from './http-util.js';
import {AUTH_DATA_HEADER_NAME, addRoomIDHeader} from './internal-headers.js';
import {listParamsSchema, makeListControl} from './list.js';
import {
  CLOSE_ROOM_PATH,
  CONNECT_URL_PATTERN,
  CREATE_ROOM_PATH,
  DELETE_ROOM_PATH,
  DISCONNECT_BEACON_PATH,
  GET_ROOM_PATH,
  INVALIDATE_ALL_CONNECTIONS_PATH,
  INVALIDATE_ROOM_CONNECTIONS_PATH,
  INVALIDATE_USER_CONNECTIONS_PATH,
  LIST_ROOMS_PATH,
  TAIL_URL_PATH,
} from './paths.js';
import {ROOM_ROUTES} from './room-do.js';
import {
  RoomRecord,
  RoomStatus,
  closeRoom,
  createRoom,
  deleteRoom,
  internalCreateRoom,
  objectIDByRoomID,
  roomProperties,
  roomPropertiesByRoomID,
  roomRecordByRoomID,
} from './rooms.js';
import {
  BaseContext,
  Router,
  bodyOnly,
  get,
  noInputParams,
  post,
  queryParams,
  roomID,
  urlVersion,
  userID,
} from './router.js';
import {registerUnhandledRejectionHandler} from './unhandled-rejection-handler.js';

export const AUTH_HANDLER_TIMEOUT_MS = 5_000;

export interface AuthDOOptions {
  roomDO: DurableObjectNamespace;
  state: DurableObjectState;
  authHandler?: AuthHandler | undefined;
  logSink: LogSink;
  logLevel: LogLevel;
  env: Env;
}
export type ConnectionKey = {
  userID: string;
  roomID: string;
  clientID: string;
};

const connectionRecordSchema = valita.object({
  connectTimestamp: valita.number(),
});

const connectionsByRoomSchema = valita.object({});

export type ConnectionRecord = valita.Infer<typeof connectionRecordSchema>;

export const AUTH_ROUTES_AUTHED_BY_API_KEY = {
  listRoomProperties: LIST_ROOMS_PATH,
  getRoomProperties: GET_ROOM_PATH,
  closeRoom: CLOSE_ROOM_PATH,
  deleteRoom: DELETE_ROOM_PATH,
  authInvalidateAll: INVALIDATE_ALL_CONNECTIONS_PATH,
  authInvalidateForUser: INVALIDATE_USER_CONNECTIONS_PATH,
  authInvalidateForRoom: INVALIDATE_ROOM_CONNECTIONS_PATH,
  createRoom: CREATE_ROOM_PATH,
} as const;

export const AUTH_WEBSOCKET_ROUTES_AUTHED_BY_API_KEY = {
  tail: TAIL_URL_PATH,
};

export const AUTH_ROUTES_CUSTOM_AUTH = {
  connect: CONNECT_URL_PATTERN,
  disconnectBeacon: DISCONNECT_BEACON_PATH,
} as const;

export const AUTH_ROUTES_UNAUTHED = {
  canaryWebSocket: '/api/canary/v0/websocket',
} as const;

export const AUTH_ROUTES = {
  ...AUTH_ROUTES_AUTHED_BY_API_KEY,
  ...AUTH_WEBSOCKET_ROUTES_AUTHED_BY_API_KEY,
  ...AUTH_ROUTES_CUSTOM_AUTH,
  ...AUTH_ROUTES_UNAUTHED,
} as const;

export const ALARM_INTERVAL = 5 * 60 * 1000;

export class BaseAuthDO implements DurableObject {
  readonly #router = new Router();
  readonly #roomDO: DurableObjectNamespace;
  // _durableStorage is a type-aware wrapper around _state.storage. It
  // always disables the input gate. The output gate is configured in the
  // constructor below. Anything that needs to read *values* out of
  // storage should probably use _durableStorage.
  readonly #durableStorage: DurableStorage;
  readonly #authHandler: AuthHandler | undefined;
  readonly #lc: LogContext;
  readonly #alarm: AlarmManager;
  readonly #env: Env;

  #revalidateConnectionsTimeoutID: TimeoutID = 0;

  // _authLock ensures that at most one auth api call is processed at a time.
  // For safety, if something requires both the auth lock and the room record
  // lock, the auth lock MUST be acquired first.
  readonly #authLock = new RWLock();
  // _roomRecordLock ensure that at most one write operation is in
  // progress on a RoomRecord at a time. For safety, if something requires
  // both the auth lock and the room record lock, the auth lock MUST be
  // acquired first.
  readonly #roomRecordLock = new RWLock();

  readonly #authRevalidateConnectionsLock = new Lock();

  constructor(options: AuthDOOptions) {
    const {roomDO, state, authHandler, logSink, logLevel, env} = options;
    this.#roomDO = roomDO;
    this.#durableStorage = new DurableStorage(
      state.storage,
      false, // don't allow unconfirmed
    );
    this.#authHandler = authHandler;
    const lc = new LogContext(logLevel, undefined, logSink).withContext(
      'component',
      'AuthDO',
    );
    registerUnhandledRejectionHandler(lc);
    this.#lc = lc.withContext('doID', state.id.toString());
    this.#alarm = new AlarmManager(state.storage);
    this.#env = env;

    this.#initRoutes();
    this.#lc.info?.('Starting AuthDO. Version:', version);
    void state.blockConcurrencyWhile(() =>
      initAuthDOSchema(this.#lc, this.#durableStorage),
    );
  }

  async fetch(request: Request): Promise<Response> {
    const lc = populateLogContextFromRequest(this.#lc, request);
    lc.info?.('Handling request:', request.url);
    try {
      const resp = await this.#router.dispatch(request, {lc});
      lc.info?.(`Returning response: ${resp.status} ${resp.statusText}`);
      return resp;
    } catch (e) {
      lc.error?.('Unhandled exception in fetch', e);
      return makeErrorResponse(e);
    }
  }

  #getRoomProperties = get()
    .with(roomID())
    .with(noInputParams())
    .handleAPIResult(async ctx => {
      const {roomID} = ctx;
      const roomProperties = await this.#roomRecordLock.withRead(() =>
        roomPropertiesByRoomID(this.#durableStorage, ctx.roomID),
      );
      if (roomProperties === undefined) {
        throw roomNotFoundAPIError(roomID);
      }
      return roomProperties;
    });

  #listRoomProperties = get()
    .with(queryParams(listParamsSchema))
    .handleAPIResult(async ctx => {
      const {query: listParams} = ctx;
      const listControl = makeListControl(listParams, 1000);
      const roomIDToProperties = await this.#roomRecordLock.withRead(() =>
        roomProperties(this.#durableStorage, listControl.getOptions()),
      );
      return listControl.makeListResults(roomIDToProperties);
    });

  #createRoom = post()
    .with(roomID())
    .with(bodyOnly(createRoomRequestSchema))
    .handleAPIResult((ctx, req) => {
      const {lc, body, roomID} = ctx;
      const {jurisdiction} = body;
      return this.#roomRecordLock.withWrite(() =>
        createRoom(
          lc,
          this.#roomDO,
          this.#durableStorage,
          // Note: we need to copy the request here because we read the body.
          new Request(req, {body: JSON.stringify(body)}),
          roomID,
          jurisdiction,
        ),
      );
    });

  // A call to closeRoom should be followed by a call to authInvalidateForRoom
  // to ensure users are logged out.
  #closeRoom = post()
    .with(roomID())
    .with(noInputParams())
    .handleAPIResult(ctx =>
      this.#roomRecordLock.withWrite(() =>
        closeRoom(ctx.lc, this.#durableStorage, ctx.roomID),
      ),
    );

  // A room must first be closed before it can be deleted. Once deleted, a room
  // will return 410 Gone for all requests.
  #deleteRoom = post()
    .with(roomID())
    .with(noInputParams())
    .handleAPIResult((ctx, req) =>
      this.#roomRecordLock.withWrite(() =>
        deleteRoom(ctx.lc, this.#roomDO, this.#durableStorage, ctx.roomID, req),
      ),
    );

  #tail = get().handle(async (ctx: BaseContext, request) => {
    const {lc} = ctx;
    lc.info?.('authDO received websocket tail request:', request.url);

    const errorResponse = requireUpgradeHeader(request, lc);
    if (errorResponse) {
      return errorResponse;
    }

    // From this point forward we want to return errors over the websocket so
    // the client can see them.
    //
    // See comment in #connectImpl for more details.

    const closeWithErrorLocal = (errorKind: TailErrorKind, msg: string) =>
      createWSAndCloseWithTailError(lc, request, errorKind, msg);

    const url = new URL(request.url);
    const roomID = url.searchParams.get('roomID');
    if (!roomID) {
      return closeWithErrorLocal(
        'InvalidConnectionRequest',
        'roomID parameter required',
      );
    }

    const roomRecord = await this.#roomRecordLock.withRead(() =>
      roomRecordByRoomID(this.#durableStorage, roomID),
    );
    if (roomRecord === undefined) {
      return closeWithErrorLocal('RoomNotFound', `room not found: ${roomID}`);
    }

    const roomObjectID = this.#roomDO.idFromString(roomRecord.objectIDString);

    // Forward the request to the Room Durable Object...
    const stub = this.#roomDO.get(roomObjectID);
    const requestToDO = new Request(request);
    return roomDOFetch(requestToDO, 'tail', stub, roomID, lc);
  });

  #disconnectBeacon = post().handle((ctx: BaseContext, request) => {
    const {lc} = ctx;
    lc.info?.('authDO received disconnect beacon request:', request.url);

    // TODO(arv): This code is pretty similar to the code in #connectImpl. Consider refactoring.

    const {searchParams} = new URL(request.url);
    const [[roomID, userID], errorResponse] = getRequiredSearchParams(
      ['roomID', 'userID'],
      searchParams,
      msg => new Response(msg, {status: 400}),
    );
    if (errorResponse) {
      return errorResponse;
    }

    const [decodedAuth, errorResponse2] = getBearerToken(request.headers);
    if (errorResponse2) {
      return errorResponse2;
    }

    return timed(lc.debug, 'inside authLock', () =>
      this.#authLock.withRead(async () => {
        const makeUnauthorizedResponse = (msg: string) =>
          new Response(msg, {status: 403});
        const [authData, errorResponse] = await this.callAuthHandlerIfDefined(
          userID,
          decodedAuth,
          roomID,
          lc,
          makeUnauthorizedResponse,
        );
        if (errorResponse) {
          return errorResponse;
        }

        const roomRecord = await timed(lc.debug, 'looking up roomRecord', () =>
          this.#roomRecordLock.withRead(
            // Check if room already exists.
            () => roomRecordByRoomID(this.#durableStorage, roomID),
          ),
        );

        if (!roomRecord) {
          return new Response('Room not found', {status: 404});
        }

        if (roomRecord.status !== RoomStatus.Open) {
          return new Response('Room closed', {status: 410 /* Gone */});
        }

        return this.#forwardRequestToRoomDO(
          roomRecord,
          request,
          authData,
          roomID,
          lc,
        );
      }),
    );
  });

  #forwardRequestToRoomDO(
    roomRecord: RoomRecord,
    request: Request,
    authData: AuthData,
    roomID: string,
    lc: LogContext,
  ): Promise<Response> {
    const roomObjectID = this.#roomDO.idFromString(roomRecord.objectIDString);

    // Forward the request to the Room Durable Object...
    const stub = this.#roomDO.get(roomObjectID);
    const requestToDO = new Request(request);
    requestToDO.headers.set(
      AUTH_DATA_HEADER_NAME,
      encodeHeaderValue(JSON.stringify(authData)),
    );
    return roomDOFetch(requestToDO, 'disconnect beacon', stub, roomID, lc);
  }

  #initRoutes() {
    this.#router.register(
      AUTH_ROUTES.listRoomProperties,
      this.#listRoomProperties,
    );
    this.#router.register(
      AUTH_ROUTES.getRoomProperties,
      this.#getRoomProperties,
    );
    this.#router.register(AUTH_ROUTES.closeRoom, this.#closeRoom);
    this.#router.register(AUTH_ROUTES.createRoom, this.#createRoom);
    this.#router.register(AUTH_ROUTES.deleteRoom, this.#deleteRoom);
    this.#router.register(
      AUTH_ROUTES.authInvalidateAll,
      this.#authInvalidateAll,
    );
    this.#router.register(
      AUTH_ROUTES.authInvalidateForUser,
      this.#authInvalidateForUser,
    );
    this.#router.register(
      AUTH_ROUTES.authInvalidateForRoom,
      this.#authInvalidateForRoom,
    );

    this.#router.register(AUTH_ROUTES.connect, this.#connect);
    this.#router.register(AUTH_ROUTES.canaryWebSocket, this.#canaryWebSocket);
    this.#router.register(AUTH_ROUTES.tail, this.#tail);
    if (getConfig('disconnectBeacon')) {
      this.#router.register(
        AUTH_ROUTES.disconnectBeacon,
        this.#disconnectBeacon,
      );
    }
  }

  #canaryWebSocket = get().handle((ctx: BaseContext, request) => {
    const url = new URL(request.url);
    const checkID = url.searchParams.get('id') ?? 'missing';
    const wSecWebSocketProtocolHeader =
      url.searchParams.get('wSecWebSocketProtocolHeader') === 'true';

    const lc = ctx.lc
      .withContext('connectCheckID', checkID)
      .withContext(
        'checkName',
        wSecWebSocketProtocolHeader
          ? 'cfWebSocketWSecWebSocketProtocolHeader'
          : 'cfWebSocket',
      );
    lc.debug?.('Handling WebSocket connection check.');

    const errorResponse = requireUpgradeHeader(request, lc);
    if (errorResponse) {
      return errorResponse;
    }

    const secWebSocketProtocolHeader = request.headers.get(
      'Sec-WebSocket-Protocol',
    );
    const responseHeaders = new Headers();
    if (wSecWebSocketProtocolHeader) {
      if (secWebSocketProtocolHeader === null) {
        return new Response('expected Sec-WebSocket-Protocol', {status: 400});
      }
      lc.debug?.(
        'Setting response Sec-WebSocket-Protocol to',
        secWebSocketProtocolHeader,
      );
      responseHeaders.set('Sec-WebSocket-Protocol', secWebSocketProtocolHeader);
    } else if (secWebSocketProtocolHeader !== null) {
      lc.debug?.(
        'Unexpected Sec-WebSocket-Protocol header',
        secWebSocketProtocolHeader,
      );
    }
    const {0: clientWS, 1: serverWS} = new WebSocketPair();
    serverWS.accept();
    lc.debug?.('Sending hello message');
    serverWS.send('hello');
    let closed = false;
    const onClose = () => {
      lc.debug?.('Socket closed');
      closed = true;
      serverWS.removeEventListener('close', onClose);
    };
    serverWS.addEventListener('close', onClose);
    // The client should close the socket after receiving the first message, but
    // if the socket is still open after 10 seconds close it.
    // We don't aggressively close it because it results in very noisy workerd
    // exception messsages like
    // "disconnected: other end of WebSocketPipe was destroyed"
    // when running locally.
    setTimeout(() => {
      if (!closed) {
        closed = true;
        serverWS.removeEventListener('close', onClose);
        lc.debug?.('Closing socket');
        serverWS.close();
      }
    }, 10_000);
    lc.debug?.('Returning response', {
      status: 101,
      headers: responseHeaders.toString(),
    });
    return new Response(null, {
      status: 101,
      headers: responseHeaders,
      webSocket: clientWS,
    });
  });

  #connect = get()
    .with(urlVersion())
    .handle((ctx, request) => {
      const {lc, version} = ctx;
      return this.#connectImpl(lc, version, request);
    });

  #connectImpl(lc: LogContext, version: number, request: Request) {
    const {url} = request;
    lc.info?.('authDO received websocket connection request:', url);

    {
      const errorResponse = requireUpgradeHeader(request, lc);
      if (errorResponse) {
        return errorResponse;
      }
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
      createWSAndCloseWithError(lc, request, errorKind, msg);

    const encodedAuth = request.headers.get('Sec-WebSocket-Protocol');
    if (this.#authHandler && !encodedAuth) {
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
    // TODO apparently many of these checks are not tested :(

    const {searchParams} = new URL(url);
    const [[clientID, roomID, userID], errorResponse] = getRequiredSearchParams(
      ['clientID', 'roomID', 'userID'],
      searchParams,
      msg => closeWithErrorLocal('InvalidConnectionRequest', msg),
    );
    if (errorResponse) {
      return errorResponse;
    }

    const jurisdiction = searchParams.get('jurisdiction') ?? undefined;
    if (jurisdiction && jurisdiction !== 'eu') {
      return closeWithErrorLocal(
        'InvalidConnectionRequest',
        'invalid jurisdiction parameter',
      );
    }
    assert(jurisdiction === undefined || jurisdiction === 'eu');

    let decodedAuth: string | undefined;
    if (encodedAuth) {
      try {
        decodedAuth = decodeURIComponent(encodedAuth);
      } catch {
        return closeWithErrorLocal(
          'InvalidConnectionRequest',
          'malformed auth',
        );
      }
    }

    return timed(lc.debug, 'inside authLock', () =>
      this.#authLock.withRead(async () => {
        const makeUnauthorizedResponse = (msg: string) =>
          closeWithErrorLocal('Unauthorized', msg);
        const [authData, errorResponse] = await this.callAuthHandlerIfDefined(
          userID,
          decodedAuth,
          roomID,
          lc,
          makeUnauthorizedResponse,
        );
        if (errorResponse) {
          return errorResponse;
        }

        // Find the room's objectID so we can connect to it. Do this BEFORE
        // writing the connection record, in case it doesn't exist or is
        // closed/deleted.
        let roomRecord = await timed(lc.debug, 'looking up roomRecord', () =>
          this.#roomRecordLock.withRead(
            // Check if room already exists.
            () => roomRecordByRoomID(this.#durableStorage, roomID),
          ),
        );

        if (!roomRecord) {
          roomRecord = await timed(lc.debug, 'creating roomRecord', () =>
            this.#roomRecordLock.withWrite(async () => {
              // checking again in case it was created while we were waiting for writeLock
              const rr = await roomRecordByRoomID(this.#durableStorage, roomID);
              if (rr) {
                return rr;
              }
              lc.debug?.('room not found, trying to create it');

              try {
                await internalCreateRoom(
                  lc,
                  this.#roomDO,
                  this.#durableStorage,
                  roomID,
                  jurisdiction,
                );
              } catch (e) {
                // Errors are thrown as APIErrors.
                return undefined;
              }
              return roomRecordByRoomID(this.#durableStorage, roomID);
            }),
          );
        }

        // If the room is closed or we failed to implicitly create it, we need to
        // give the client some visibility into this. If we just return a 404 here
        // without accepting the connection the client doesn't have any access to
        // the return code or body. So we accept the connection and send an error
        // message to the client, then close the connection. We trust it will be
        // logged by onSocketError in the client.

        if (roomRecord === undefined || roomRecord.status !== RoomStatus.Open) {
          const kind = roomRecord ? 'RoomClosed' : 'RoomNotFound';
          return createWSAndCloseWithError(lc, request, kind, roomID);
        }

        // Record the connection in DO storage
        await timed(lc.debug, 'writing connection record', () =>
          recordConnection(
            {
              userID: authData.userID,
              roomID,
              clientID,
            },
            this.#durableStorage,
            {
              connectTimestamp: Date.now(),
            },
          ),
        );

        const responseFromDO = await this.#forwardRequestToRoomDO(
          roomRecord,
          request,
          authData,
          roomID,
          lc,
        );
        await this.#scheduleRevalidateConnectionsTask(lc);
        return responseFromDO;
      }),
    );
  }

  async callAuthHandlerIfDefined(
    userID: string,
    decodedAuth: string | undefined,
    roomID: string,
    lc: LogContext,
    makeUnauthorizedResponse: (msg: string) => Response,
  ): Promise<
    | [authData: AuthData, errorResponse: undefined]
    | [authData: undefined, errorResponse: Response]
  > {
    let authData: AuthData = {
      userID,
    };

    const authHandler = this.#authHandler;
    if (authHandler) {
      const auth = must(decodedAuth);

      const timeout = async () => {
        await sleep(AUTH_HANDLER_TIMEOUT_MS);
        throw new Error('authHandler timed out');
      };

      const callHandlerWithTimeout = () =>
        Promise.race([authHandler(auth, roomID, this.#env), timeout()]);

      const [authHandlerAuthData, response] = await timed(
        lc.info,
        'calling authHandler',
        async () => {
          try {
            return [await callHandlerWithTimeout(), undefined] as const;
          } catch (e) {
            return [
              undefined,
              makeUnauthorizedResponse(`authHandler rejected: ${String(e)}`),
            ] as const;
          }
        },
      );
      if (response !== undefined) {
        return [undefined, response];
      }

      if (!authHandlerAuthData || !authHandlerAuthData.userID) {
        if (!authHandlerAuthData) {
          lc.info?.('authData returned by authHandler is not an object.');
        } else if (!authHandlerAuthData.userID) {
          lc.info?.('authData returned by authHandler has no userID.');
        }
        return [undefined, makeUnauthorizedResponse('no authData')];
      }
      if (authHandlerAuthData.userID !== userID) {
        lc.info?.(
          'authData returned by authHandler has a different userID.',
          authHandlerAuthData.userID,
          userID,
        );
        return [
          undefined,
          makeUnauthorizedResponse(
            'userID returned by authHandler must match userID specified in Reflect constructor.',
          ),
        ];
      }
      authData = authHandlerAuthData;
    }

    return [authData, undefined];
  }

  #authInvalidateForRoom = post()
    .with(roomID())
    .with(noInputParams())
    .handleAPIResult((ctx, req) => {
      const {lc, roomID} = ctx;
      lc.debug?.(`authInvalidateForRoom ${roomID} waiting for lock.`);
      return this.#authLock.withWrite(async () => {
        lc.debug?.(`authInvalidateForRoom ${roomID} acquired lock.`);
        if (!(await roomHasConnections(this.#durableStorage, roomID))) {
          lc.debug?.(
            `authInvalidateForRoom ${roomID} no connections to invalidate returning 200.`,
          );
          return;
        }

        lc.debug?.(`Sending authInvalidateForRoom request to ${roomID}`);
        // The request to the Room DO must be completed inside the write lock
        // to avoid races with connect requests for this room.
        const roomObjectID = await this.#roomRecordLock.withRead(() =>
          objectIDByRoomID(this.#durableStorage, this.#roomDO, roomID),
        );
        if (roomObjectID === undefined) {
          throw roomNotFoundAPIError(roomID);
        }
        const stub = this.#roomDO.get(roomObjectID);
        const response = await roomDOFetch(
          req,
          'authInvalidateForRoom',
          stub,
          roomID,
          lc,
        );
        if (!response.ok) {
          lc.debug?.(
            `Received error response from ${roomID}. ${
              response.status
            } ${await response.clone().text()}`,
          );
          throw new ErrorWithForwardedResponse(response);
        }
      });
    });

  #authInvalidateForUser = post()
    .with(userID())
    .with(noInputParams())
    .handleAPIResult((ctx, req) => {
      const {lc, userID} = ctx;
      lc.debug?.(`authInvalidateForUser waiting for lock.`);
      return this.#authLock.withWrite(async () => {
        lc.debug?.(`authInvalidateForUser acquired lock.`);
        const connections = await this.#durableStorage.list(
          {
            prefix: getConnectionKeyStringUserPrefix(userID),
          },
          connectionRecordSchema,
        );
        // The requests to the Room DOs must be completed inside the write lock
        // to avoid races with new connect requests for this user.
        const response = await this.#forwardInvalidateRequest(
          lc,
          'authInvalidateForUser',
          req,
          '',
          connections,
        );
        if (!response.ok) {
          throw new ErrorWithForwardedResponse(response);
        }
      });
    });

  #authInvalidateAll = post()
    .with(noInputParams())
    .handleAPIResult((ctx, req) => {
      const {lc} = ctx;
      lc.debug?.(`authInvalidateAll waiting for lock.`);
      return this.#authLock.withWrite(async () => {
        lc.debug?.(`authInvalidateAll acquired lock.`);
        // The request to the Room DOs must be completed inside the write lock
        // to avoid races with connect requests.
        const response = await this.#forwardInvalidateRequest(
          lc,
          'authInvalidateAll',
          req,
          '',
          // Use async generator because the full list of connections
          // may exceed the DO's memory limits.
          getConnections(this.#durableStorage),
        );
        if (!response.ok) {
          throw new ErrorWithForwardedResponse(response);
        }
      });
    });

  async alarm(): Promise<void> {
    const lc = this.#lc.withContext('handler', 'alarm');
    await this.#alarm.fireScheduled(lc);
  }

  runRevalidateConnectionsTaskForTest() {
    return this.#revalidateConnectionsTask(this.#lc);
  }

  async #revalidateConnectionsTask(lc: LogContext) {
    this.#revalidateConnectionsTimeoutID = 0;
    await this.#authRevalidateConnections(lc);
    if (await hasAnyConnection(this.#durableStorage)) {
      await this.#scheduleRevalidateConnectionsTask(lc);
    }
  }

  async #scheduleRevalidateConnectionsTask(lc: LogContext): Promise<void> {
    lc.debug?.('Ensuring revalidate connections task is scheduled.');
    if (this.#revalidateConnectionsTimeoutID === 0) {
      lc.debug?.('Scheduling revalidate connections task.');
      this.#revalidateConnectionsTimeoutID =
        await this.#alarm.scheduler.promiseTimeout(
          lc => this.#revalidateConnectionsTask(lc),
          ALARM_INTERVAL,
        );
    }
  }

  /**
   * Revalidates all connections in the server by sending a request to the roomDO API.
   * Deletes any connections that are no longer valid.
   */
  #authRevalidateConnections(lc: LogContext): Promise<void> {
    lc.debug?.('Revalidating connections waiting for lock.');
    return this.#authRevalidateConnectionsLock.withLock(async () => {
      lc.debug?.('Revalidating connections acquired lock.');
      const connectionsByRoom = getConnectionsByRoom(this.#durableStorage, lc);
      let connectionCount = 0;
      let revalidatedCount = 0;
      let deleteCount = 0;
      for await (const {roomID, connectionKeys} of connectionsByRoom) {
        connectionCount += connectionKeys.length;
        lc.info?.(
          `Revalidating ${connectionKeys.length} connections for room ${roomID}.`,
        );
        lc.debug?.('waiting for authLock.');
        await this.#authLock.withWrite(async () => {
          lc.debug?.('authLock acquired.');
          const roomObjectID = await this.#roomRecordLock.withRead(() =>
            objectIDByRoomID(this.#durableStorage, this.#roomDO, roomID),
          );
          if (roomObjectID === undefined) {
            lc.error?.(`Can't find room ${roomID}, skipping`);
            return;
          }
          const stub = this.#roomDO.get(roomObjectID);
          const req = new Request(
            `https://unused-reflect-room-do.dev${ROOM_ROUTES.authConnections}`,
            {
              method: 'POST',
            },
          );
          const response = await roomDOFetch(
            req,
            'revalidate connections',
            stub,
            roomID,
            lc,
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
              await deleteConnection(keyToDelete, this.#durableStorage);
            }
            await this.#durableStorage.flush();
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
    });
  }

  async #forwardInvalidateRequest(
    lc: LogContext,
    invalidateRequestName: string,
    request: Request,
    body: string,
    connections:
      | Iterable<[string, ConnectionRecord]>
      | AsyncGenerator<[string, ConnectionRecord]>,
  ): Promise<Response> {
    const roomIDSet = new Set<string>();
    for await (const [keyString] of connections) {
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
      const roomObjectID = await this.#roomRecordLock.withRead(() =>
        objectIDByRoomID(this.#durableStorage, this.#roomDO, roomID),
      );

      if (roomObjectID === undefined) {
        const msg = `No objectID for ${roomID}, skipping`;
        lc.error?.(msg);
        errorResponses.push(new Response(msg, {status: 500}));
        continue;
      }

      const stub = this.#roomDO.get(roomObjectID);
      const req = new Request(request, {body});
      responsePromises.push(
        roomDOFetch(req, 'fwd invalidate request', stub, roomID, lc),
      );
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

export async function roomDOFetch(
  request: Request,
  fetchDescription: string,
  roomDOStub: DurableObjectStub,
  roomID: string,
  lc: LogContext,
): Promise<Response> {
  lc.debug?.(`Sending request ${request.url} to roomDO with roomID ${roomID}`);
  const requestWithRoomID = addRoomIDHeader(new Request(request), roomID);
  const responseFromDO = await timed(
    lc.debug,
    `RoomDO fetch for ${fetchDescription}`,
    async () => {
      try {
        return await roomDOStub.fetch(requestWithRoomID);
      } catch (e) {
        lc.error?.(
          `Exception fetching ${requestWithRoomID.url} from roomDO with roomID ${roomID}`,
          e,
        );
        throw e;
      }
    },
  );
  lc.debug?.(
    'received DO response',
    responseFromDO.status,
    responseFromDO.statusText,
  );
  return responseFromDO;
}

// In the past this prefix was 'connection/',
// and some old reflect deployments may have legacy entries with the
// 'connection/' prefix.
// The prefix was changed due to a customer that had built up so many
// entries that the connection revalidation process was exceeding memory.
// Deleting this large number of entries would take a long time, so instead
// we simply changed prefixes and abandoned the old entries.
const CONNECTION_KEY_PREFIX = 'conn/';
const CONNECTIONS_BY_ROOM_INDEX_PREFIX = 'conns_by_room/';

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

async function roomHasConnections(
  storage: DurableStorage,
  roomID: string,
): Promise<boolean> {
  return (
    (
      await storage.list(
        {prefix: getConnectionRoomIndexPrefix(roomID), limit: 1},
        connectionsByRoomSchema,
      )
    ).size > 0
  );
}

/**
 * Provides a way to iterate over all stored connection keys grouped by
 * room id, in a way that will not exceed memory limits even if not all stored
 * connection keys can fit in memory at once.  It does assume that
 * all connection keys for a given room id can fit in memory.
 */
async function* getConnectionsByRoom(
  storage: DurableStorage,
  lc: LogContext,
): AsyncGenerator<{
  roomID: string;
  connectionKeys: ConnectionKey[];
}> {
  connectionsByRoomSchema;
  let connectionsForRoom:
    | {
        roomID: string;
        connectionKeys: ConnectionKey[];
      }
    | undefined;
  for await (const batch of storage.batchScan(
    {prefix: CONNECTIONS_BY_ROOM_INDEX_PREFIX},
    connectionsByRoomSchema,
    1000,
  )) {
    for (const [key] of batch) {
      const connectionKey = connectionKeyFromRoomIndexString(key);
      if (!connectionKey) {
        lc.error?.('Failed to parse connection room index key', key);
        continue;
      }
      if (
        connectionsForRoom === undefined ||
        connectionsForRoom.roomID !== connectionKey.roomID
      ) {
        if (connectionsForRoom !== undefined) {
          yield connectionsForRoom;
        }
        connectionsForRoom = {
          roomID: connectionKey.roomID,
          connectionKeys: [],
        };
      }
      connectionsForRoom.connectionKeys.push(connectionKey);
    }
    if (connectionsForRoom !== undefined) {
      yield connectionsForRoom;
    }
  }
}

/**
 * Provides a way to iterate over connection records in a way that
 * will not exceed memory limits even if not all connection records can fit in
 * memory at once.  Assumes at least 1000 entries can fit in memory at a time.
 */
async function* getConnections(
  storage: DurableStorage,
): AsyncGenerator<[string, ConnectionRecord]> {
  for await (const batch of storage.batchScan(
    {prefix: CONNECTION_KEY_PREFIX},
    connectionRecordSchema,
    1000,
  )) {
    for (const entry of batch) {
      yield entry;
    }
  }
}

async function hasAnyConnection(storage: DurableStorage): Promise<boolean> {
  const entries = await storage.list(
    {prefix: CONNECTION_KEY_PREFIX, limit: 1},
    connectionRecordSchema,
  );
  return entries.size > 0;
}

export async function recordConnection(
  connectionKey: ConnectionKey,
  storage: DurableStorage,
  record: ConnectionRecord,
) {
  const connectionKeyString = connectionKeyToString(connectionKey);
  const connectionRoomIndexString =
    connectionKeyToConnectionRoomIndexString(connectionKey);
  // done in a single put to ensure atomicity
  await storage.putEntries({
    [connectionKeyString]: record,
    [connectionRoomIndexString]: {},
  });
}

async function deleteConnection(
  connectionKey: ConnectionKey,
  storage: DurableStorage,
) {
  const connectionKeyString = connectionKeyToString(connectionKey);
  const connectionRoomIndexString =
    connectionKeyToConnectionRoomIndexString(connectionKey);
  // done in a single delete to ensure atomicity
  await storage.delEntries([connectionKeyString, connectionRoomIndexString]);
}
