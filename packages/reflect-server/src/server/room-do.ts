import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {
  createRoomRequestSchema,
  invalidateForRoomRequestSchema,
  invalidateForUserRequestSchema,
} from 'reflect-protocol';
import {disconnectSchema} from 'reflect-protocol/src/disconnect.js';
import type {Env, MutatorDefs} from 'reflect-shared';
import {version} from 'reflect-shared';
import {getConfig} from 'reflect-shared/src/config.js';
import {BufferSizer} from 'shared/src/buffer-sizer.js';
import * as valita from 'shared/src/valita.js';
import {ConnectionLifetimeReporter} from '../events/connection-lifetimes.js';
import {ConnectionSecondsReporter} from '../events/connection-seconds.js';
import type {MutatorMap} from '../process/process-mutation.js';
import {processPending} from '../process/process-pending.js';
import {processRoomStart} from '../process/process-room-start.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {
  ConnectionCountTrackingClientMap,
  type ClientID,
  type ClientMap,
  type ClientState,
  type Socket,
} from '../types/client-state.js';
import type {PendingMutation} from '../types/mutation.js';
import {decodeHeaderValue} from '../util/headers.js';
import {LoggingLock} from '../util/lock.js';
import {populateLogContextFromRequest} from '../util/log-context-common.js';
import {randomID} from '../util/rand.js';
import {AlarmManager} from './alarms.js';
import {CLIENT_GC_FREQUENCY} from './client-gc.js';
import {handleClose} from './close.js';
import {handleConnection} from './connect.js';
import {closeConnections, getConnections} from './connections.js';
import type {DisconnectHandler} from './disconnect.js';
import {getRequiredSearchParams} from './get-required-search-params.js';
import {requireUpgradeHeader, upgradeWebsocketResponse} from './http-util.js';
import {ROOM_ID_HEADER_NAME} from './internal-headers.js';
import {handleMessage} from './message.js';
import {
  AUTH_CONNECTIONS_PATH,
  CONNECT_URL_PATTERN,
  CREATE_ROOM_PATH,
  DISCONNECT_BEACON_PATH,
  INTERNAL_CREATE_ROOM_PATH,
  LEGACY_CONNECT_PATH,
  LEGACY_CREATE_ROOM_PATH,
  TAIL_URL_PATH,
} from './paths.js';
import {initRoomSchema} from './room-schema.js';
import type {RoomStartHandler} from './room-start.js';
import {
  BaseContext,
  Handler,
  Router,
  get,
  post,
  requireAuthAPIKey,
  withBody,
} from './router.js';
import {connectTail} from './tail.js';
import {registerUnhandledRejectionHandler} from './unhandled-rejection-handler.js';

const roomIDKey = '/system/roomID';
const deletedKey = '/system/deleted';

export interface RoomDOOptions<MD extends MutatorDefs> {
  mutators: MD;
  state: DurableObjectState;
  authApiKey: string;
  roomStartHandler: RoomStartHandler;
  disconnectHandler: DisconnectHandler;
  logSink: LogSink;
  logLevel: LogLevel;
  allowUnconfirmedWrites: boolean;
  maxMutationsPerTurn: number;
  env: Env;
}

export const ROOM_ROUTES = {
  deletePath: '/api/room/v0/room/:roomID/delete',
  authInvalidateAll: '/api/auth/v0/invalidateAll',
  authInvalidateForUser: '/api/auth/v0/invalidateForUser',
  authInvalidateForRoom: '/api/auth/v0/invalidateForRoom',
  authConnections: AUTH_CONNECTIONS_PATH,
  legacyCreateRoom: LEGACY_CREATE_ROOM_PATH,
  createRoom: CREATE_ROOM_PATH,
  internalCreateRoom: INTERNAL_CREATE_ROOM_PATH,
  legacyConnect: LEGACY_CONNECT_PATH,
  connect: CONNECT_URL_PATTERN,
  disconnectBeacon: DISCONNECT_BEACON_PATH,
  tail: TAIL_URL_PATH,
} as const;

export class BaseRoomDO<MD extends MutatorDefs> implements DurableObject {
  readonly #clients: ClientMap;
  readonly #pendingMutations: PendingMutation[] = [];
  readonly #bufferSizer = new BufferSizer({
    initialBufferSizeMs: 25,
    minBufferSizeMs: 0,
    maxBufferSizeMs: 500,
    adjustBufferSizeIntervalMs: 5_000,
  });
  #maxProcessedMutationTimestamp = 0;
  readonly #lock = new LoggingLock();
  readonly #mutators: MutatorMap;
  readonly #roomStartHandler: RoomStartHandler;
  readonly #disconnectHandler: DisconnectHandler;
  readonly #maxMutationsPerTurn: number;
  #roomIDDependentInitCompleted = false;
  #lc: LogContext;
  readonly #storage: DurableStorage;
  readonly #authApiKey: string;
  #turnTimerID: ReturnType<typeof setInterval> | 0 = 0;

  readonly #turnDuration: number;
  readonly #router = new Router();

  readonly #alarm: AlarmManager;
  readonly #connectionSecondsReporter: ConnectionSecondsReporter;
  readonly #connectionLifetimeReporter: ConnectionLifetimeReporter;
  readonly #env: Env;
  #lastGCClientsTimestamp: undefined | number = undefined;

  constructor(options: RoomDOOptions<MD>) {
    const {
      mutators,
      roomStartHandler,
      disconnectHandler,
      state,
      authApiKey,
      logSink,
      logLevel,
      maxMutationsPerTurn,
      env,
    } = options;

    this.#mutators = new Map([...Object.entries(mutators)]) as MutatorMap;
    this.#roomStartHandler = roomStartHandler;
    this.#disconnectHandler = disconnectHandler;
    this.#maxMutationsPerTurn = maxMutationsPerTurn;
    this.#storage = new DurableStorage(
      state.storage,
      options.allowUnconfirmedWrites,
    );

    this.#alarm = new AlarmManager(state.storage);
    this.#connectionSecondsReporter = new ConnectionSecondsReporter(
      this.#alarm.scheduler,
    );
    this.#connectionLifetimeReporter = new ConnectionLifetimeReporter(
      this.#alarm.scheduler,
    );
    this.#env = env;
    this.#clients = new ConnectionCountTrackingClientMap(
      this.#connectionSecondsReporter,
      this.#connectionLifetimeReporter,
    );

    this.#initRoutes();

    this.#turnDuration = getDefaultTurnDuration(options.allowUnconfirmedWrites);
    this.#authApiKey = authApiKey;
    const lc = new LogContext(logLevel, undefined, logSink).withContext(
      'component',
      'RoomDO',
    );
    registerUnhandledRejectionHandler(lc);
    this.#lc = lc.withContext('doID', state.id.toString());

    this.#lc.info?.('Starting RoomDO. Version:', version);

    void state.blockConcurrencyWhile(async () => {
      await initRoomSchema(this.#lc, this.#storage);
    });
  }

  #initRoutes() {
    this.#router.register(ROOM_ROUTES.deletePath, this.#deleteAllData);
    this.#router.register(
      ROOM_ROUTES.authInvalidateAll,
      this.#authInvalidateAll,
    );
    this.#router.register(
      ROOM_ROUTES.authInvalidateForUser,
      this.#authInvalidateForUser,
    );
    this.#router.register(
      ROOM_ROUTES.authInvalidateForRoom,
      this.#authInvalidateForRoom,
    );
    this.#router.register(ROOM_ROUTES.authConnections, this.#authConnections);

    this.#router.register(ROOM_ROUTES.createRoom, this.#createRoom);
    this.#router.register(ROOM_ROUTES.legacyCreateRoom, this.#createRoom);
    this.#router.register(
      ROOM_ROUTES.internalCreateRoom,
      this.#internalCreateRoom,
    );

    this.#router.register(ROOM_ROUTES.connect, this.#connect);
    this.#router.register(ROOM_ROUTES.legacyConnect, this.#connect);

    this.#router.register(ROOM_ROUTES.tail, this.#tail);
    if (getConfig('disconnectBeacon')) {
      this.#router.register(
        ROOM_ROUTES.disconnectBeacon,
        this.#disconnectBeacon,
      );
    }
  }

  #requireAPIKey = <Context extends BaseContext, Resp>(
    next: Handler<Context, Resp>,
  ) => requireAuthAPIKey(() => this.#authApiKey, next);

  async fetch(request: Request): Promise<Response> {
    const lc = populateLogContextFromRequest(this.#lc, request);
    try {
      if (await this.deleted()) {
        return new Response('deleted', {
          status: 410, // Gone
        });
      }
      const roomIDHeaderValue = request.headers.get(ROOM_ID_HEADER_NAME);
      if (roomIDHeaderValue === null || roomIDHeaderValue === '') {
        return new Response('Missing Room ID Header', {status: 500});
      }
      if (!this.#roomIDDependentInitCompleted) {
        await this.#lock.withLock(lc, 'initRoomID', async lcInLock => {
          if (this.#roomIDDependentInitCompleted) {
            lcInLock.debug?.('roomID already initialized, returning');
            return;
          }
          const roomID = decodeHeaderValue(roomIDHeaderValue);
          await processRoomStart(
            lcInLock,
            this.#env,
            this.#roomStartHandler,
            this.#storage,
            roomID,
          );
          this.#lc = this.#lc.withContext('roomID', roomID);
          this.#roomIDDependentInitCompleted = true;
          this.#connectionSecondsReporter.setRoomID(roomID);
          lc.info?.('initialized roomID');
        });
      }

      return await this.#router.dispatch(request, {lc});
    } catch (e) {
      lc.error?.('Unhandled exception in fetch', e);
      return new Response(
        e instanceof Error ? e.message : 'Unexpected error.',
        {status: 500},
      );
    }
  }

  #setRoomID(roomID: string) {
    return this.#storage.put(roomIDKey, roomID);
  }

  maybeRoomID(): Promise<string | undefined> {
    return this.#storage.get(roomIDKey, valita.string());
  }

  #setDeleted() {
    return this.#storage.put(deletedKey, true);
  }

  async deleted(): Promise<boolean> {
    return (await this.#storage.get(deletedKey, valita.boolean())) === true;
  }

  /**
   * _internalCreateRoom does not require an API key. It is used by the
   * _createRoom after it has validated the API key. It is also used as an RPC
   * from the AuthDO.
   *
   */
  #internalCreateRoom = withBody(createRoomRequestSchema, async ctx => {
    const {roomID} = ctx.body;
    this.#lc.info?.('Handling create room request for roomID', roomID);
    await this.#setRoomID(roomID);
    await this.#storage.flush();
    this.#lc.debug?.('Flushed roomID to storage', roomID);
    return new Response('ok');
  });

  #createRoom = this.#requireAPIKey(this.#internalCreateRoom);

  // There's a bit of a question here about whether we really want to delete *all* the
  // data when a room is deleted. This deletes everything, including values kept by the
  // system e.g. the roomID. If we store more system keys in the future we might want to have
  // delete room only delete the room user data and not the system keys, because once
  // system keys are deleted who knows what behavior the room will have when its apis are
  // called. Maybe it's fine if they error out, dunno.
  #deleteAllData = post(
    this.#requireAPIKey(async ctx => {
      const {lc} = ctx;
      // Maybe we should validate that the roomID in the request matches?
      lc.info?.('delete all data');
      await this.#storage.deleteAll();
      lc.info?.('done deleting all data');
      await this.#setDeleted();
      return new Response('ok');
    }),
  );

  #connect = get((ctx, request) => {
    const {lc} = ctx;
    const errorResponse = requireUpgradeHeader(request, lc);
    if (errorResponse) {
      return errorResponse;
    }

    const {0: clientWS, 1: serverWS} = new WebSocketPair();
    const url = new URL(request.url);
    lc.debug?.('connection request', url.toString(), 'waiting for lock');
    serverWS.accept();

    void this.#lock
      .withLock(lc, 'handleConnection', async lc => {
        await handleConnection(
          lc,
          serverWS,
          this.#storage,
          url,
          request.headers,
          this.#clients,
          this.#handleMessage,
          this.#handleClose,
        );
        this.#processUntilDone(lc);
      })
      .catch(e => {
        lc.error?.('unhandled exception in handleConnection', e);
      });

    return upgradeWebsocketResponse(clientWS, request.headers);
  });

  #disconnectBeacon = post(async (ctx, request) => {
    const {lc} = ctx;
    const {searchParams} = new URL(request.url);
    const [[clientID, roomID, userID], errorResponse] = getRequiredSearchParams(
      ['clientID', 'roomID', 'userID'],
      searchParams,
      (message: string) => new Response(message, {status: 400}),
    );

    if (errorResponse) {
      lc.debug?.('Failed to get roomID and userID');
      return errorResponse;
    }

    lc.debug?.('disconnect client beacon request', roomID, userID);

    const bodyJSON = await request.json();
    const pushBody = valita.parse(bodyJSON, disconnectSchema);
    lc.debug?.('client disconnect request', clientID, pushBody);

    // TODO(arv): Apply the mutations if any.
    // TODO(arv): Delete the client record.
    // TODO(arv): Collect the presence state.

    return new Response('ok');
  });

  #tail = get((ctx, request) => {
    const {lc} = ctx;
    lc.debug?.('tail request', request.url);

    const errorResponse = requireUpgradeHeader(request, lc);
    if (errorResponse) {
      return errorResponse;
    }

    const {0: clientWS, 1: serverWS} = new WebSocketPair();

    serverWS.accept();
    connectTail(serverWS);

    return upgradeWebsocketResponse(clientWS, request.headers);
  });

  #authInvalidateForRoom = post(
    this.#requireAPIKey(
      withBody(invalidateForRoomRequestSchema, async ctx => {
        const {lc, body} = ctx;
        const {roomID} = body;
        lc.debug?.(
          `Closing room ${roomID}'s connections fulfilling auth api invalidateForRoom request.`,
        );
        await this.#closeConnections(_ => true);
        return new Response('Success', {status: 200});
      }),
    ),
  );

  #authInvalidateForUser = post(
    this.#requireAPIKey(
      withBody(invalidateForUserRequestSchema, async ctx => {
        const {lc, body} = ctx;
        const {userID} = body;
        lc.debug?.(
          `Closing user ${userID}'s connections fulfilling auth api invalidateForUser request.`,
        );
        await this.#closeConnections(
          clientState => clientState.auth.userID === userID,
        );
        return new Response('Success', {status: 200});
      }),
    ),
  );

  #authInvalidateAll = post(
    this.#requireAPIKey(async ctx => {
      const {lc} = ctx;
      lc.debug?.(
        'Closing all connections fulfilling auth api invalidateAll request.',
      );
      await this.#closeConnections(_ => true);
      return new Response('Success', {status: 200});
    }),
  );

  #authConnections = post(
    this.#requireAPIKey(ctx => {
      const {lc} = ctx;
      lc.debug?.('Retrieving all auth connections');
      return new Response(JSON.stringify(getConnections(this.#clients)));
    }),
  );

  #closeConnections(
    predicate: (clientState: ClientState) => boolean,
  ): Promise<void> {
    return this.#lock.withLock(this.#lc, 'closeConnections', () =>
      closeConnections(this.#clients, predicate),
    );
  }

  #handleMessage = (
    lc: LogContext,
    clientID: ClientID,
    data: string,
    ws: Socket,
  ): void => {
    void this.#handleMessageInner(lc, clientID, data, ws);
  };

  async #handleMessageInner(
    lc: LogContext,
    clientID: ClientID,
    data: string,
    ws: Socket,
  ): Promise<void> {
    lc = lc.withContext('msgID', randomID());
    lc.debug?.('handling message', data);

    try {
      await this.#lock.withLock(lc, 'handleMessage', async lc => {
        await handleMessage(
          lc,
          this.#storage,
          this.#clients,
          this.#pendingMutations,
          clientID,
          data,
          ws,
          () => this.#processUntilDone(lc),
        );
      });
    } catch (e) {
      lc.error?.('Unhandled exception in handleMessage', e);
    }
  }

  async alarm(): Promise<void> {
    const lc = this.#lc.withContext('handler', 'alarm');
    await this.#alarm.fireScheduled(lc);
  }

  #processUntilDone(lc: LogContext) {
    lc.debug?.('handling processUntilDone');
    if (this.#turnTimerID) {
      lc.debug?.('already processing, nothing to do');
      return;
    }

    this.#turnTimerID = this.runInLockAtInterval(
      // The logging in turn processing should use this.#lc (i.e. the RoomDO's
      // general log context), rather than lc which has the context of a
      // specific request/connection
      this.#lc,
      '#processNext',
      this.#turnDuration,
      logContext => this.#processNextInLock(logContext),
    );
  }

  // Exposed for testing.
  runInLockAtInterval(
    lc: LogContext,
    name: string,
    interval: number,
    callback: (lc: LogContext) => Promise<void>,
    beforeQueue = () => {
      /* hook for testing */
    },
  ): ReturnType<typeof setInterval> {
    let queued = false;

    return setInterval(async () => {
      beforeQueue(); // Hook for testing.

      // setInterval() is recommended to only be used with logic that completes within the interval:
      //
      // https://developer.mozilla.org/en-US/docs/Web/API/setInterval#ensure_that_execution_duration_is_shorter_than_interval_frequency
      //
      // We do not have this guarantee with the `callback`, and because calls are serialized by the lock,
      // a long invocation can result in setInterval() queueing up many subsequent invocations and
      // consequently hogging the lock.
      //
      // To avoid this self-DOS situation, we only allow one invocation to be queued, meanwhile
      // aborting redundant invocations fired by setInterval().
      if (queued) {
        lc.info?.(
          `Previous ${name} is still queued. Dropping redundant invocation.`,
        );
        return;
      }
      queued = true;

      await this.#lock.withLock(
        lc,
        name,
        async lc => {
          queued = false;
          await callback(lc).catch(e => {
            lc.error?.(`Unhandled exception in ${name}`, e);
          });
        },
        // The callback is expected to run close to and occasionally exceed the interval.
        // Log if it runs for more than 1.5x the interval.
        interval * 1.5,
      );
    }, interval);
  }

  async #processNextInLock(lc: LogContext) {
    const {maxProcessedMutationTimestamp, nothingToProcess} =
      await processPending(
        lc,
        this.#env,
        this.#storage,
        this.#clients,
        this.#pendingMutations,
        this.#mutators,
        this.#disconnectHandler,
        this.#maxProcessedMutationTimestamp,
        this.#bufferSizer,
        this.#maxMutationsPerTurn,
        (now: number) => this.#shouldGCClients(now),
      );
    this.#maxProcessedMutationTimestamp = maxProcessedMutationTimestamp;
    if (nothingToProcess && this.#turnTimerID) {
      clearInterval(this.#turnTimerID);
      this.#turnTimerID = 0;
    }
  }

  #shouldGCClients(now: number): boolean {
    if (
      this.#lastGCClientsTimestamp === undefined ||
      now - this.#lastGCClientsTimestamp > CLIENT_GC_FREQUENCY
    ) {
      this.#lastGCClientsTimestamp = now;
      return true;
    }
    return false;
  }

  #handleClose = async (
    lc: LogContext,
    clientID: ClientID,
    ws: Socket,
  ): Promise<void> => {
    await this.#lock.withLock(lc, '#handleClose', lc => {
      handleClose(lc, this.#clients, clientID, ws);
      this.#processUntilDone(lc);
    });
  };
}

export function getDefaultTurnDuration(
  allowUnconfirmedWrites: boolean,
): number {
  return Math.floor(1000 / (allowUnconfirmedWrites ? 60 : 15));
}
