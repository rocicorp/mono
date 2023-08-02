import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {
  createRoomRequestSchema,
  invalidateForRoomRequestSchema,
  invalidateForUserRequestSchema,
} from 'reflect-protocol';
import {version} from 'reflect-shared';
import type {MutatorDefs} from 'reflect-types/src/mod.js';
import {BufferSizer} from 'shared/src/buffer-sizer.js';
import * as valita from 'shared/src/valita.js';
import type {MutatorMap} from '../process/process-mutation.js';
import {processPending} from '../process/process-pending.js';
import {processRoomStart} from '../process/process-room-start.js';
import {DurableStorage} from '../storage/durable-storage.js';
import type {
  ClientID,
  ClientMap,
  ClientState,
  Socket,
} from '../types/client-state.js';
import type {PendingMutation} from '../types/mutation.js';
import {randomID} from '../util/rand.js';
import {handleClose} from './close.js';
import {handleConnection} from './connect.js';
import {closeConnections, getConnections} from './connections.js';
import type {DisconnectHandler} from './disconnect.js';
import {handleMessage} from './message.js';
import {
  CONNECT_URL_PATTERN,
  CREATE_ROOM_PATH,
  INTERNAL_CREATE_ROOM_PATH,
  LEGACY_CONNECT_PATH,
  LEGACY_CREATE_ROOM_PATH,
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
import {registerUnhandledRejectionHandler} from './unhandled-rejection-handler.js';
import {LoggingLock} from '../util/lock.js';
import {populateLogContextFromRequest} from '../util/log-context-common.js';

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
}

export const ROOM_ROUTES = {
  deletePath: '/api/room/v0/room/:roomID/delete',
  authInvalidateAll: '/api/auth/v0/invalidateAll',
  authInvalidateForUser: '/api/auth/v0/invalidateForUser',
  authInvalidateForRoom: '/api/auth/v0/invalidateForRoom',
  authConnections: '/api/auth/v0/connections',
  legacyCreateRoom: LEGACY_CREATE_ROOM_PATH,
  createRoom: CREATE_ROOM_PATH,
  internalCreateRoom: INTERNAL_CREATE_ROOM_PATH,
  legacyConnect: LEGACY_CONNECT_PATH,
  connect: CONNECT_URL_PATTERN,
} as const;

export class BaseRoomDO<MD extends MutatorDefs> implements DurableObject {
  readonly #clients: ClientMap = new Map();
  readonly #pendingMutations: PendingMutation[] = [];
  readonly #bufferSizer = new BufferSizer({
    initialBufferSizeMs: 200,
    minBufferSizeMs: 0,
    maxBufferSizeMs: 500,
    adjustBufferSizeIntervalMs: 10_000,
  });
  #maxProcessedMutationTimestamp = 0;
  readonly #lock = new LoggingLock();
  readonly #mutators: MutatorMap;
  readonly #disconnectHandler: DisconnectHandler;
  #lcHasRoomIdContext = false;
  #lc: LogContext;
  readonly #storage: DurableStorage;
  readonly #authApiKey: string;
  #turnTimerID: ReturnType<typeof setInterval> | 0 = 0;

  readonly #turnDuration: number;
  readonly #router = new Router();

  constructor(options: RoomDOOptions<MD>) {
    const {
      mutators,
      roomStartHandler,
      disconnectHandler,
      state,
      authApiKey,
      logSink,
      logLevel,
    } = options;

    this.#mutators = new Map([...Object.entries(mutators)]) as MutatorMap;
    this.#disconnectHandler = disconnectHandler;
    this.#storage = new DurableStorage(
      state.storage,
      options.allowUnconfirmedWrites,
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

    this.#lc.info?.('Starting server');
    this.#lc.info?.('Version:', version);

    void state.blockConcurrencyWhile(async () => {
      await initRoomSchema(this.#lc, this.#storage);
      await processRoomStart(this.#lc, roomStartHandler, this.#storage);
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
  }

  #requireAPIKey = <Context extends BaseContext, Resp>(
    next: Handler<Context, Resp>,
  ) => requireAuthAPIKey(() => this.#authApiKey, next);

  async fetch(request: Request): Promise<Response> {
    let lc = populateLogContextFromRequest(this.#lc, request);

    try {
      if (await this.deleted()) {
        return new Response('deleted', {
          status: 410, // Gone
        });
      }
      const roomID = await this.maybeRoomID();
      const url = new URL(request.url);
      const urlRoomID = url.searchParams.get('roomID');
      if (
        // roomID is not going to be set on the createRoom request, or after
        // the room has been deleted.
        roomID !== undefined &&
        // roomID is not going to be set for all calls, eg to delete the room.
        urlRoomID !== null &&
        urlRoomID !== roomID
      ) {
        lc.error?.('roomID mismatch', 'urlRoomID', urlRoomID, 'roomID', roomID);
        return new Response('Unexpected roomID', {status: 400});
      }

      if (!this.#lcHasRoomIdContext) {
        await this.#lock.withLock(lc, 'initRoomIDContext', lcInLock => {
          if (this.#lcHasRoomIdContext) {
            lcInLock.debug?.('roomID context already initialized, returning');
            return;
          }
          if (urlRoomID !== null && roomID === undefined) {
            lcInLock.error?.('Expected roomID to be present in storage', {
              urlRoomID,
            });
          }
          if (roomID || urlRoomID) {
            const roomIDForContext = roomID ?? urlRoomID;
            this.#lc = this.#lc.withContext('roomID', roomIDForContext);
            lc = lc.withContext('roomID', roomIDForContext);
            this.#lcHasRoomIdContext = true;
            lc.info?.('initialized roomID context');
          }
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

  // roomID errors and returns "unknown" if the roomID is not set. Prefer
  // roomID() to maybeRoomID() in cases where the roomID is expected to be set,
  // which is most cases.
  async roomID(lc: LogContext): Promise<string> {
    const roomID = await this.maybeRoomID();
    if (roomID !== undefined) {
      return roomID;
    }
    lc.error?.('roomID is not set');
    return 'unknown';
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
    if (request.headers.get('Upgrade') !== 'websocket') {
      lc.error?.('roomDO: missing Upgrade header');
      return new Response('expected websocket', {status: 400});
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

    return new Response(null, {status: 101, webSocket: clientWS});
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

  #handleMessage = async (
    lc: LogContext,
    clientID: ClientID,
    data: string,
    ws: Socket,
  ): Promise<void> => {
    lc = lc.withContext('msgID', randomID());
    lc.debug?.('handling message', data, 'waiting for lock');

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
      lc.error?.('Unhandled exception in _handleMessage', e);
    }
  };

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
  ): NodeJS.Timer {
    let queued = false;

    return setInterval(async () => {
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
        this.#storage,
        this.#clients,
        this.#pendingMutations,
        this.#mutators,
        this.#disconnectHandler,
        this.#maxProcessedMutationTimestamp,
        this.#bufferSizer,
      );
    this.#maxProcessedMutationTimestamp = maxProcessedMutationTimestamp;
    if (nothingToProcess && this.#turnTimerID) {
      clearInterval(this.#turnTimerID);
      this.#turnTimerID = 0;
    }
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
  return 1000 / (allowUnconfirmedWrites ? 60 : 15);
}
