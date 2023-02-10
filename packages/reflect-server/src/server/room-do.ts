import type { MutatorDefs } from "replicache";
import { processPending } from "../process/process-pending.js";
import type { MutatorMap } from "../process/process-mutation.js";
import type {
  ClientID,
  ClientMap,
  ClientState,
  Socket,
} from "../types/client-state.js";
import { Lock } from "@rocicorp/lock";
import { LogSink, LogContext, LogLevel } from "@rocicorp/logger";
import { handleClose } from "./close.js";
import { handleConnection } from "./connect.js";
import { handleMessage } from "./message.js";
import { randomID } from "../util/rand.js";
import { version } from "../util/version.js";
import { dispatch } from "./dispatch.js";
import type { InvalidateForUserRequest } from "../protocol/api/auth.js";
import { closeConnections, getConnections } from "./connections.js";
import type { DisconnectHandler } from "./disconnect.js";
import { DurableStorage } from "../storage/durable-storage.js";
import { getConnectedClients } from "../types/connected-clients.js";
import * as s from "superstruct";
import type { CreateRoomRequest } from "../protocol/api/room.js";
import { Router } from "itty-router";
import type { RociRequest, RociRouter } from "./middleware.js";
import { addRoutes } from "./room-do-routes.js";

const roomIDKey = "/system/roomID";
const deletedKey = "/system/deleted";

export interface RoomDOOptions<MD extends MutatorDefs> {
  mutators: MD;
  state: DurableObjectState;
  authApiKey: string | undefined;
  disconnectHandler: DisconnectHandler;
  logSink: LogSink;
  logLevel: LogLevel;
  allowUnconfirmedWrites: boolean;
}
export class BaseRoomDO<MD extends MutatorDefs> implements DurableObject {
  private readonly _clients: ClientMap = new Map();
  private readonly _lock = new Lock();
  private readonly _mutators: MutatorMap;
  private readonly _disconnectHandler: DisconnectHandler;
  private _lcHasRoomIdContext = false;
  private _lc: LogContext;
  private readonly _storage: DurableStorage;
  private readonly _authApiKey: string | undefined;
  private _turnTimerID: ReturnType<typeof setInterval> | 0 = 0;
  private readonly _turnDuration: number;
  private _router: RociRouter;

  constructor(options: RoomDOOptions<MD>) {
    const {
      mutators,
      disconnectHandler,
      state,
      authApiKey,
      logSink,
      logLevel,
    } = options;

    this._mutators = new Map([...Object.entries(mutators)]) as MutatorMap;
    this._disconnectHandler = disconnectHandler;
    this._storage = new DurableStorage(
      state.storage,
      options.allowUnconfirmedWrites
    );

    this._router = Router();
    addRoutes(this._router, this, authApiKey);
    this._turnDuration = 1000 / (options.allowUnconfirmedWrites ? 60 : 15);
    this._authApiKey = authApiKey;
    this._lc = new LogContext(logLevel, logSink)
      .addContext("RoomDO")
      .addContext("doID", state.id.toString());
    this._lc.info?.("Starting server");
    this._lc.info?.("Version:", version);
  }

  async fetch(request: Request): Promise<Response> {
    try {
      if (await this.deleted()) {
        return new Response("deleted", { status: 410 /* Gone */ });
      }
      if (!this._lcHasRoomIdContext) {
        const roomID = await this.maybeRoomID();
        const url = new URL(request.url);
        const urlRoomID = url.searchParams.get("roomID");
        if (
          // roomID is not going to be set on the createRoom request, or after
          // the room has been deleted.
          roomID !== undefined &&
          // roomID is not going to be set for all calls, eg to delete the room.
          urlRoomID !== null &&
          urlRoomID !== roomID
        ) {
          console.log("roomID mismatch", roomID, urlRoomID);
          this._lc.error?.(
            "roomID mismatch",
            "urlRoomID",
            urlRoomID,
            "roomID",
            roomID
          );
          return new Response("Unexpected roomID", { status: 400 });
        }

        if (roomID) {
          this._lc = this._lc.addContext("roomID", roomID);
          this._lcHasRoomIdContext = true;
        }
      }
      const response = await this._router.handle(request);
      if (response !== undefined) {
        return response;
      }
      return await dispatch(
        request,
        this._lc.addContext("req", randomID()),
        this._authApiKey,
        this
      );
    } catch (e) {
      this._lc.error?.("Unhandled exception in fetch", e);
      return new Response(
        e instanceof Error ? e.message : "Unexpected error.",
        {
          status: 500,
        }
      );
    }
  }

  private async _setRoomID(roomID: string) {
    return this._storage.put(roomIDKey, roomID);
  }

  async maybeRoomID(): Promise<string | undefined> {
    return this._storage.get(roomIDKey, s.string());
  }

  private async _setDeleted() {
    return this._storage.put(deletedKey, true);
  }

  async deleted(): Promise<boolean> {
    return (await this._storage.get(deletedKey, s.boolean())) === true;
  }

  // roomID errors and returns "unknown" if the roomID is not set. Prefer
  // roomID() to maybeRoomID() in cases where the roomID is expected to be set,
  // which is most cases.
  async roomID(): Promise<string> {
    const roomID = await this.maybeRoomID();
    if (roomID !== undefined) {
      return roomID;
    }
    this._lc.error?.("roomID is not set");
    return Promise.resolve("unknown");
  }

  // A more appropriate name might be init(), but this is easy since authDO and
  // roomDO share dispatch and handlers.
  async createRoom(
    _lc: LogContext,
    _request: Request,
    createRoomRequest: CreateRoomRequest
  ) {
    const { roomID } = createRoomRequest;
    await this._setRoomID(roomID);
    return new Response("ok");
  }

  // There's a bit of a question here about whether we really want to detele *all* the
  // data when a room is deleted. This deletes everything, including values kept by the
  // system e.g. the roomID. If we store more system keys in the future we might want to have
  // delete room only delete the room user data and not the system keys, because once
  // system keys are deleted who knows what behavior the room will have when its apis are
  // called. Maybe it's fine if they error out, dunno.
  async deleteAllData() {
    // Maybe we should validate that the roomID in the request matches?
    this._lc.info?.("delete all data");
    await this._storage.deleteAll();
    this._lc.info?.("done deleting all data");
    await this._setDeleted();
    return new Response("ok");
  }

  async connect(lc: LogContext, request: Request): Promise<Response> {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("expected websocket", { status: 400 });
    }
    const pair = new WebSocketPair();
    const ws = pair[1];
    const url = new URL(request.url);
    lc.debug?.("connection request", url.toString(), "waiting for lock");
    ws.accept();

    void this._lock.withLock(async () => {
      lc.debug?.("received lock");
      await handleConnection(
        lc,
        ws,
        this._storage,
        url,
        request.headers,
        this._clients,
        this._handleMessage,
        this._handleClose
      );
    });
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  async authInvalidateForUser(
    lc: LogContext,
    _request: RociRequest,
    { userID }: InvalidateForUserRequest
  ): Promise<Response> {
    lc.debug?.(
      `Closing user ${userID}'s connections fulfilling auth api invalidateForUser request.`
    );
    await this._closeConnections(
      (clientState) => clientState.userData.userID === userID
    );
    return new Response("Success", { status: 200 });
  }

  async authInvalidateForRoom(
    lc: LogContext
    // Ideally we'd ensure body.roomID matches this DO's roomID but we
    // don't know this DO's roomID...
    // { roomID }: InvalidateForRoom
  ): Promise<Response> {
    lc.info?.(
      "Closing all connections fulfilling auth api invalidateForRoom request."
    );
    await this._closeConnections((_) => true);
    return new Response("Success", { status: 200 });
  }

  async authInvalidateAll(lc: LogContext): Promise<Response> {
    lc.info?.(
      "Closing all connections fulfilling auth api invalidateAll request."
    );
    await this._closeConnections((_) => true);
    return new Response("Success", { status: 200 });
  }

  async authConnections(): Promise<Response> {
    // Note this intentionally does not acquire this._lock, as it is
    // unnecessary and can add latency.
    return new Response(JSON.stringify(getConnections(this._clients)));
  }

  private _closeConnections(
    predicate: (clientState: ClientState) => boolean
  ): Promise<void> {
    return this._lock.withLock(() =>
      closeConnections(this._clients, predicate)
    );
  }

  private _handleMessage = async (
    clientID: ClientID,
    data: string,
    ws: Socket
  ): Promise<void> => {
    const lc = this._lc
      .addContext("msg", randomID())
      .addContext("client", clientID);
    lc.debug?.("handling message", data, "waiting for lock");

    try {
      await this._lock.withLock(async () => {
        lc.debug?.("received lock");
        handleMessage(lc, this._clients, clientID, data, ws, () =>
          this._processUntilDone()
        );
      });
    } catch (e) {
      this._lc.error?.("Unhandled exception in _handleMessage", e);
    }
  };

  private async _processUntilDone() {
    const lc = this._lc.addContext("req", randomID());
    lc.debug?.("handling processUntilDone");
    if (this._turnTimerID) {
      lc.debug?.("already processing, nothing to do");
      return;
    }
    this._turnTimerID = setInterval(() => {
      void this._processNext(lc);
    }, this._turnDuration);
  }

  private async _processNext(lc: LogContext) {
    lc.debug?.(
      `processNext - starting turn at ${Date.now()} - waiting for lock`
    );
    await this._lock.withLock(async () => {
      lc.debug?.(`received lock at ${Date.now()}`);

      const storedConnectedClients = await getConnectedClients(this._storage);
      let hasDisconnectsToProcess = false;
      for (const clientID of storedConnectedClients) {
        if (!this._clients.has(clientID)) {
          hasDisconnectsToProcess = true;
          break;
        }
      }
      if (!hasPendingMutations(this._clients) && !hasDisconnectsToProcess) {
        lc.debug?.("No pending mutations or disconnects to process, exiting");
        if (this._turnTimerID) {
          clearInterval(this._turnTimerID);
          this._turnTimerID = 0;
        }
        return;
      }

      await processPending(
        lc,
        this._storage,
        this._clients,
        this._mutators,
        this._disconnectHandler,
        Date.now()
      );
    });
  }

  private _handleClose = async (
    clientID: ClientID,
    ws: Socket
  ): Promise<void> => {
    const lc = this._lc
      .addContext("req", randomID())
      .addContext("client", clientID);
    lc.debug?.("handling close - waiting for lock");
    await this._lock.withLock(async () => {
      lc.debug?.("received lock");
      handleClose(lc, this._clients, clientID, ws);
      await this._processUntilDone();
    });
  };
}

function hasPendingMutations(clients: ClientMap) {
  for (const clientState of clients.values()) {
    if (clientState.pending.length > 0) {
      return true;
    }
  }
  return false;
}
