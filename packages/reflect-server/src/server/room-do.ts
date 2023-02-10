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
      // This is ugly, but there is no way to get
      // the DO name (as opposed to the DO id), so instead
      // we get it from the first request carrying a roomID
      // param (the roomID is the DO name).
      if (!this._lcHasRoomIdContext) {
        const url = new URL(request.url);
        const roomID = url.searchParams.get("roomID");
        if (roomID) {
          this._lc = this._lc.addContext("roomID", roomID);
          this._lcHasRoomIdContext = true;
        }
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

  async connect(lc: LogContext, request: Request): Promise<Response> {
    if (request.headers.get("Upgrade") !== "websocket") {
      lc.error?.("roomDO: missing Upgrade header");
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
    _request: Request,
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
