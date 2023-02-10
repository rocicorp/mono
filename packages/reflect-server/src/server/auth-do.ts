import { encodeHeaderValue } from "../util/headers.js";
import { randomID } from "../util/rand.js";
import { LogSink, LogContext, LogLevel } from "@rocicorp/logger";
import { version } from "../util/version.js";
import { AuthHandler, UserData, USER_DATA_HEADER_NAME } from "./auth.js";
import { dispatch, paths } from "./dispatch.js";
import { RWLock } from "@rocicorp/lock";
import {
  ConnectionsResponse,
  connectionsResponseSchema,
  InvalidateForRoomRequest,
  InvalidateForUserRequest,
} from "../protocol/api/auth.js";
import * as s from "superstruct";
import { createAuthAPIHeaders } from "./auth-api-headers.js";
import { newWebSocketPair, sendError } from "../util/socket.js";

export interface AuthDOOptions {
  roomDO: DurableObjectNamespace;
  state: DurableObjectState;
  authHandler: AuthHandler;
  authApiKey: string | undefined;
  logSink: LogSink;
  logLevel: LogLevel;
  // newWebSocketPair is a seam we use for testing. I cannot figure out
  // how to get jest to mock a module.
  newWebSocketPair?: typeof newWebSocketPair;
}
export type ConnectionKey = {
  userID: string;
  roomID: string;
  clientID: string;
};
export type ConnectionRecord = {
  connectTimestamp: number;
};

export class BaseAuthDO implements DurableObject {
  private readonly _roomDO: DurableObjectNamespace;
  private readonly _state: DurableObjectState;
  private readonly _authHandler: AuthHandler;
  private readonly _authApiKey?: string;
  private readonly _lc: LogContext;
  private readonly _lock: RWLock;
  private readonly _newWebSocketPair: typeof newWebSocketPair;

  constructor(options: AuthDOOptions) {
    const { roomDO, state, authHandler, authApiKey, logSink, logLevel } =
      options;
    this._newWebSocketPair = options.newWebSocketPair || newWebSocketPair;
    this._roomDO = roomDO;
    this._state = state;
    this._authHandler = authHandler;
    this._authApiKey = authApiKey;
    this._lc = new LogContext(logLevel, logSink)
      .addContext("AuthDO")
      .addContext("doID", state.id.toString());
    this._lock = new RWLock();
    this._lc.info?.("Starting server");
    this._lc.info?.("Version:", version);
  }

  async fetch(request: Request): Promise<Response> {
    // Match route against pattern /:name/*action
    const lc = this._lc.addContext("req", randomID());
    lc.debug?.("Handling request:", request.url);
    try {
      const resp = await dispatch(request, lc, this._authApiKey, this);
      lc.debug?.(`Returning response: ${resp.status} ${resp.statusText}`);
      return resp;
    } catch (e) {
      lc.error?.("Unhandled exception in fetch", e);
      return new Response(
        e instanceof Error ? e.message : "Unexpected error.",
        {
          status: 500,
        }
      );
    }
  }

  async connect(lc: LogContext, request: Request): Promise<Response> {
    lc.info?.("authDO received websocket connection request:", request.url);

    const url = new URL(request.url);
    if (url.pathname !== "/connect") {
      lc.error?.("authDO returning 400 bc path is not /connect:", request.url);
      return new Response("unknown route", {
        status: 400,
      });
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      lc.error?.(
        "authDO returning 400 bc missing Upgrade header:",
        request.url
      );
      return new Response("expected websocket", { status: 400 });
    }

    const encodedAuth = request.headers.get("Sec-WebSocket-Protocol");
    if (!encodedAuth) {
      lc.error?.("authDO auth not found in Sec-WebSocket-Protocol header.");
      return createUnauthorizedResponse("auth required");
    }

    // From this point forward we want to return errors over the
    // websocket so the client can see them. This is a bit dodgy
    // since adversaries who send unauthorized or bad requests
    // cause us to allocate websockets. But we don't have an
    // alternative to piping errors down to the client at the
    // moment.
    //
    // TODO consider using socket close codes in the 4xxx range
    //   for the signaling instead of messages.
    // TODO should probably unify the way this works with how
    //   roomDO connect() does it.

    const closeWithError = (error: string) => {
      const pair = this._newWebSocketPair();
      const ws = pair[1];
      lc.error?.("accepting connection to send error", url.toString());
      ws.accept();

      lc.error?.("invalid connection request", error);
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
      responseHeaders.set("Sec-WebSocket-Protocol", encodedAuth);
      return new Response(null, {
        status: 101,
        headers: responseHeaders,
        webSocket: pair[0],
      });
    };

    // TODO apparently many of these checks are not tested :(
    const roomID = url.searchParams.get("roomID");
    if (roomID === null || roomID === "") {
      return closeWithError("400: roomID parameter required");
    }

    const clientID = url.searchParams.get("clientID");
    if (!clientID) {
      return closeWithError("400: clientID parameter required");
    }

    lc = lc.addContext("client", clientID).addContext("room", roomID);

    let decodedAuth: string | undefined;
    try {
      decodedAuth = decodeURIComponent(encodedAuth);
    } catch (e) {
      return closeWithError("400: malformed auth");
    }
    const auth = decodedAuth;
    return this._lock.withRead(async () => {
      let userData: UserData | undefined;
      try {
        userData = await this._authHandler(auth, roomID);
      } catch (e) {
        return closeWithError("401: authHandler rejected");
      }
      if (!userData || !userData.userID) {
        if (!userData) {
          lc.info?.("userData returned by authHandler is falsey.");
        } else if (!userData.userID) {
          lc.info?.("userData returned by authHandler has no userID.");
        }
        return closeWithError("401: no userData");
      }

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

      // Forward the request to the Room Durable Object for roomID...
      const id = this._roomDO.idFromName(roomID);
      const stub = this._roomDO.get(id);
      const requestToDO = new Request(request);
      requestToDO.headers.set(
        USER_DATA_HEADER_NAME,
        encodeHeaderValue(JSON.stringify(userData))
      );
      const responseFromDO = await stub.fetch(requestToDO);
      const responseHeaders = new Headers(responseFromDO.headers);
      // While Sec-WebSocket-Protocol is just being used as a mechanism for
      // sending `auth` since custom headers are not supported by the browser
      // WebSocket API, the Sec-WebSocket-Protocol semantics must be followed.
      // Send a Sec-WebSocket-Protocol response header with a value
      // matching the Sec-WebSocket-Protocol request header, to indicate
      // support for the protocol, otherwise the client will close the connection.
      responseHeaders.set("Sec-WebSocket-Protocol", encodedAuth);

      const response = new Response(responseFromDO.body, {
        status: responseFromDO.status,
        statusText: responseFromDO.statusText,
        webSocket: responseFromDO.webSocket,
        headers: responseHeaders,
      });
      return response;
    });
  }

  async authInvalidateForUser(
    lc: LogContext,
    request: Request,
    { userID }: InvalidateForUserRequest
  ): Promise<Response> {
    lc.debug?.(`authInvalidateForUser ${userID} waiting for lock.`);
    return this._lock.withWrite(async () => {
      lc.debug?.("got lock.");
      const connectionKeys = (
        await this._state.storage.list({
          prefix: getConnectionKeyStringUserPrefix(userID),
        })
      ).keys();
      // The requests to the Room DOs must be completed inside the write lock
      // to avoid races with new connect requests for this user.
      return this._forwardInvalidateRequest(
        lc,
        "authInvalidateForUser",
        request,
        [...connectionKeys]
      );
    });
  }

  async authInvalidateForRoom(
    lc: LogContext,
    request: Request,
    { roomID }: InvalidateForRoomRequest
  ): Promise<Response> {
    lc.debug?.(`authInvalidateForRoom ${roomID} waiting for lock.`);
    return this._lock.withWrite(async () => {
      lc.debug?.("got lock.");
      lc.debug?.(`Sending authInvalidateForRoom request to ${roomID}`);
      // The request to the Room DO must be completed inside the write lock
      // to avoid races with connect requests for this room.
      const id = this._roomDO.idFromName(roomID);
      const stub = this._roomDO.get(id);
      const response = await stub.fetch(request);
      if (!response.ok) {
        lc.debug?.(
          `Received error response from ${roomID}. ${
            response.status
          } ${await response.text}`
        );
      }
      return response;
    });
  }

  async authInvalidateAll(lc: LogContext, request: Request): Promise<Response> {
    lc.debug?.(`authInvalidateAll waiting for lock.`);
    return this._lock.withWrite(async () => {
      lc.debug?.("got lock.");
      const connectionKeys = (
        await this._state.storage.list({
          prefix: CONNECTION_KEY_PREFIX,
        })
      ).keys();
      // The request to the Room DOs must be completed inside the write lock
      // to avoid races with connect requests.
      return this._forwardInvalidateRequest(lc, "authInvalidateAll", request, [
        ...connectionKeys,
      ]);
    });
  }

  async authRevalidateConnections(lc: LogContext): Promise<Response> {
    lc.info?.(`Starting auth revalidation.`);
    const authApiKey = this._authApiKey;
    if (authApiKey === undefined) {
      lc.info?.(
        "Returning Unauthorized because REFLECT_AUTH_API_KEY is not defined in env."
      );
      return new Response("Unauthorized", {
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
        lc.error?.("Failed to parse connection key", keyString);
        continue;
      }
      const { roomID } = connectionKey;
      let keyStringSet = connectionKeyStringsByRoomID.get(roomID);
      if (!keyStringSet) {
        keyStringSet = new Set();
        connectionKeyStringsByRoomID.set(roomID, keyStringSet);
      }
      keyStringSet.add(keyString);
    }
    lc.info?.(
      `Revalidating ${connectionRecords.size} ConnectionRecords across ${connectionKeyStringsByRoomID.size} rooms.`
    );
    let deleteCount = 0;
    for (const [
      roomID,
      connectionKeyStringsForRoomID,
    ] of connectionKeyStringsByRoomID) {
      lc.debug?.(`revalidating connections for ${roomID} waiting for lock.`);
      await this._lock.withWrite(async () => {
        lc.debug?.("got lock.");
        const id = this._roomDO.idFromName(roomID);
        const stub = this._roomDO.get(id);
        const response = await stub.fetch(
          new Request(
            `https://unused-reflect-room-do.dev${paths.authConnections}`,
            {
              headers: createAuthAPIHeaders(authApiKey),
            }
          )
        );
        let connectionsResponse: ConnectionsResponse | undefined;
        try {
          const responseJSON = await response.json();
          s.assert(responseJSON, connectionsResponseSchema);
          connectionsResponse = responseJSON;
        } catch (e) {
          lc.error?.(`Bad ${paths.authConnections} response from roomDO`, e);
        }
        if (connectionsResponse) {
          const openConnectionKeyStrings = new Set(
            connectionsResponse.map(({ userID, clientID }) =>
              connectionKeyToString({
                roomID,
                userID,
                clientID,
              })
            )
          );
          const keysToDelete: string[] = [
            ...connectionKeyStringsForRoomID,
          ].filter((keyString) => !openConnectionKeyStrings.has(keyString));
          try {
            deleteCount += await this._state.storage.delete(keysToDelete);
          } catch (e) {
            lc.info?.("Failed to delete connections for roomID", roomID);
          }
        }
      });
    }
    lc.info?.(
      `Revalidated ${connectionRecords.size} ConnectionRecords, deleted ${deleteCount} ConnectionRecords.`
    );
    return new Response("Complete", { status: 200 });
  }

  private async _forwardInvalidateRequest(
    lc: LogContext,
    invalidateRequestName: string,
    request: Request,
    connectionKeyStrings: string[]
  ): Promise<Response> {
    const connectionKeys = connectionKeyStrings.map((keyString) => {
      const connectionKey = connectionKeyFromString(keyString);
      if (!connectionKey) {
        lc.error?.("Failed to parse connection key", keyString);
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
    const responsePromises = [];
    lc.debug?.(
      `Sending ${invalidateRequestName} requests to ${roomIDs.length} rooms`
    );
    // Send requests to room DOs in parallel
    for (const roomID of roomIDs) {
      const id = this._roomDO.idFromName(roomID);
      const stub = this._roomDO.get(id);
      responsePromises.push(stub.fetch(request));
    }
    const errorResponses = [];
    for (let i = 0; i < responsePromises.length; i++) {
      const response = await responsePromises[i];
      if (!response.ok) {
        errorResponses.push(response);
        lc.debug?.(
          `Received error response from ${roomIDs[i]}. ${
            response.status
          } ${await response.text}`
        );
      }
    }
    if (errorResponses.length === 0) {
      return new Response("Success", {
        status: 200,
      });
    }
    return errorResponses[0];
  }
}

const CONNECTION_KEY_PREFIX = "connection/";

function connectionKeyToString(key: ConnectionKey): string {
  return `${CONNECTION_KEY_PREFIX}${encodeURIComponent(
    key.userID
  )}/${encodeURIComponent(key.roomID)}/${encodeURIComponent(key.clientID)}/`;
}

function getConnectionKeyStringUserPrefix(userID: string): string {
  return `${CONNECTION_KEY_PREFIX}${encodeURIComponent(userID)}/`;
}

export function connectionKeyFromString(
  key: string
): ConnectionKey | undefined {
  if (!key.startsWith(CONNECTION_KEY_PREFIX)) {
    return undefined;
  }
  const parts = key.split("/");
  if (parts.length !== 5 || parts[4] !== "") {
    return undefined;
  }
  return {
    userID: decodeURIComponent(parts[1]),
    roomID: decodeURIComponent(parts[2]),
    clientID: decodeURIComponent(parts[3]),
  };
}

function createUnauthorizedResponse(message = "Unauthorized"): Response {
  return new Response(message, {
    status: 401,
  });
}
