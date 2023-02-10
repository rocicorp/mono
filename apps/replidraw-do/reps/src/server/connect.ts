import { DurableStorage } from "../storage/durable-storage";
import {
  ClientRecord,
  clientRecordKey,
  clientRecordSchema,
} from "../types/client-record";
import {
  ClientID,
  ClientMap,
  ClientState,
  Socket,
} from "../types/client-state";
import { LogContext } from "../util/logger";
import { ConnectedMessage } from "../protocol/connected";

export type MessageHandler = (
  clientID: ClientID,
  data: string,
  ws: Socket
) => void;

export type CloseHandler = (clientID: ClientID) => void;

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
  durable: DurableObjectStorage,
  url: URL,
  clients: ClientMap,
  onMessage: MessageHandler,
  onClose: CloseHandler
) {
  const { result, error } = getConnectRequest(url);
  if (result === null) {
    lc.info?.("invalid connection request", error);
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    ws.send(error!);
    ws.close();
    return;
  }

  lc = lc.addContext("client", result.clientID);
  lc.debug?.("parsed request", result);

  const { clientID, baseCookie } = result;
  const storage = new DurableStorage(durable);
  const existingRecord = await storage.get(
    clientRecordKey(clientID),
    clientRecordSchema
  );
  lc.debug?.("Existing client record", existingRecord);
  const lastMutationID = existingRecord?.lastMutationID ?? 0;
  const record: ClientRecord = {
    baseCookie,
    lastMutationID,
  };
  await storage.put(clientRecordKey(clientID), record);
  lc.debug?.("Put client record", record);

  // Add or update ClientState.
  const existing = clients.get(clientID);
  if (existing) {
    lc.debug?.("Closing old socket");
    existing.socket.close();
  }

  ws.addEventListener("message", (event) =>
    onMessage(clientID, event.data.toString(), ws)
  );
  ws.addEventListener("close", () => onClose(clientID));

  const client: ClientState = {
    socket: ws,
    clockBehindByMs: undefined,
    pending: [],
  };
  clients.set(clientID, client);

  const connectedMessage: ConnectedMessage = ["connected", {}];
  ws.send(JSON.stringify(connectedMessage));
}

export function getConnectRequest(url: URL) {
  const getParam = (name: string, required: boolean) => {
    const value = url.searchParams.get(name);
    if (value === "" || value === null) {
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
        `invalid querystring parameter ${name}, url: ${url}, got: ${value}`
      );
    }
    return int;
  };

  try {
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const clientID = getParam("clientID", true)!;
    const baseCookie = getIntegerParam("baseCookie", false);
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const timestamp = getIntegerParam("ts", true)!;

    return {
      result: {
        clientID,
        baseCookie,
        timestamp,
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
