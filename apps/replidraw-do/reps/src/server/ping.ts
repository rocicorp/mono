import { Socket } from "../types/client-state";
import { PongMessage } from "../protocol/pong";
import { LogContext } from "../util/logger";

/**
 * handles the 'ping' upstream message by sending a pong!
 * @param ws socket connection to requesting client
 * @returns
 */
export function handlePing(lc: LogContext, ws: Socket) {
  lc.debug?.("handling ping");
  const pongMessage: PongMessage = ["pong", {}];
  ws.send(JSON.stringify(pongMessage));
}
