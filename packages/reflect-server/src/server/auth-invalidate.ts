import type { ClientMap } from "../types/client-state.js";
import type { LogContext } from "@rocicorp/logger";

export function handleAuthInvalidate(
  clients: ClientMap,
  lc: LogContext,
  userID?: string
): Response {
  let closedCount = 0;
  for (const clientState of clients.values()) {
    if (userID === undefined || userID === clientState.userData.userID) {
      clientState.socket.send(JSON.stringify(["error", "Auth invalidated."]));
      clientState.socket.close();
      closedCount++;
    }
  }
  lc.debug?.("Closed", closedCount, "connections.");
  return new Response("Success", { status: 200 });
}
