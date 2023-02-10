import type {Downstream} from '../protocol/down.js';
import type {Socket} from '../types/client-state.js';

export function sendError(ws: Socket, body: string) {
  const message: Downstream = ['error', body];
  ws.send(JSON.stringify(message));
}

// This function is a seam we can use to inject a mock socket pair
// for testing.
export function newWebSocketPair() {
  return new WebSocketPair();
}
