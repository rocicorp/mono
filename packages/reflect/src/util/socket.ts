import type {Downstream} from '../protocol/down.js';

export function sendError(ws: WebSocket, body: string) {
  const message: Downstream = ['error', body];
  ws.send(JSON.stringify(message));
}
