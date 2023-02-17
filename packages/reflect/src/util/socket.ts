import type {Upstream} from '../protocol/up.js';

export function send(ws: WebSocket, data: Upstream) {
  ws.send(JSON.stringify(data));
}
