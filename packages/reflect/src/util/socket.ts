import type {Upstream} from 'protocol/src/up.js';

export function send(ws: WebSocket, data: Upstream) {
  ws.send(JSON.stringify(data));
}
