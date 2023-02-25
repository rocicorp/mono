import type {Upstream} from 'reflect-protocol';

export function send(ws: WebSocket, data: Upstream) {
  ws.send(JSON.stringify(data));
}
