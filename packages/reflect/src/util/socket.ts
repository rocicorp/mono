import type {Upstream} from 'protocol';

export function send(ws: WebSocket, data: Upstream) {
  ws.send(JSON.stringify(data));
}
