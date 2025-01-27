import type {Upstream} from '../../../zero-protocol/src/up.ts';

export function send(ws: WebSocket, data: Upstream) {
  ws.send(JSON.stringify(data));
}
