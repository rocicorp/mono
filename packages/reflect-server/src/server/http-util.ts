import type {LogContext} from '@rocicorp/logger';

export function isWebsocketUpgrade(request: Request): boolean {
  return request.headers.get('Upgrade') === 'websocket';
}

export function okResponse() {
  return new Response('ok');
}

export function requireUpgradeHeader(
  request: Request,
  lc: LogContext,
): Response | null {
  if (!isWebsocketUpgrade(request)) {
    lc.error?.('missing Upgrade header for', request.url);
    return new Response('expected websocket', {status: 400});
  }
  return null;
}

export function upgradeWebsocketResponse(
  ws: WebSocket,
  requestHeaders: Headers,
) {
  // Need to forward the Sec-WebSocket-Protocol header
  const responseHeaders = new Headers();
  const protocol = requestHeaders.get('Sec-WebSocket-Protocol');
  if (protocol) {
    responseHeaders.set('Sec-WebSocket-Protocol', protocol);
  }
  return new Response(null, {
    status: 101,
    webSocket: ws,
    headers: responseHeaders,
  });
}

export function roomNotFoundResponse() {
  return new Response('room not found', {status: 404});
}
