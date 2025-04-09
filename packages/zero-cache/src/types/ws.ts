import type {LogContext} from '@rocicorp/logger';
import type {WebSocket} from 'ws';
import {elide} from './strings.ts';

// https://github.com/Luka967/websocket-close-codes
export const PROTOCOL_ERROR = 1002;
export const INTERNAL_ERROR = 1011;

export type ErrorCode = typeof PROTOCOL_ERROR | typeof INTERNAL_ERROR;

export function closeWithError(
  lc: LogContext,
  ws: WebSocket,
  err: unknown,
  code: ErrorCode = INTERNAL_ERROR,
) {
  const endpoint = ws.url ?? 'client';
  const errMsg = String(err);
  lc.warn?.(`closing connection to ${endpoint} with error`, errMsg);

  // close messages must be less than or equal to 123 bytes:
  // https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close#reason
  ws.close(code, elide(errMsg, 123));
}

export function sendPingsForLiveness(
  lc: LogContext,
  ws: WebSocket,
  intervalMs: number,
) {
  let alive = true;
  ws.on('pong', () => (alive = true));

  let heartbeatTimer: NodeJS.Timeout | undefined;
  function startHeartBeats() {
    heartbeatTimer = setInterval(() => {
      if (!alive) {
        lc.warn?.(
          `socket@${ws.url} did not respond to heartbeat. Terminating...`,
        );
        ws.terminate();
        return;
      }

      alive = false;
      ws.ping();
    }, intervalMs);
  }

  if (ws.readyState === ws.CONNECTING) {
    ws.once('open', () => startHeartBeats());
  } else if (ws.readyState === ws.OPEN) {
    startHeartBeats();
  }

  ws.once('close', () => clearInterval(heartbeatTimer));
}

export function expectPingsForLiveness(
  lc: LogContext,
  ws: WebSocket,
  intervalMs: number,
  timeoutBufferMs = 3_000,
) {
  let missedPingTimer: NodeJS.Timeout | undefined;

  function expectNextPing() {
    clearTimeout(missedPingTimer);

    missedPingTimer = setTimeout(() => {
      lc.warn?.(`socket@${ws.url} did not send heartbeat. Terminating...`);
      ws.terminate();
    }, intervalMs + timeoutBufferMs);
  }

  ws.on('ping', expectNextPing);
  ws.once('close', () => clearTimeout(missedPingTimer));

  expectNextPing();
}
