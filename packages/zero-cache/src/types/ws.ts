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
  let gotLivenessSignal = true;

  let livenessTimer: NodeJS.Timeout | undefined;
  function startHeartBeats() {
    livenessTimer = setInterval(() => {
      if (!gotLivenessSignal) {
        lc.warn?.(
          `socket@${ws.url} did not respond to heartbeat. Terminating...`,
        );
        ws.terminate();
        return;
      }
      // Reset gotLivenessSignal and expect another pong or message to arrive
      // before the next interval elapses.
      gotLivenessSignal = false;
      ws.ping();
    }, intervalMs);
  }

  if (ws.readyState === ws.CONNECTING) {
    ws.once('open', () => startHeartBeats());
  } else if (ws.readyState === ws.OPEN) {
    startHeartBeats();
  }

  // Both pongs and messages are accepted as signs of liveness.
  // Checking for pongs only risks false positives as pongs may be backed
  // up behind a large stream of messages.
  const signalAlive = () => (gotLivenessSignal = true);
  ws.on('pong', signalAlive);
  ws.on('message', signalAlive);
  ws.once('close', () => clearInterval(livenessTimer));
}

export function expectPingsForLiveness(
  lc: LogContext,
  ws: WebSocket,
  intervalMs: number,
  timeoutBufferMs = 3_000,
) {
  let gotLivenessSignal = false;

  const livenessTimer = setInterval(() => {
    if (!gotLivenessSignal) {
      lc.warn?.(
        `socket@${ws.url} did not send heartbeat or messages. Terminating...`,
      );
      ws.terminate();
      return;
    }
    // Reset gotLivenessSignal and expect another ping or message to arrive
    // before the next interval elapses.
    gotLivenessSignal = false;
  }, intervalMs + timeoutBufferMs);

  // Both pings and messages are accepted as signs of liveness.
  // Checking for pings only risks false positives as pings may be backed
  // up behind a large stream of messages.
  const signalAlive = () => (gotLivenessSignal = true);
  ws.on('ping', signalAlive);
  ws.on('message', signalAlive);
  ws.once('close', () => clearTimeout(livenessTimer));
}
