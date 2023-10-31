import type {TailMessage} from 'reflect-protocol/src/tail.js';
import {originalConsole, setConsole, type Console} from './console.js';

const tailWebSockets = new Set<WebSocket>();

export function connectTail(ws: WebSocket) {
  ws.addEventListener('close', () => disconnectTail(ws), {once: true});
  ws.send(JSON.stringify({type: 'connected'} satisfies TailMessage));
  tailWebSockets.add(ws);
}

type Level = keyof Console;

function disconnectTail(ws: WebSocket) {
  tailWebSockets.delete(ws);
}

class TailConsole implements Console {
  declare debug: (...data: unknown[]) => void;
  declare error: (...data: unknown[]) => void;
  declare info: (...data: unknown[]) => void;
  declare log: (...data: unknown[]) => void;
  declare warn: (...data: unknown[]) => void;
}

function log(level: Level, message: unknown[]) {
  if (tailWebSockets.size === 0) {
    originalConsole[level](...message);
  } else {
    try {
      const msg = JSON.stringify({
        type: 'log',
        level,
        message,
      } satisfies TailMessage);

      for (const ws of tailWebSockets) {
        ws.send(msg);
      }
    } catch (err) {
      originalConsole.error('Failed to send msg', err);
    }
  }
}

for (const level of ['debug', 'error', 'info', 'log', 'warn'] as const) {
  TailConsole.prototype[level] = function (...data: unknown[]) {
    log(level, data);
  };
}

// Override the global console with our own ahead of time so that references to
// console in the code will use our console.
setConsole(new TailConsole());
