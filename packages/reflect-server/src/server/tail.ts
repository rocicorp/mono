import {originalConsole, setConsole, type Console} from './console.js';

const tailWebSockets = new Set<WebSocket>();

export function connectTail(ws: WebSocket) {
  ws.addEventListener('close', () => disconnectTail(ws), {once: true});
  tailWebSockets.add(ws);
}

function disconnectTail(ws: WebSocket) {
  tailWebSockets.delete(ws);
}

type LogRecord = {
  message: unknown; // Really JSON but we will JSON stringify this soon...
  level: string;
  timestamp: number;
};

type Level = 'debug' | 'error' | 'info' | 'log' | 'warn';

class TailConsole implements Console {
  debug(...data: unknown[]): void {
    this.#log('debug', data);
  }

  error(...data: unknown[]): void {
    this.#log('error', data);
  }

  info(...data: unknown[]): void {
    this.#log('info', data);
  }

  log(...data: unknown[]): void {
    this.#log('log', data);
  }

  warn(...data: unknown[]): void {
    this.#log('warn', data);
  }

  #log(level: Level, message: unknown[]) {
    if (tailWebSockets.size === 0) {
      originalConsole[level](...message);
    } else {
      const logRecord: LogRecord = {
        message,
        level,
        timestamp: Date.now(),
      };
      const msg = JSON.stringify({logs: [logRecord]});

      for (const ws of tailWebSockets) {
        ws.send(msg);
      }
    }
  }
}

// Override the global console with our own ahead of time so that references to
// console in the code will use our console.
setConsole(new TailConsole());
