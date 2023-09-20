import type {LogContext} from '@rocicorp/logger';

const tailWebSockets = new Set<WebSocket>();

export function connectTail(ws: WebSocket, id: string, lc: LogContext) {
  ws.addEventListener('close', () => disconnectTail(ws), {once: true});
  tailWebSockets.add(ws);
  if (tailWebSockets.size === 1) {
    installConsoleLogHooks(lc, id);
  }
}

function disconnectTail(ws: WebSocket) {
  tailWebSockets.delete(ws);
  if (tailWebSockets.size === 0) {
    uninstallConsoleLogHooks();
  }
}

// From @types/cloudflare/workers-types
interface Console {
  debug(...data: unknown[]): void;
  error(...data: unknown[]): void;
  info(...data: unknown[]): void;
  log(...data: unknown[]): void;
  warn(...data: unknown[]): void;
}

const originalConsole: Console = console;

type LogRecord = {
  message: unknown; // Really JSON but we will JSON stringify this soon...
  level: string;
  timestamp: number;
};

const methods = ['debug', 'error', 'info', 'log', 'warn'] as const;

const tailConsole = {} as Console;
for (const method of methods) {
  tailConsole[method] = (...args: unknown[]) => {
    const logRecord: LogRecord = {
      message: args,
      level: method,
      timestamp: Date.now(),
    };
    const msg = JSON.stringify({logs: [logRecord]});

    for (const ws of tailWebSockets) {
      ws.send(msg);
    }
  };
}

function installConsoleLogHooks(lc: LogContext, id: string) {
  lc.debug?.('installConsoleLogHooks', id);
  (globalThis as unknown as {console: Console}).console = tailConsole;
}

function uninstallConsoleLogHooks() {
  (globalThis as unknown as {console: Console}).console = originalConsole;
}
