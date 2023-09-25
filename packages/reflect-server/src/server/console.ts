// From @types/cloudflare/workers-types
// Subset of Web and Node interfaces
export interface Console {
  debug(...data: unknown[]): void;
  error(...data: unknown[]): void;
  info(...data: unknown[]): void;
  log(...data: unknown[]): void;
  warn(...data: unknown[]): void;
}

export const originalConsole: Console = console;

export function setConsole(console: Console) {
  (globalThis as unknown as {console: Console}).console = console;
}
