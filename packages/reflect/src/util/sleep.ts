export function sleep(ms = 0, setTimeout = globalThis.setTimeout) {
  return new Promise<void>(resolve => setTimeout(resolve, ms));
}
