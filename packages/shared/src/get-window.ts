/**
 * Returns the window object. This is wrapped in a function because Replicache
 * runs in environments that do not have a window (such as Web Workers, Deno
 * etc)
 */
export function getWindow(): Window | undefined {
  return typeof window !== 'undefined' ? window : undefined;
}
