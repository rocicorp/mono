// Helpers for some objects from the browser environment. These are wrapped in
// functions because Replicache runs in environments that do not have these
// objects (such as Web Workers, Deno etc).

export function getDocument(): Document | undefined {
  return typeof document !== 'undefined' ? document : undefined;
}

export function getLocation(): Location | undefined {
  return typeof location !== 'undefined' ? location : undefined;
}
