// Helpers for some objects from the browser environment. These are wrapped in
// functions because Replicache runs in environments that do not have these
// objects (such as Web Workers, Deno etc).

type GlobalThis = typeof globalThis;

export function getBrowserGlobal<T extends keyof GlobalThis>(
  name: T,
): GlobalThis[T] | undefined {
  return (globalThis as unknown as GlobalThis)[name] as GlobalThis[T];
}

export function mustGetBrowserGlobal<T extends keyof GlobalThis>(
  name: T,
): GlobalThis[T] {
  const r = getBrowserGlobal(name);
  if (r === undefined) {
    throw new Error(
      `Unsupported JavaScript environment: Could not find ${name}.`,
    );
  }
  return r;
}
