// Helpers for some objects from the browser environment. These are wrapped in
// functions because Replicache runs in environments that do not have these
// objects (such as Web Workers, Deno etc).

type GlobalThis = typeof globalThis;

const overrides = new Map<keyof GlobalThis, GlobalThis[keyof GlobalThis]>();

export function overrideBrowserGlobal<T extends keyof GlobalThis>(
  name: T,
  value: GlobalThis[T],
) {
  overrides.set(name, value);
}

export function clearBrowserOverrides() {
  overrides.clear();
}

export function getBrowserGlobal<T extends keyof GlobalThis>(
  name: T,
): GlobalThis[T] | undefined {
  if (overrides.has(name)) {
    return overrides.get(name);
  }
  return globalThis[name];
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
