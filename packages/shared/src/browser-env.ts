// Helpers for some objects from the browser environment. These are wrapped in
// functions because Replicache runs in environments that do not have these
// objects (such as Web Workers, Deno etc).

type GlobalThis = typeof globalThis;

export function getBrowserGlobal<T extends keyof GlobalThis>(
  name: T,
): GlobalThis[T] | undefined {
  return globalThis[name];
}

/**
 * Returns the global method with the given name, bound to the global object.
 * This is important because some methods (e.g. `requestAnimationFrame`) are not
 * bound to the global object by default.
 *
 * If you end up using {@linkcode getBrowserGlobal} instead in a case like this:
 *
 * ```js
 * this.#raf = getBrowserGlobal('requestAnimationFrame') ?? rafFallback;
 * ...
 * this.#raf(() => ...);
 * ```
 *
 * You will end up with `Uncaught TypeError: Illegal invocation` because `this`
 * is not bound to the global object
 */
export function getBrowserGlobalMethod<T extends keyof GlobalThis>(
  name: T,
): GlobalThis[T] | undefined {
  return globalThis[name]?.bind(globalThis);
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
