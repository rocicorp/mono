/**
 * `drizzle-orm`'s `PgSession.prepareQuery` signature changed in the 1.0 RC
 * line:
 * - `<= 1.0.0-beta`: `(query, fields, name, isResponseInArrayMode, customResultMapper?, queryMetadata?, cacheConfig?)`
 * - `>= 1.0.0-rc.1`: `(query, mode: 'arrays' | 'objects' | 'raw', name, mapper?, queryMetadata?, cacheConfig?)`
 *
 * Passing the old positional form to a newer drizzle leaves `mode` undefined,
 * so it returns rows as arrays instead of objects. That silently breaks
 * `getServerSchema`'s `information_schema` introspection (it reads
 * `row.dataType`, which is `undefined` on an array row) and every transaction
 * fails with `Cannot read properties of undefined (reading 'toLowerCase')`.
 *
 * `zero` is built against `drizzle-orm@^0.45`, so the typed call uses the old
 * signature; this detects a newer drizzle at runtime and calls the new one.
 *
 * Rather than reading `drizzle-orm`'s version from `package.json`, we detect
 * the signature empirically from `prepareQuery` itself.
 */

let usesModeArg: boolean | undefined;

export function drizzleUsesModeArg(
  prepareQuery: (...args: never[]) => unknown,
): boolean {
  if (usesModeArg === undefined) {
    usesModeArg = detectUsesModeArg(prepareQuery);
  }
  return usesModeArg;
}

/**
 * Detects whether a drizzle `PgSession.prepareQuery` uses the new (>= rc.1)
 * `mode` signature, without reading `drizzle-orm`'s version from
 * `package.json`.
 *
 * The only behavioral difference that matters to us is the second positional
 * parameter: the new signature names it `mode` (a `'arrays' | 'objects' |
 * 'raw'` string), while the old one names it `fields`. We read that directly
 * off the function source. As a resilience fallback (e.g. if the source is
 * minified and parameter names are mangled), we use the parameter count: the
 * old signature declares 7 required-ish params, the new one declares 4 before
 * its first optional.
 *
 * Exported for testing.
 *
 * @param prepareQuery - The `session.prepareQuery` function to inspect.
 */
export function detectUsesModeArg(
  prepareQuery: (...args: never[]) => unknown,
): boolean {
  const secondParam = secondParamName(prepareQuery);
  if (secondParam === 'mode') {
    return true;
  }
  if (secondParam === 'fields') {
    return false;
  }
  // Names were unavailable (e.g. minified). Fall back to arity: the old
  // signature has more leading required params than the new one.
  return prepareQuery.length <= 4;
}

/** Matches a leading JavaScript identifier (strips defaults/destructuring noise). */
const IDENTIFIER_RE = /^[A-Za-z_$][\w$]*/;

/**
 * Extracts the name of the second declared parameter of a function from its
 * source, or `undefined` if it can't be determined.
 */
function secondParamName(
  fn: (...args: never[]) => unknown,
): string | undefined {
  try {
    const src = Function.prototype.toString.call(fn);
    const open = src.indexOf('(');
    const close = src.indexOf(')', open);
    if (open === -1 || close === -1) {
      return undefined;
    }
    const params = src.slice(open + 1, close).split(',');
    const second = params[1]?.trim();
    if (!second) {
      return undefined;
    }
    const match = IDENTIFIER_RE.exec(second);
    return match?.[0];
  } catch {
    return undefined;
  }
}
