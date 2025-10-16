/** The values that can be represented in JSON */
export type JSONValue =
  | null
  | string
  | boolean
  | number
  | Array<JSONValue>
  | JSONObject;

/**
 * A JSON object. This is a map from strings to JSON values or `undefined`. We
 * allow `undefined` values as a convenience... but beware that the `undefined`
 * values do not round trip to the server. For example:
 *
 * ```
 * // Time t1
 * await tx.set('a', {a: undefined});
 *
 * // time passes, in a new transaction
 * const v = await tx.get('a');
 * console.log(v); // either {a: undefined} or {}
 * ```
 */
export type JSONObject = {[key: string]: JSONValue | undefined};
