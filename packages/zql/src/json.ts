// This file contains a copy of the JSON type definition used in Replicache and
// Reflect. It is purposely copied to decouple these three projects, or in other
// words to make this library structurally typed wrt its dependencies.

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

/** Like {@link JSONValue} but deeply readonly */
export type ReadonlyJSONValue =
  | null
  | string
  | boolean
  | number
  | ReadonlyArray<ReadonlyJSONValue>
  | ReadonlyJSONObject;

/** Like {@link JSONObject} but deeply readonly */
export type ReadonlyJSONObject = {
  readonly [key: string]: ReadonlyJSONValue | undefined;
};
