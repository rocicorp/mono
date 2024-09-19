/**
 * Utility type that statically confirms that a defined type T
 * satisfies a wider type U.
 *
 * Example:
 *
 * ```ts
 * export MyType = Satisfies<JSONValue, {
 *  // type definition
 * }>;
 * ```
 */
export type Satisfies<U, T extends U> = T;
