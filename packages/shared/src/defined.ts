/**
 * A value that can be distinguished from "absent" by an `!== undefined` check.
 * `undefined` itself is excluded, so APIs that use it as a sentinel -- a
 * `Map.get` miss, an exhausted {@linkcode ValueIterator} -- stay unambiguous.
 */
export type Defined = {} | null;
