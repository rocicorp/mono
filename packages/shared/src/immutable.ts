type Primitive = undefined | null | boolean | string | number | symbol | bigint;

/**
 * Create a deeply immutable type from a type that may contain mutable types.
 */
export type Immutable<T> = T extends Primitive
  ? T
  : T extends Array<infer U>
  ? ImmutableArray<U>
  : ImmutableObject<T>;
// This does not deal with Maps or Sets (or Date or RegExp or ...).

export type ImmutableArray<T> = ReadonlyArray<Immutable<T>>;

export type ImmutableObject<T> = {readonly [K in keyof T]: Immutable<T[K]>};
