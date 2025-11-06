type IsPlainObject<T> = T extends object
  ? T extends Function | unknown[]
    ? false
    : true
  : false;

// Force TypeScript to evaluate/flatten a type
type Simplify<T> = {[K in keyof T]: T[K]} & {};

export type DeepMerge<A, B> = Simplify<{
  [K in keyof A | keyof B]: K extends keyof B
    ? K extends keyof A
      ? IsPlainObject<A[K]> extends true
        ? IsPlainObject<B[K]> extends true
          ? Simplify<DeepMerge<A[K], B[K]>> // Recursively merge objects
          : B[K] // B wins
        : B[K]
      : B[K]
    : K extends keyof A
      ? A[K]
      : never;
}>;
