type IsPlainObject<T> = T extends object
  ? T extends Function | unknown[]
    ? false
    : true
  : false;

export type DeepMerge<A, B> = {
  [K in keyof A | keyof B]: K extends keyof B
    ? K extends keyof A
      ? IsPlainObject<A[K]> extends true
        ? IsPlainObject<B[K]> extends true
          ? DeepMerge<A[K], B[K]> // Recursively merge objects
          : B[K] // B wins
        : B[K]
      : B[K]
    : K extends keyof A
      ? A[K]
      : never;
};
