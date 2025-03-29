type IsPlainObject<T> = T extends object
  ? // eslint-disable-next-line @typescript-eslint/ban-types, @typescript-eslint/no-explicit-any
    T extends Function | any[]
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
