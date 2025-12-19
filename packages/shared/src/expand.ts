/**
 * Expand/simplifies a type for display in Intellisense.
 */
export type Expand<T> = T extends infer O
  ? {readonly [K in keyof O]: O[K]}
  : never;

export type ExpandRecursive<T> = T extends object
  ? T extends infer O
    ? {readonly [K in keyof O]: ExpandRecursive<O[K]>}
    : never
  : T;
