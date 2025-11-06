import type {DeepMerge} from './deep-merge.ts';

/**
 * Helper function to build nested object structure from a dot-separated key path.
 * For example, "users.profile.update" creates {users: {profile: {update: value}}}
 *
 * The function is type-safe and returns a refined type that merges the new path
 * with the existing target type, allowing for progressive type refinement.
 *
 * Note: TypeScript doesn't support assertion functions that transform types (only narrow),
 * so we need to return the value.
 *
 * @param target - The root object to build the structure in
 * @param fullKey - Dot-separated key path (e.g., "users.profile.update")
 * @param value - The value to set at the leaf node
 * @returns The target object with refined type including the new nested path
 *
 * @example
 * ```ts
 * const obj1 = {};
 * const obj2 = buildNestedObjectPath(obj1, 'users.create', '.', createFn);
 * const obj3 = buildNestedObjectPath(obj2, 'users|update', '|', updateFn);
 * // obj3 is now typed as: { users: { create: typeof createFn, update: typeof updateFn } }
 * ```
 */
export function buildNestedObjectPath<
  T extends Record<string, unknown>,
  Path extends string,
  Separator extends string,
  Value,
>(
  target: T,
  fullKey: Path,
  separator: Separator,
  value: Value,
): DeepMerge<T, BuildNested<Path, Separator, Value>> {
  const parts = fullKey.split(separator);
  let current = target as Record<string, unknown>;
  for (let i = 0; i < parts.length - 1; i++) {
    const part = parts[i];
    let next = current[part];
    if (next === undefined) {
      next = {};
      current[part] = next;
    }
    current = next as Record<string, unknown>;
  }
  current[parts[parts.length - 1]] = value;
  return target as DeepMerge<T, BuildNested<Path, Separator, Value>>;
}

// Recursively build nested object from dot-separated path
export type BuildNested<
  Path extends string,
  Separator extends string,
  Value,
> = Path extends `${infer First}${Separator}${infer Rest}`
  ? {[K in First]: BuildNested<Rest, Separator, Value>}
  : {[K in Path]: Value};
