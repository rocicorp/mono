import {getValueAtPath} from '../../shared/src/get-value-at-path.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {Transaction} from '../../zql/src/mutate/custom.ts';
import {validateInput} from '../../zql/src/query/validate-input.ts';
import {
  isMutator,
  isMutatorDefinition,
  type MutationRequest,
  type Mutator,
  type MutatorDefinition,
} from './mutator.ts';
import type {Schema} from './schema.ts';

/**
 * Creates a MutatorRegistry from a tree of MutatorDefinitions,
 * optionally extending a base MutatorRegistry.
 *
 * @example
 * ```typescript
 * // Create a new registry
 * const mutators = defineMutators({
 *   user: {
 *     create: defineMutator(...),
 *     delete: defineMutator(...),
 *   },
 *   post: {
 *     publish: defineMutator(...),
 *   },
 * });
 *
 * // Extend an existing registry (e.g., for server-side overrides)
 * const serverMutators = defineMutators(mutators, {
 *   user: {
 *     create: defineMutator(...),  // overrides mutators.user.create
 *   },
 *   // post.publish is inherited from mutators
 * });
 *
 * // Access mutators by path
 * const mr = mutators.user.create({name: 'Alice'});
 *
 * // Execute on client
 * zero.mutate(mr);
 *
 * // Execute on server
 * mr.mutator.fn({tx, ctx, args: mr.args});
 *
 * // Lookup by name (for server-side dispatch)
 * const mutator = getMutator(mutators, 'user.create');
 * ```
 */
export function defineMutators<
  S extends Schema,
  C,
  T extends MutatorDefinitionsTree<S, C>,
>(definitions: T): MutatorRegistry<S, C, T>;

export function defineMutators<
  S extends Schema,
  C,
  TBase extends MutatorDefinitionsTree<S, C>,
>(
  base: MutatorRegistry<S, C, TBase>,
  overrides: MutatorDefinitionsTree<S, C>,
): MutatorRegistry<S, C, TBase>;

export function defineMutators<S extends Schema, C>(
  definitionsOrBase: MutatorDefinitionsTree<S, C> | AnyMutatorRegistry,
  maybeOverrides?: MutatorDefinitionsTree<S, C>,
): AnyMutatorRegistry {
  let tree: Record<string | symbol, unknown>;

  if (isMutatorRegistry(definitionsOrBase) && maybeOverrides !== undefined) {
    // Extending a base registry
    tree = buildTreeWithBase(definitionsOrBase, maybeOverrides, []);
  } else {
    // Creating a new registry from definitions
    tree = buildTree(definitionsOrBase as MutatorDefinitionsTree<S, C>, []);
  }

  tree[mutatorRegistryTag] = true;

  return tree as AnyMutatorRegistry;
}

/**
 * Like `defineMutators`, but allows specifying Schema and Context types upfront.
 * This is useful when TypeScript can't infer the context type from the definitions.
 *
 * @example
 * ```ts
 * const defineMutators = defineMutatorsWithType<Schema, AuthData>();
 *
 * // Create a new registry
 * const mutators = defineMutators({
 *   user: {
 *     create: defineMutator(...),
 *   },
 * });
 *
 * // Or extend a base registry
 * const serverMutators = defineMutators(clientMutators, {
 *   user: {
 *     create: defineMutator(...),  // override
 *   },
 * });
 * ```
 */
export function defineMutatorsWithType<S extends Schema, C>(): {
  <T extends MutatorDefinitionsTree<S, C>>(
    definitions: T,
  ): MutatorRegistry<S, C, T>;
  <TBase extends MutatorDefinitionsTree<S, C>>(
    base: MutatorRegistry<S, C, TBase>,
    overrides: MutatorDefinitionsTree<S, C>,
  ): MutatorRegistry<S, C, TBase>;
} {
  // oxlint-disable-next-line no-explicit-any
  return defineMutators as any;
}

/**
 * Gets a Mutator by its dot-separated name from a MutatorRegistry.
 * Returns undefined if not found.
 */
export function getMutator(
  registry: unknown,
  name: string,
  // oxlint-disable-next-line no-explicit-any
): Mutator<any, any, any, any> | undefined {
  if (typeof registry !== 'object' || registry === null) {
    return undefined;
  }
  const m = getValueAtPath(registry, name, '.');

  // oxlint-disable-next-line no-explicit-any
  return m as Mutator<any, any, any, any> | undefined;
}

/**
 * Gets a Mutator by its dot-separated name from a MutatorRegistry.
 * Throws if not found.
 */
export function mustGetMutator(
  registry: AnyMutatorRegistry,
  name: string,
  // oxlint-disable-next-line no-explicit-any
): Mutator<any, any, any, any> {
  const mutator = getMutator(registry, name);
  if (mutator === undefined) {
    throw new Error(`Mutator not found: ${name}`);
  }
  return mutator;
}

/**
 * Checks if a value is a MutatorRegistry.
 */
export function isMutatorRegistry<S extends Schema, C>(
  value: unknown,
): value is MutatorRegistry<S, C, MutatorDefinitionsTree<S, C>> {
  return (
    typeof value === 'object' && value !== null && mutatorRegistryTag in value
  );
}

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

/**
 * A tree of MutatorDefinitions, possibly nested.
 */
export type MutatorDefinitionsTree<S extends Schema, C> = {
  readonly [key: string]: // oxlint-disable-next-line no-explicit-any
  MutatorDefinition<S, C, any, any, any> | MutatorDefinitionsTree<S, C>;
};

/**
 * Alias for MutatorDefinitionsTree for backward compatibility.
 */
export type MutatorDefinitions<S extends Schema, C> = MutatorDefinitionsTree<
  S,
  C
>;

/**
 * The result of defineMutators(). A tree of Mutators with a tag for detection.
 */
export type MutatorRegistry<
  S extends Schema,
  C,
  T extends MutatorDefinitionsTree<S, C>,
> = ToMutatorTree<S, C, T> & {
  [mutatorRegistryTag]: true;
};

/**
 * A branded type for use in type constraints. Use this instead of
 * `MutatorRegistry<S, C, any>` to avoid TypeScript drilling into
 * the complex ToMutatorTree structure and hitting variance issues.
 */
export type AnyMutatorRegistry = {[mutatorRegistryTag]: true} & Record<
  string,
  unknown
>;

// ----------------------------------------------------------------------------
// Internal
// ----------------------------------------------------------------------------

const mutatorRegistryTag = Symbol('mutatorRegistry');

/**
 * Transforms a MutatorDefinitionsTree into a tree of Mutators.
 * Each MutatorDefinition becomes a Mutator at the same path.
 * Uses TOutput (the validated type) as TArgs for the resulting Mutator.
 */
type ToMutatorTree<
  S extends Schema,
  C,
  T extends MutatorDefinitionsTree<S, C>,
> = {
  readonly [K in keyof T]: T[K] extends MutatorDefinition<
    S,
    C,
    // oxlint-disable-next-line no-explicit-any
    any, // TInput - we don't need it for the public Mutator type
    infer TOutput,
    infer TWrappedTransaction
  >
    ? Mutator<S, C, TOutput, TWrappedTransaction>
    : T[K] extends MutatorDefinitionsTree<S, C>
      ? ToMutatorTree<S, C, T[K]>
      : never;
};

// oxlint-disable-next-line no-explicit-any
type AnyMutatorDefinition = MutatorDefinition<Schema, any, any, any, any>;

function buildTree<S extends Schema, C>(
  defs: MutatorDefinitionsTree<S, C>,
  path: string[],
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(defs)) {
    const currentPath = [...path, key];

    if (isMutatorDefinition(value)) {
      const name = currentPath.join('.');

      const mutator = createMutator(name, value as AnyMutatorDefinition);
      result[key] = mutator;
    } else {
      // Nested namespace
      result[key] = buildTree(
        value as MutatorDefinitionsTree<S, C>,
        currentPath,
      );
    }
  }

  return result;
}

/**
 * Builds a tree by merging a base registry with overrides.
 * Overrides can contain MutatorDefinitions (which get converted to Mutators)
 * or nested objects (which get recursively merged).
 * Base Mutators are copied directly when not overridden.
 */
function buildTreeWithBase<S extends Schema, C>(
  base: AnyMutatorRegistry | Record<string, unknown>,
  overrides: MutatorDefinitionsTree<S, C>,
  path: string[],
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  // First, copy all base entries
  for (const [key, value] of Object.entries(base)) {
    // Skip the registry tag
    if (typeof key === 'symbol') continue;

    const currentPath = [...path, key];

    if (isMutator(value)) {
      // Copy the base mutator directly
      result[key] = value;
    } else if (typeof value === 'object' && value !== null) {
      // Nested namespace - recurse with empty overrides to copy
      result[key] = buildTreeWithBase(
        value as Record<string, unknown>,
        {},
        currentPath,
      );
    }
  }

  // Then apply overrides
  for (const [key, value] of Object.entries(overrides)) {
    const currentPath = [...path, key];

    if (isMutatorDefinition(value)) {
      // Override with new mutator
      const name = currentPath.join('.');
      result[key] = createMutator(
        name,
        // oxlint-disable-next-line no-explicit-any
        value as MutatorDefinition<S, C, any, any, any>,
      );
    } else if (typeof value === 'object' && value !== null) {
      // Nested override - merge with existing base namespace
      const baseNamespace = (result[key] ?? {}) as Record<string, unknown>;
      result[key] = buildTreeWithBase(
        baseNamespace,
        value as MutatorDefinitionsTree<S, C>,
        currentPath,
      );
    }
  }

  return result;
}

function createMutator<S extends Schema, C>(
  name: string,
  // oxlint-disable-next-line no-explicit-any
  definition: MutatorDefinition<S, C, any, any, any>,
  // oxlint-disable-next-line no-explicit-any
): Mutator<S, C, any, any> {
  const {validator} = definition;

  // fn takes ReadonlyJSONValue args because it's called during rebase (from
  // stored JSON) and on the server (from wire format). Validation happens here.
  const fn = async (options: {
    args: ReadonlyJSONValue | undefined;
    ctx: C;
    tx: Transaction<S, unknown>;
  }): Promise<void> => {
    const validatedArgs = validator
      ? validateInput(name, options.args, validator, 'mutator')
      : options.args;
    await definition({
      args: validatedArgs,
      ctx: options.ctx,
      tx: options.tx,
    });
  };

  // Create the callable mutator
  // oxlint-disable-next-line no-explicit-any
  const mutator = (args: unknown): MutationRequest<S, C, any, any> => ({
    // oxlint-disable-next-line no-explicit-any
    mutator: mutator as Mutator<S, C, any, any>,
    args,
  });
  mutator.mutatorName = name;
  mutator.fn = fn;

  // oxlint-disable-next-line no-explicit-any
  return mutator as Mutator<S, C, any, any>;
}
