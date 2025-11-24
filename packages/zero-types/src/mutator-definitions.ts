// oxlint-disable no-explicit-any
import type {MutatorDefinition} from './define-mutator.ts';
import type {Schema} from './schema.ts';

/**
 * A collection of mutator definitions that can be organized hierarchically.
 *
 * This type represents a mapping where each key can either point to:
 * - A single `MutatorDefinition` that defines how to mutate data within a schema
 * - A nested `MutatorDefinitions` object for hierarchical organization
 *
 * @template S - The schema type that defines the structure of the data
 * @template C - The context type available to mutators during execution
 *
 * @example
 * ```typescript
 * const mutators: MutatorDefinitions<MySchema, MyContext> = {
 *   user: {
 *     create: userCreateMutator,
 *     update: userUpdateMutator,
 *     delete: userDeleteMutator
 *   },
 *   post: {
 *     publish: postPublishMutator
 *   }
 * };
 * ```
 */
export type MutatorDefinitions<S extends Schema, C> = {
  readonly [key: string]:
    | MutatorDefinition<S, C, any, any, any>
    | MutatorDefinitions<S, C>;
};

export type NamespacedNamesOfMutatorDefinitions<
  QD extends MutatorDefinitions<Schema, any>,
> = {
  [K in keyof QD]: QD[K] extends MutatorDefinition<Schema, any, any, any, any>
    ? K & string
    : QD[K] extends {
          [key: string]: MutatorDefinition<Schema, any, any, any, any>;
        }
      ? {
          [NK in keyof QD[K]]: `${K & string}.${NK & string}`;
        }[keyof QD[K]]
      : never;
}[keyof QD];
