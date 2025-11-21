import type {Schema} from '../../../zero-types/src/schema.ts';
import type {QueryDefinition} from './define-query.ts';

// oxlint-disable no-explicit-any

/**
 * Defines a collection of custom queries that can be arbitrarily nested.
 *
 * This type supports creating namespaced query hierarchies of any depth.
 * Each key can either be a QueryDefinition (a leaf query) or another
 * QueryDefinitions object (a namespace containing more queries).
 *
 * @example
 * ```typescript
 * const queries = {
 *   getUser: defineQuery(...),           // Direct query
 *   admin: {                              // Namespace
 *     users: {                            // Nested namespace
 *       getAll: defineQuery(...),         // Deeply nested query
 *     },
 *   },
 * };
 * ```
 */
export type QueryDefinitions<S extends Schema, Context> = {
  readonly [key: string]:
    | QueryDefinition<S, any, any, Context, any, any>
    | QueryDefinitions<S, Context>;
};

export type NamespacedNamesOfQueryDefinitions<
  QD extends QueryDefinitions<Schema, any>,
> = {
  [K in keyof QD]: QD[K] extends QueryDefinition<
    Schema,
    keyof Schema['tables'] & string,
    any,
    any,
    any,
    any
  >
    ? K & string
    : QD[K] extends {
          [key: string]: QueryDefinition<
            Schema,
            keyof Schema['tables'] & string,
            any,
            any,
            any,
            any
          >;
        }
      ? {
          [NK in keyof QD[K]]: `${K & string}.${NK & string}`;
        }[keyof QD[K]]
      : never;
}[keyof QD];
