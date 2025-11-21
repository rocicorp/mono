import type {Schema} from '../../../zero-types/src/schema.ts';
import type {QueryDefinitions} from '../../../zql/src/query/query-definitions.ts';
import type {CustomMutatorDefs} from './custom.ts';

/**
 * Users can augment this interface to register their application's schema,
 * queries, mutators, and context types globally via declaration merging:
 *
 * ```ts
 * declare module '@rocicorp/zero' {
 *   interface Register {
 *     schema: typeof schema;
 *     queries: typeof queries;
 *     mutators: typeof mutators;
 *     context: Context;
 *   }
 * }
 * ```
 */
export interface Register {}

export type RegisteredSchema<TRegister = Register> = TRegister extends {
  schema: infer S extends Schema;
}
  ? S
  : Schema;

export type RegisteredMutators<TRegister = Register> = TRegister extends {
  mutators: infer M extends CustomMutatorDefs | undefined;
}
  ? M
  : CustomMutatorDefs | undefined;

export type RegisteredContext<TRegister = Register> = TRegister extends {
  context: infer C;
}
  ? C
  : unknown;

export type RegisteredQueries<
  TRegister = Register,
  TSchema extends Schema = RegisteredSchema<TRegister>,
  TContext = RegisteredContext<TRegister>,
> = TRegister extends {
  queries: infer Q extends QueryDefinitions<TSchema, TContext> | undefined;
}
  ? Q
  : undefined;
