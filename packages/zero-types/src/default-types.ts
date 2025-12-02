import type {BaseDatabase} from './database.ts';
import type {Schema} from './schema.ts';

// oxlint-disable no-explicit-any

/**
 * Applications can augment this interface to register their Zero types via
 * declaration merging:
 *
 * ```ts
 * declare module '@rocicorp/zero' {
 *   interface DefaultTypes {
 *     schema: typeof schema;
 *     context: Context;
 *     dbProvider: typeof dbProvider;
 *   }
 * }
 * ```
 */
export interface DefaultTypes {}

export type DefaultSchema<TRegister = DefaultTypes> = TRegister extends {
  schema: infer S extends Schema;
}
  ? S
  : Schema;

export type DefaultContext<TRegister = DefaultTypes> = TRegister extends {
  context: infer C;
}
  ? C
  : unknown;

export type DefaultDbProvider<TRegister = DefaultTypes> = TRegister extends {
  dbProvider: infer D extends BaseDatabase<any, any, any>;
}
  ? D
  : unknown;

export type DefaultWrappedTransaction<TRegister = DefaultTypes> =
  DefaultDbProvider<TRegister> extends BaseDatabase<
    infer TTransaction,
    any,
    any
  >
    ? TTransaction extends {
        dbTransaction: infer TDbTransaction;
      }
      ? TDbTransaction extends {
          wrappedTransaction: infer TWrappedTransaction;
        }
        ? TWrappedTransaction
        : unknown
      : unknown
    : unknown;
