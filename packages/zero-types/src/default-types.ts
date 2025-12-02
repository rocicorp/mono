import type {Database} from './database.ts';
import type {Schema} from './schema.ts';

/**
 * Applications can augment this interface to register their Zero types via
 * declaration merging:
 *
 * ```ts
 * declare module '@rocicorp/zero' {
 *   interface DefaultTypes {
 *     schema: typeof schema;
 *     queries: typeof queries;
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

export type DefaultQueries<TRegister = DefaultTypes> = TRegister extends {
  queries: infer Q;
}
  ? Q
  : undefined;

export type DefaultMutators<TRegister = DefaultTypes> = TRegister extends {
  mutators: infer M;
}
  ? M
  : undefined;

export type DefaultContext<TRegister = DefaultTypes> = TRegister extends {
  context: infer C;
}
  ? C
  : unknown;

export type DefaultDbProvider<TRegister = DefaultTypes> = TRegister extends {
  dbProvider: infer D extends Database<unknown>;
}
  ? D
  : unknown;

export type DefaultWrappedTransaction<TRegister = DefaultTypes> =
  DefaultDbProvider<TRegister> extends Database<infer T>
    ? T extends {
        dbTransaction: infer TDbTransaction;
      }
      ? TDbTransaction extends {
          wrappedTransaction: infer TWrappedTransaction;
        }
        ? TWrappedTransaction
        : unknown
      : unknown
    : unknown;
