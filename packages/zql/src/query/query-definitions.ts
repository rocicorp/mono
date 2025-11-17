import type {Schema} from '../../../zero-types/src/schema.ts';
import type {DefinedQueryFunction} from './define-query.ts';

// oxlint-disable no-explicit-any

export type QueryDefinitions<TSchema extends Schema, TContext> = {
  [namespaceOrKey: string]:
    | {
        [key: string]: DefinedQueryFunction<
          TSchema,
          keyof TSchema['tables'] & string,
          any,
          TContext,
          any,
          any
        >;
      }
    | DefinedQueryFunction<
        TSchema,
        keyof TSchema['tables'] & string,
        any,
        TContext,
        any,
        any
      >;
};

export type NamespacedNamesOfQueryDefinitions<
  QD extends QueryDefinitions<Schema, any>,
> = {
  [K in keyof QD]: QD[K] extends DefinedQueryFunction<
    Schema,
    keyof Schema['tables'] & string,
    any,
    any,
    any,
    any
  >
    ? K & string
    : QD[K] extends {
          [key: string]: DefinedQueryFunction<
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
