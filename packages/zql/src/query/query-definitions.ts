import type {Schema} from '../../../zero-types/src/schema.ts';
import type {QueryDefinition} from './define-query.ts';

// oxlint-disable no-explicit-any

export type QueryDefinitions<S extends Schema, Context> = {
  readonly [key: string]:
    | {
        [key: string]: QueryDefinition<S, any, any, Context, any, any>;
      }
    | QueryDefinition<S, any, any, Context, any, any>;
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
