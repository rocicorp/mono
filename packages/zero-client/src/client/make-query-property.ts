import type {LogContext} from '@rocicorp/logger';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {wrapCustomQuery} from '../../../zql/src/query/define-query.ts';
import type {QueryDefinitions} from '../../../zql/src/query/query-definitions.ts';
import {newQuery} from '../../../zql/src/query/query-impl.ts';
import {isQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {Query} from '../../../zql/src/query/query.ts';

/**
 * Registers both entity queries (from schema tables) and custom queries at arbitrary depth.
 * Returns a combined query interface that includes table queries and custom query namespaces.
 *
 * @param schema - The Zero schema containing table definitions
 * @param queries - Optional custom query definitions that can be nested arbitrarily deep
 * @param contextHolder - The Zero instance that will be passed to wrapCustomQuery for binding
 * @returns A query object with both entity and custom queries
 */
export function makeQueryProperty<
  S extends Schema,
  TContext,
  QD extends QueryDefinitions<S, TContext> | undefined,
>(
  schema: S,
  queries: QD,
  contextHolder: {context: TContext},
  lc: LogContext,
): MakeQueryPropertyType<QD, S, TContext> {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  const rv = {} as Record<string, any>;

  // Register entity queries for each table
  for (const name of Object.keys(schema.tables)) {
    rv[name] = newQuery(schema, name);
  }

  // Register custom queries if provided
  if (queries) {
    // Recursively process query definitions at arbitrary depth
    const processQueries = (
      queriesToProcess: QueryDefinitions<S, TContext>,
      target: Record<string, unknown>,
      namespacePrefix: string[] = [],
    ) => {
      for (const [key, value] of Object.entries(queriesToProcess)) {
        if (typeof value === 'function') {
          const queryName = [...namespacePrefix, key].join('.');
          // Single query function - wrap it with the name
          const targetValue = target[key];
          if (isQueryInternals(targetValue)) {
            lc.debug?.(
              `Query key "${[...namespacePrefix, key].join('.')}" conflicts with an existing table name.`,
            );
          }

          target[key] = wrapCustomQuery(queryName, value, contextHolder);
        } else {
          // Namespace with nested queries
          let existing = target[key];
          // Check if the namespace conflicts with an existing table query
          if (isQueryInternals(existing)) {
            lc.debug?.(
              `Query namespace "${[...namespacePrefix, key].join('.')}" conflicts with an existing table name.`,
            );
          }
          if (existing === undefined) {
            existing = {};
            target[key] = existing;
          }
          processQueries(value, existing as Record<string, unknown>, [
            ...namespacePrefix,
            key,
          ]);
        }
      }
    };
    processQueries(queries, rv);
  }

  return rv as MakeQueryPropertyType<QD, S, TContext>;
}

/**
 * The shape exposed on the `Zero.query` instance with custom queries.
 * Custom defined queries are added as properties that can be called to create query objects.
 */
type MakeCustomQueryInterfaces<
  S extends Schema,
  QD extends QueryDefinitions<S, TContext>,
  TContext,
> = {
  readonly [NamespaceOrName in keyof QD]: QD[NamespaceOrName] extends (options: {
    ctx: TContext;
    args: infer Args;
  }) => Query<S, infer TTable, infer TReturn>
    ? [Args] extends [undefined]
      ? () => Query<S, TTable & string, TReturn>
      : undefined extends Args
        ? (args?: Args) => Query<S, TTable & string, TReturn>
        : (args: Args) => Query<S, TTable & string, TReturn>
    : {
        readonly [P in keyof QD[NamespaceOrName]]: MakeCustomQueryInterface<
          S,
          QD[NamespaceOrName][P],
          TContext
        >;
      };
};

type MakeCustomQueryInterface<
  TSchema extends Schema,
  F,
  TContext,
> = F extends (options: {
  ctx: TContext;
  args: infer Args;
}) => Query<TSchema, infer TTable, infer TReturn>
  ? [Args] extends [undefined]
    ? () => Query<TSchema, TTable & string, TReturn>
    : undefined extends Args
      ? (args?: Args) => Query<TSchema, TTable & string, TReturn>
      : (args: Args) => Query<TSchema, TTable & string, TReturn>
  : never;

export type MakeEntityQueriesFromSchema<S extends Schema> = {
  readonly [K in keyof S['tables'] & string]: Query<S, K>;
};

export type MakeQueryPropertyType<
  QD extends QueryDefinitions<S, C> | undefined,
  S extends Schema,
  C,
> =
  QD extends QueryDefinitions<S, C>
    ? MakeEntityQueriesFromSchema<S> & MakeCustomQueryInterfaces<S, QD, C>
    : MakeEntityQueriesFromSchema<S>;
