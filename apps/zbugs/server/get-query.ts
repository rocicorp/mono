import type {
  AnyNamedQueryFunction,
  AnyQuery,
  ReadonlyJSONValue,
} from '@rocicorp/zero';
import {queries as sharedQueries} from '../shared/queries.ts';

// It's important to map incoming queries by queryName, not the
// field name in queries. The latter is just a local identifier.
// queryName is more like an API name that should be stable between
// clients and servers.

export function getQuery(
  name: string,
  args: ReadonlyJSONValue | undefined,
): AnyQuery {
  if (name in sharedQueries) {
    // Cast is necessary because TypeScript sees a union of incompatible
    // function signatures (each with different parameter types based on
    // validators). At runtime, all queries accept ReadonlyJSONValue or undefined.
    const f = sharedQueries[
      name as keyof typeof sharedQueries
    ] as AnyNamedQueryFunction;
    return f(args);
  }
  throw new Error(`Unknown query: ${name}`);
}
