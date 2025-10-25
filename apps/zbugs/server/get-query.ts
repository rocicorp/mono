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

export function getQuery(name: string, args: ReadonlyJSONValue): AnyQuery {
  if (name in sharedQueries) {
    // Type assertion needed: sharedQueries contains NamedQueryFunction types with
    // different argument types. TypeScript can't verify all accept the args type,
    // but this is safe since all queries accept JSON-serializable input.
    const f = sharedQueries[
      name as keyof typeof sharedQueries
    ] as AnyNamedQueryFunction;
    return f(args);
  }
  throw new Error(`Unknown query: ${name}`);
}
