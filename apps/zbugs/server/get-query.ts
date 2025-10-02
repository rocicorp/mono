import type {AnyQuery, ReadonlyJSONValue} from '@rocicorp/zero';
import type {AuthData} from '../shared/auth.ts';
import {queries as sharedQueries} from '../shared/queries.ts';

// It's important to map incoming queries by queryName, not the
// field name in queries. The latter is just a local identifier.
// queryName is more like an API name that should be stable between
// clients and servers.

export function getQuery(
  context: AuthData | undefined,
  name: string,
  args: readonly ReadonlyJSONValue[],
): AnyQuery {
  if (name in sharedQueries) {
    // Type assertion is necessary because sharedQueries contains different
    // NamedQueryFunction types with varying signatures. All defineQuery
    // functions have the same runtime structure: they accept (args) and return
    // an object with withContext method.
    const queryFn = sharedQueries[
      name as keyof typeof sharedQueries
    ] as unknown as (...args: readonly ReadonlyJSONValue[]) => AnyQuery;

    const rootQuery = queryFn(...args);
    return rootQuery.withContext(context);
  }
  throw new Error(`Unknown query: ${name}`);
}
