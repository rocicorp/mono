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
const queriesByQueryName = Object.fromEntries(
  Object.values(sharedQueries).map(q => [q.queryName, q]),
);

export function getQuery(
  name: string,
  args: readonly ReadonlyJSONValue[],
): AnyQuery {
  if (name in queriesByQueryName) {
    const f = queriesByQueryName[name] as AnyNamedQueryFunction;
    return f(...args);
  }
  throw new Error(`Unknown query: ${name}`);
}
