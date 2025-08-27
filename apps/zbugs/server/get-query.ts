import {withValidation, type ReadonlyJSONValue} from '@rocicorp/zero';
import {queries} from '../shared/queries.ts';
import type {AuthData} from '../shared/auth.ts';

// It's important to map incoming queries by queryName, not the
// field name in queries. The latter is just a local identifier.
// queryName is more like an API name that should be stable between
// clients and servers.
const validated = Object.fromEntries(
  Object.values(queries).map(q => [q.queryName, withValidation(q)]),
);

export function getQuery(
  context: AuthData | undefined,
  name: string,
  args: readonly ReadonlyJSONValue[],
) {
  if (name in validated) {
    return validated[name](context, ...args);
  }

  throw new Error(`Unknown query: ${name}`);
}
