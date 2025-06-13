import type {NamedQueryImpl, ReadonlyJSONValue} from '@rocicorp/zero';
import * as serverQueries from '../server/server-queries.ts';
import * as sharedQueries from '../shared/queries.ts';

export function getQuery(
  context: serverQueries.ServerContext,
  name: string,
  args: readonly ReadonlyJSONValue[],
) {
  let query;
  if (isServerQuery(name)) {
    query = (serverQueries[name] as serverQueries.ServerQuery)(
      context,
      ...args,
    );
  } else if (isSharedQuery(name)) {
    query = (sharedQueries[name] as NamedQueryImpl)(...args);
  } else {
    throw new Error(`Unknown query: ${name}`);
  }

  return query;
}

function isServerQuery(key: string): key is keyof typeof serverQueries {
  return key in serverQueries;
}

function isSharedQuery(key: string): key is keyof typeof sharedQueries {
  return key in sharedQueries;
}
