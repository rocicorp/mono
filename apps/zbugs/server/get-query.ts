import {
  withContext,
  withValidation,
  type ReadonlyJSONValue,
  type SyncedQuery,
} from '@rocicorp/zero';
import {queries} from '../shared/queries.ts';
import type {AuthData} from '../shared/auth.ts';

export function getQuery(
  context: AuthData | undefined,
  name: string,
  args: readonly ReadonlyJSONValue[],
) {
  if (isQuery(name)) {
    // The cast is required because, otherwise, TypeScript reduces `queries[name]` to the supertype of all
    // queries defined on `queries`. This is because `name` can be any key of the `queries` object.
    // E.g.,
    // const queries = { foo(id: string) {}, bar(created: number) {}}
    // const q = queries[name];
    // typeof q == `(arg: never) => SyncedQuery`
    try {
      return withValidation(withContext(queries[name] as SyncedQuery))(
        context,
        ...args,
      );
    } catch (e) {
      console.error(`Error in getQuery for ${name}`, e);
    }
  }
  throw new Error(`Unknown query: ${name}`);
}

function isQuery(key: string): key is keyof typeof queries {
  return key in queries;
}
