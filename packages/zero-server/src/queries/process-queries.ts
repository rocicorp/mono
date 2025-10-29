import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {MaybePromise} from '../../../shared/src/types.ts';
import * as v from '../../../shared/src/valita.ts';
import {mapAST} from '../../../zero-protocol/src/ast.ts';
import {
  transformRequestMessageSchema,
  type TransformResponseMessage,
} from '../../../zero-protocol/src/custom-queries.ts';
import {clientToServer} from '../../../zero-schema/src/name-mapper.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {queryWithContext} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';

/**
 * Invokes the callback `cb` for each query in the request or JSON body.
 * The callback should return a Query or Promise<Query> that is the transformed result.
 *
 * This function will call `cb` in parallel for each query found in the request.
 *
 * If you need to limit concurrency, you can use a library like `p-limit` to wrap the `cb` function.
 */
export async function handleGetQueriesRequest<S extends Schema, Context>(
  cb: (
    name: string,
    args: readonly ReadonlyJSONValue[],
  ) => MaybePromise<{query: AnyQuery} | AnyQuery>,
  schema: S,
  requestOrJsonBody: Request | ReadonlyJSONValue,
  context: Context,
): Promise<TransformResponseMessage> {
  const nameMapper = clientToServer(schema.tables);

  let body: ReadonlyJSONValue;
  if (requestOrJsonBody instanceof Request) {
    body = await requestOrJsonBody.json();
  } else {
    body = requestOrJsonBody;
  }

  const parsed = v.parse(body, transformRequestMessageSchema);
  const responses = await Promise.all(
    parsed[1].map(async req => {
      let query = await cb(req.name, req.args);
      // For backwards compatibility, we allow wrapping the query in an object.
      if ('query' in query) {
        query = query.query;
      }
      const q = queryWithContext(query, context);
      return {
        id: req.id,
        name: req.name,
        ast: mapAST(q.ast, nameMapper),
      };
    }),
  );

  return ['transformed', responses];
}
