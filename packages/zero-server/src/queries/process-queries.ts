import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {type AnyQuery} from '../../../zql/src/query/query-impl.ts';
import * as v from '../../../shared/src/valita.ts';
import {
  transformRequestMessageSchema,
  type TransformResponseMessage,
} from '../../../zero-protocol/src/custom-queries.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {clientToServer} from '../../../zero-schema/src/name-mapper.ts';
import {mapAST} from '../../../zero-protocol/src/ast.ts';

/**
 * Invokes the callback `cb` for each query in the request or JSON body.
 * The callback should return a Query or Promise<Query> that is the transformed result.
 *
 * This function will call `cb` in parallel for each query found in the request.
 *
 * If you need to limit concurrency, you can use a library like `p-limit` to wrap the `cb` function.
 */
export async function getQueries<S extends Schema>(
  cb: (
    name: string,
    args: readonly ReadonlyJSONValue[],
  ) => Promise<{query: AnyQuery}>,
  schema: S,
  requestOrJsonBody: Request | ReadonlyJSONValue,
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
      const {query} = await cb(req.name, req.args);

      return {
        id: req.id,
        name: req.name,
        ast: mapAST(query.ast, nameMapper),
      };
    }),
  );

  return ['transformed', responses];
}
