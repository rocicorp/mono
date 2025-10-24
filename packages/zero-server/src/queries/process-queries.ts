import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {type AnyQuery} from '../../../zql/src/query/query-impl.ts';
import * as v from '../../../shared/src/valita.ts';
import {
  transformRequestMessageSchema,
  type TransformRequestMessage,
  type TransformResponseMessage,
} from '../../../zero-protocol/src/custom-queries.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {clientToServer} from '../../../zero-schema/src/name-mapper.ts';
import {mapAST} from '../../../zero-protocol/src/ast.ts';
import type {MaybePromise} from '../../../shared/src/types.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {createLogContext} from '../logging.ts';
import type {LogLevel} from '@rocicorp/logger';
import {getErrorDetails, getErrorMessage} from '../error.ts';

/**
 * Invokes the callback `cb` for each query in the request or JSON body.
 * The callback should return a Query or Promise<Query> that is the transformed result.
 *
 * This function will call `cb` in parallel for each query found in the request.
 *
 * If you need to limit concurrency, you can use a library like `p-limit` to wrap the `cb` function.
 */
export async function handleGetQueriesRequest<S extends Schema>(
  cb: (
    name: string,
    args: readonly ReadonlyJSONValue[],
  ) => MaybePromise<{query: AnyQuery}>,
  schema: S,
  requestOrJsonBody: Request | ReadonlyJSONValue,
  logLevel?: LogLevel,
): Promise<TransformResponseMessage> {
  const lc = createLogContext(logLevel ?? 'info').withContext('GetQueries');

  let parsed: TransformRequestMessage;
  let queryIDs: string[] = [];
  try {
    let body: ReadonlyJSONValue;
    if (requestOrJsonBody instanceof Request) {
      body = await requestOrJsonBody.json();
    } else {
      body = requestOrJsonBody;
    }

    parsed = v.parse(body, transformRequestMessageSchema);

    queryIDs = parsed[1].map(r => r.id);
  } catch (error) {
    lc.error?.('Failed to parse get queries request', error);

    const message = `Failed to parse get queries request: ${getErrorMessage(error)}`;
    const details = getErrorDetails(error);

    return [
      'transformFailed',
      {
        kind: ErrorKind.TransformFailed,
        origin: ErrorOrigin.Server,
        type: 'parse',
        message,
        queryIDs,
        ...(details ? {details} : {}),
      },
    ];
  }

  try {
    const nameMapper = clientToServer(schema.tables);

    // TODO(0xcadams): should every query fail if one fails?
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
  } catch (e) {
    const message = getErrorMessage(e);
    const details = getErrorDetails(e);

    return [
      'transformFailed',
      {
        kind: ErrorKind.TransformFailed,
        origin: ErrorOrigin.Server,
        type: 'internal',
        message,
        queryIDs,
        ...(details ? {details} : {}),
      },
    ];
  }
}
