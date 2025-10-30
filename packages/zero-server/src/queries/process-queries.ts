import type {LogLevel} from '@rocicorp/logger';
import {getErrorDetails, getErrorMessage} from '../../../shared/src/error.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {MaybePromise} from '../../../shared/src/types.ts';
import * as v from '../../../shared/src/valita.ts';
import {mapAST} from '../../../zero-protocol/src/ast.ts';
import {
  transformRequestMessageSchema,
  type TransformRequestMessage,
  type TransformResponseBody,
  type TransformResponseMessage,
} from '../../../zero-protocol/src/custom-queries.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {clientToServer} from '../../../zero-schema/src/name-mapper.ts';
import {QueryParseError} from '../../../zql/src/query/error.ts';
import type {AnyQuery} from '../../../zql/src/query/query-impl.ts';
import {createLogContext} from '../logging.ts';

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
        reason: ErrorReason.Parse,
        message,
        queryIDs,
        ...(details ? {details} : {}),
      },
    ];
  }

  try {
    const nameMapper = clientToServer(schema.tables);

    const responses: TransformResponseBody = await Promise.all(
      parsed[1].map(async req => {
        let finalQuery: AnyQuery;
        try {
          const result = await cb(req.name, req.args);

          finalQuery = result.query;
        } catch (error) {
          const message = getErrorMessage(error);
          const details = getErrorDetails(error);

          return {
            error: error instanceof QueryParseError ? 'parse' : 'app',
            id: req.id,
            name: req.name,
            message,
            ...(details ? {details} : {}),
          };
        }

        try {
          const ast = mapAST(finalQuery.ast, nameMapper);

          return {
            id: req.id,
            name: req.name,
            ast,
          };
        } catch (error) {
          lc.error?.('Failed to map AST', error);
          throw error;
        }
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
        reason: ErrorReason.Internal,
        message,
        queryIDs,
        ...(details ? {details} : {}),
      },
    ];
  }
}
