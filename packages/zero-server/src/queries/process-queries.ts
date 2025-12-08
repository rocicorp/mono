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
import {clientToServer} from '../../../zero-schema/src/name-mapper.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {QueryParseError} from '../../../zql/src/query/error.ts';
import type {AnyQueryBuilder} from '../../../zql/src/query/query-builder.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import {createLogContext} from '../logging.ts';

/**
 * Invokes the callback `cb` for each query in the request or JSON body.
 * The callback should return a Query or Promise<Query> that is the transformed result.
 *
 * This function will call `cb` in parallel for each query found in the request.
 *
 * If you need to limit concurrency, you can use a library like `p-limit` to wrap the `cb` function.
 * @deprecated Use {@linkcode handleQueryRequest} instead.
 */
export function handleGetQueriesRequest<S extends Schema>(
  cb: (
    name: string,
    args: readonly ReadonlyJSONValue[],
  ) => MaybePromise<{query: AnyQueryBuilder} | AnyQueryBuilder>,
  schema: S,
  requestOrJsonBody: Request | ReadonlyJSONValue,
  logLevel: LogLevel = 'info',
): Promise<TransformResponseMessage> {
  return transform(cb, schema, requestOrJsonBody, 'getQueries', logLevel);
}

/**
 * Invokes the callback `cb` for each query in the request or JSON body.
 * The callback should return a Query or Promise<Query> that is the transformed result.
 *
 * This function will call `cb` in parallel for each query found in the request.
 *
 * If you need to limit concurrency, you can use a library like `p-limit` to wrap the `cb` function.
 * @deprecated Use {@linkcode handleQueryRequest} instead.
 */
export function handleTransformRequest<S extends Schema>(
  cb: (
    name: string,
    args: readonly ReadonlyJSONValue[],
  ) => MaybePromise<{query: AnyQueryBuilder} | AnyQueryBuilder>,
  schema: S,
  requestOrJsonBody: Request | ReadonlyJSONValue,
  logLevel: LogLevel = 'info',
): Promise<TransformResponseMessage> {
  return transform(cb, schema, requestOrJsonBody, 'transform', logLevel);
}

/**
 * Processes a transform request by invoking the provided callback for each query.
 * The callback should return a Query that is the transformed result.
 *
 * This function will call `transformQuery` in parallel for each query found in the request.
 *
 * @param transformQuery - Callback function that takes a query name and args, and returns a Query
 * @param schema - The Zero schema
 * @param requestOrJsonBody - Either a Request object or the JSON body directly
 * @param logLevel - Logging level (defaults to 'info')
 * @returns A Promise that resolves to a TransformResponseMessage
 */
export function handleQueryRequest<S extends Schema>(
  transformQuery: TransformQueryFunction,
  schema: S,
  requestOrJsonBody: Request | ReadonlyJSONValue,
  logLevel: LogLevel = 'info',
) {
  return transform(
    (name, argsArray) => transformQuery(name, argsArray[0]),
    schema,
    requestOrJsonBody,
    'query',
    logLevel,
  );
}

async function transform<S extends Schema>(
  cb: (
    name: string,
    args: readonly ReadonlyJSONValue[],
  ) => MaybePromise<{query: AnyQueryBuilder} | AnyQueryBuilder>,
  schema: S,
  requestOrJsonBody: Request | ReadonlyJSONValue,
  apiName: 'query' | 'getQueries' | 'transform',
  logLevel: LogLevel = 'info',
): Promise<TransformResponseMessage> {
  const lc = createLogContext(logLevel).withContext('TransformRequest');
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
    lc.error?.(`Failed to parse ${apiName} request`, error);

    const message = `Failed to parse ${apiName} request: ${getErrorMessage(error)}`;
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
        let finalQuery: AnyQueryBuilder;
        try {
          const result = await cb(req.name, req.args);
          finalQuery = 'query' in result ? result.query : result;
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
          const q = asQueryInternals(finalQuery);
          const ast = mapAST(q.ast, nameMapper);

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

/**
 * A function that transforms a query by name and arguments into a QueryBuilder object.
 *
 * @param name - The name of the query (can be dot-separated for nested queries)
 * @param args - The arguments to pass to the query (can be undefined)
 * @returns A QueryBuilder object
 */
export type TransformQueryFunction = (
  name: string,
  args: ReadonlyJSONValue | undefined,
) => AnyQueryBuilder;
