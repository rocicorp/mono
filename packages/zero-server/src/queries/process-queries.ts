import type {LogLevel} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.ts';
import {getErrorDetails, getErrorMessage} from 'shared/src/error.ts';
import type {ReadonlyJSONValue} from 'shared/src/json.ts';
import type {MaybePromise} from 'shared/src/types.ts';
import * as v from 'shared/src/valita.ts';
import {mapAST} from '../../../zero-protocol/src/ast.ts';
import {
  transformRequestMessageSchema,
  type TransformRequestMessage,
  type TransformResponseBody,
} from '../../../zero-protocol/src/custom-queries.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import type {QueryResponse} from '../../../zero-protocol/src/query-server.ts';
import {clientToServer} from '../../../zero-schema/src/name-mapper.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {QueryParseError} from '../../../zql/src/query/error.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
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
  ) => MaybePromise<{query: AnyQuery} | AnyQuery>,
  schema: S,
  requestOrJsonBody: Request | ReadonlyJSONValue,
  logLevel: LogLevel = 'info',
): Promise<QueryResponse> {
  return transform(
    normalizeLegacyQueryRequestArgs(cb, schema, requestOrJsonBody, logLevel),
    'getQueries',
  );
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
  ) => MaybePromise<{query: AnyQuery} | AnyQuery>,
  schema: S,
  requestOrJsonBody: Request | ReadonlyJSONValue,
  logLevel: LogLevel = 'info',
): Promise<QueryResponse> {
  return transform(
    normalizeLegacyQueryRequestArgs(cb, schema, requestOrJsonBody, logLevel),
    'transform',
  );
}

/**
 * Parsed query params accepted by {@linkcode handleQueryRequest} when the
 * incoming request URL has already been handled by your framework.
 */
export type QuerySearchParams = URLSearchParams | Record<string, string>;

export type HandleQueryRequestArgs<S extends Schema> = {
  /** Callback that transforms each requested query into a `Query`. */
  handler: QueryRequestHandler;
  /** Schema used when building the returned ASTs. */
  schema: S;
  /**
   * Authenticated user ID. Null or undefined means the user is logged out.
   */
  userID: string | null | undefined;
  /** Optional log level for request parsing and execution. */
  logLevel?: LogLevel | undefined;
} & (
  | {
      /** Fetch request containing the `/query` JSON body. */
      request: Request;
    }
  | {
      /** Parsed query params from the `/query` request URL. */
      query: QuerySearchParams;
      /** Parsed JSON body from the `/query` request. */
      body: ReadonlyJSONValue;
    }
);

type NormalizedQueryRequestArgs<S extends Schema> = {
  readonly schema: S;
  readonly handler: LegacyQueryRequestHandler;
  // Note: semantics of undefined differ from HandleQueryRequestArgs.userID.
  // Here, undefined means the app didn't provide a user ID - we do not know if
  // the user is logged in or not. This is legacy behavior needed to support
  // deprecated signatures of handleQueryRequest which did not receive userID
  // from app.
  readonly userID: string | null | undefined;
  readonly logLevel: LogLevel;
} & (
  | {
      readonly type: 'request';
      readonly request: Request;
    }
  | {
      readonly type: 'body';
      readonly jsonBody: ReadonlyJSONValue;
    }
);

/**
 * Process a `/query` request from a Fetch `Request`.
 */
export function handleQueryRequest<S extends Schema>(
  input: HandleQueryRequestArgs<S>,
): Promise<QueryResponse>;

/**
 * @deprecated Pass a single object instead:
 * `handleQueryRequest({handler, schema, request, userID, logLevel})`.
 */
export function handleQueryRequest<S extends Schema>(
  transformQuery: QueryRequestHandler,
  schema: S,
  request: Request,
  logLevel?: LogLevel,
): Promise<QueryResponse>;

/**
 * @deprecated Pass a single object instead:
 * `handleQueryRequest({handler, schema, body, userID, logLevel})`.
 */
export function handleQueryRequest<S extends Schema>(
  transformQuery: QueryRequestHandler,
  schema: S,
  jsonBody: ReadonlyJSONValue,
  logLevel?: LogLevel,
): Promise<QueryResponse>;

export function handleQueryRequest<S extends Schema>(
  inputOrTransformQuery: HandleQueryRequestArgs<S> | QueryRequestHandler,
  schema?: S,
  requestOrJsonBody?: Request | ReadonlyJSONValue,
  logLevel?: LogLevel,
): Promise<QueryResponse> {
  const normalized =
    typeof inputOrTransformQuery === 'object' &&
    'handler' in inputOrTransformQuery
      ? normalizeQueryRequestInput(inputOrTransformQuery)
      : normalizeLegacyQueryRequestArgs(
          wrapQueryRequestHandler(inputOrTransformQuery),
          schema,
          requestOrJsonBody,
          logLevel,
        );

  return transform(normalized, 'query');
}

function normalizeQueryRequestInput<S extends Schema>(
  input: HandleQueryRequestArgs<S>,
): NormalizedQueryRequestArgs<S> {
  return 'request' in input
    ? normalizeLegacyQueryRequestArgs(
        wrapQueryRequestHandler(input.handler),
        input.schema,
        input.request,
        input.logLevel,
        input.userID ?? null,
      )
    : normalizeLegacyQueryRequestArgs(
        wrapQueryRequestHandler(input.handler),
        input.schema,
        input.body,
        input.logLevel,
        input.userID ?? null,
      );
}

function normalizeLegacyQueryRequestArgs<S extends Schema>(
  handler: LegacyQueryRequestHandler,
  schema: S | undefined,
  requestOrJsonBody: Request | ReadonlyJSONValue | undefined,
  logLevel: LogLevel | undefined,
  userID?: string | null,
): NormalizedQueryRequestArgs<S> {
  assert(
    typeof schema !== 'undefined',
    'Schema must be provided when using handleQueryRequest',
  );

  if (requestOrJsonBody instanceof Request) {
    return {
      type: 'request',
      handler,
      schema,
      request: requestOrJsonBody,
      userID,
      logLevel: logLevel ?? 'info',
    };
  }

  assert(
    typeof requestOrJsonBody !== 'undefined',
    'JSON body cannot be undefined',
  );

  return {
    type: 'body',
    handler,
    schema,
    jsonBody: requestOrJsonBody,
    userID,
    logLevel: logLevel ?? 'info',
  };
}

async function transform<S extends Schema>(
  args: NormalizedQueryRequestArgs<S>,
  apiName: 'query' | 'transform' | 'getQueries',
): Promise<QueryResponse> {
  const lc = createLogContext(args.logLevel).withContext('TransformRequest');
  let parsed: TransformRequestMessage;
  let queryIDs: string[] = [];
  try {
    let body: ReadonlyJSONValue;
    if (args.type === 'request') {
      body = await args.request.json();
    } else {
      body = args.jsonBody;
    }

    parsed = v.parse(body, transformRequestMessageSchema);

    queryIDs = parsed[1].map(r => r.id);
  } catch (error) {
    lc.error?.(`Failed to parse ${apiName} request`, error);

    const message = `Failed to parse ${apiName} request: ${getErrorMessage(error)}`;
    const details = getErrorDetails(error);

    return {
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Parse,
      message,
      queryIDs,
      ...(details ? {details} : {}),
    };
  }

  try {
    const nameMapper = clientToServer(args.schema.tables);

    const responses: TransformResponseBody = await Promise.all(
      parsed[1].map(async req => {
        let finalQuery: AnyQuery;
        try {
          const result = await args.handler(req.name, req.args);
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

    return {
      kind: 'QueryResponse',
      queries: responses,
      ...(typeof args.userID !== 'undefined' ? {userID: args.userID} : {}),
    } as const satisfies QueryResponse;
  } catch (e) {
    const message = getErrorMessage(e);
    const details = getErrorDetails(e);

    return {
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Internal,
      message,
      queryIDs,
      ...(details ? {details} : {}),
    };
  }
}

/**
 * A function that transforms a query by name and arguments into a Query object.
 *
 * @param name - The name of the query (can be dot-separated for nested queries)
 * @param args - The arguments to pass to the query (can be undefined)
 * @returns A Query object
 */
export type QueryRequestHandler = (
  name: string,
  args: ReadonlyJSONValue | undefined,
) => AnyQuery;

/** @deprecated Use `QueryRequestHandler` instead. */
export type TransformQueryFunction = QueryRequestHandler;

export type LegacyQueryRequestHandler = (
  name: string,
  args: readonly ReadonlyJSONValue[],
) => MaybePromise<{query: AnyQuery} | AnyQuery>;

function wrapQueryRequestHandler(
  handler: QueryRequestHandler,
): LegacyQueryRequestHandler {
  return (name, args) => handler(name, args[0]);
}
