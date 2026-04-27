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
    cb,
    schema,
    undefined,
    requestOrJsonBody,
    'getQueries',
    logLevel,
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
    cb,
    schema,
    undefined,
    requestOrJsonBody,
    'transform',
    logLevel,
  );
}

type UserID = string | null | undefined;
type QueryString = URLSearchParams | Record<string, string>;

type HandleQueryRequestArgs = [
  userIDOrQueryStringOrRequestOrJsonBody:
    | UserID
    | Request
    | QueryString
    | ReadonlyJSONValue,
  queryStringOrRequestOrJsonBodyOrLogLevel?:
    | QueryString
    | Request
    | ReadonlyJSONValue
    | LogLevel,
  requestOrJsonBodyOrLogLevel?: Request | ReadonlyJSONValue | LogLevel,
  logLevel?: LogLevel,
];

/**
 * Process a `/query` request and include the authenticated user in the success
 * response.
 *
 * @param transformQuery - Runs once per requested query with the query name
 * and first JSON argument. Returns a `Query`.
 * @param schema - Schema used when building the returned ASTs.
 * @param userID - User ID included in successful responses. Pass `null` or
 * `undefined` when unauthenticated.
 * @param requestOrJsonBody - A Fetch `Request`, or a parsed JSON body.
 * @param logLevel - Log level for request processing. Defaults to `'info'`.
 * @returns A `QueryResponse`. Success returns `userID: userID ?? null` and
 * one result per query. Per-query errors stay in `queries`; malformed
 * requests return `TransformFailed`.
 */
export function handleQueryRequest<S extends Schema>(
  transformQuery: TransformQueryFunction,
  schema: S,
  userID: string | null | undefined,
  requestOrJsonBody: Request | ReadonlyJSONValue,
  logLevel?: LogLevel,
): Promise<QueryResponse>;

/**
 * Process a `/query` request from parsed query parameters and a parsed JSON
 * body, and include the authenticated user in the success response.
 *
 * @param transformQuery - Runs once per requested query with the query name
 * and first JSON argument. Returns a `Query`.
 * @param schema - Schema used when building the returned ASTs.
 * @param userID - User ID included in successful responses. Pass `null` or
 * `undefined` when unauthenticated.
 * @param queryString - Parsed query params. Accepted for symmetry and
 * currently unused.
 * @param body - Parsed JSON body.
 * @param logLevel - Log level for request processing. Defaults to `'info'`.
 * @returns A `QueryResponse`. Success returns `userID: userID ?? null` and
 * one result per query. Per-query errors stay in `queries`; malformed
 * requests return `TransformFailed`.
 */
export function handleQueryRequest<S extends Schema>(
  transformQuery: TransformQueryFunction,
  schema: S,
  userID: string | null | undefined,
  queryString: URLSearchParams | Record<string, string>,
  body: ReadonlyJSONValue,
  logLevel?: LogLevel,
): Promise<QueryResponse>;

/**
 * Process a `/query` request from a Fetch `Request` or parsed JSON body.
 *
 * @param transformQuery - Runs once per requested query with the query name
 * and first JSON argument. Returns a `Query`.
 * @param schema - Schema used when building the returned ASTs.
 * @param requestOrJsonBody - A Fetch `Request`, or a parsed JSON body.
 * @param logLevel - Log level for request processing. Defaults to `'info'`.
 * @returns A `QueryResponse`. Success returns `userID: null` and one result
 * per query. Per-query errors stay in `queries`; malformed requests return
 * `TransformFailed`.
 */
export function handleQueryRequest<S extends Schema>(
  transformQuery: TransformQueryFunction,
  schema: S,
  requestOrJsonBody: Request | ReadonlyJSONValue,
  logLevel?: LogLevel,
): Promise<QueryResponse>;

/**
 * Process a `/query` request from parsed query parameters and a parsed JSON
 * body.
 *
 * @param transformQuery - Runs once per requested query with the query name
 * and first JSON argument. Returns a `Query`.
 * @param schema - Schema used when building the returned ASTs.
 * @param queryString - Parsed query params. Accepted for symmetry and
 * currently unused.
 * @param body - Parsed JSON body.
 * @param logLevel - Log level for request processing. Defaults to `'info'`.
 * @returns A `QueryResponse`. Success returns `userID: null` and one result
 * per query. Per-query errors stay in `queries`; malformed requests return
 * `TransformFailed`.
 */
export function handleQueryRequest<S extends Schema>(
  transformQuery: TransformQueryFunction,
  schema: S,
  queryString: URLSearchParams | Record<string, string>,
  body: ReadonlyJSONValue,
  logLevel?: LogLevel,
): Promise<QueryResponse>;

export function handleQueryRequest<S extends Schema>(
  transformQuery: TransformQueryFunction,
  schema: S,
  ...args: HandleQueryRequestArgs
): Promise<QueryResponse> {
  const normalized = normalizeQueryRequestArgs(args);
  const requestOrJsonBody =
    normalized.source === 'request' ? normalized.request : normalized.jsonBody;

  return transform(
    (name, argsArray) => transformQuery(name, argsArray[0]),
    schema,
    normalized.userID,
    requestOrJsonBody,
    'query',
    normalized.logLevel,
  );
}

type NormalizedQueryRequestArgs =
  | {
      readonly source: 'request';
      readonly userID: UserID;
      readonly queryString: URLSearchParams;
      readonly request: Request;
      readonly logLevel: LogLevel;
    }
  | {
      readonly source: 'parsed';
      readonly userID: UserID;
      readonly queryString: QueryString | undefined;
      readonly jsonBody: ReadonlyJSONValue;
      readonly logLevel: LogLevel;
    };

export function normalizeQueryRequestArgs(
  args: HandleQueryRequestArgs,
): NormalizedQueryRequestArgs {
  const [firstArg, secondArg, thirdArg, fourthArg] = args;

  if (firstArg instanceof Request) {
    return {
      source: 'request',
      userID: undefined,
      queryString: new URL(firstArg.url).searchParams,
      request: firstArg,
      logLevel: (secondArg as LogLevel | undefined) ?? 'info',
    };
  }

  if (isUserID(firstArg)) {
    if (secondArg instanceof Request) {
      return {
        source: 'request',
        userID: firstArg ?? null,
        queryString: new URL(secondArg.url).searchParams,
        request: secondArg,
        logLevel: (thirdArg as LogLevel | undefined) ?? 'info',
      };
    }

    if (isQueryString(secondArg)) {
      return {
        source: 'parsed',
        userID: firstArg ?? null,
        queryString: secondArg,
        jsonBody: thirdArg as ReadonlyJSONValue,
        logLevel: (fourthArg as LogLevel | undefined) ?? 'info',
      };
    }

    return {
      source: 'parsed',
      userID: firstArg ?? null,
      queryString: undefined,
      jsonBody: secondArg as ReadonlyJSONValue,
      logLevel: (thirdArg as LogLevel | undefined) ?? 'info',
    };
  }

  if (isQueryString(firstArg)) {
    return {
      source: 'parsed',
      userID: undefined,
      queryString: firstArg,
      jsonBody: secondArg as ReadonlyJSONValue,
      logLevel: (thirdArg as LogLevel | undefined) ?? 'info',
    };
  }

  return {
    source: 'parsed',
    userID: undefined,
    queryString: undefined,
    jsonBody: firstArg as ReadonlyJSONValue,
    logLevel: (secondArg as LogLevel | undefined) ?? 'info',
  };
}

function isUserID(value: unknown): value is UserID {
  return value === null || value === undefined || typeof value === 'string';
}

function isQueryString(value: unknown): value is QueryString {
  if (value instanceof URLSearchParams) {
    return true;
  }

  if (
    value instanceof Request ||
    typeof value !== 'object' ||
    value === null ||
    Array.isArray(value)
  ) {
    return false;
  }

  return Object.values(value).every(v => typeof v === 'string');
}

async function transform<S extends Schema>(
  cb: (
    name: string,
    args: readonly ReadonlyJSONValue[],
  ) => MaybePromise<{query: AnyQuery} | AnyQuery>,
  schema: S,
  userID: string | null | undefined,
  requestOrJsonBody: Request | ReadonlyJSONValue,
  apiName: 'query' | 'getQueries' | 'transform',
  logLevel: LogLevel = 'info',
): Promise<QueryResponse> {
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
    const nameMapper = clientToServer(schema.tables);

    const responses: TransformResponseBody = await Promise.all(
      parsed[1].map(async req => {
        let finalQuery: AnyQuery;
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

    return {
      kind: 'QueryResponse',
      queries: responses,
      ...(typeof userID !== 'undefined' ? {userID} : {}),
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
export type TransformQueryFunction = (
  name: string,
  args: ReadonlyJSONValue | undefined,
) => AnyQuery;
