import {trace} from '@opentelemetry/api';
import type {LogContext} from '@rocicorp/logger';
import {startAsyncSpan} from '../../../otel/src/span.ts';
import {TimedCache} from '../../../shared/src/cache.ts';
import {getErrorMessage} from '../../../shared/src/error.ts';
import {sortedEntries} from '../../../shared/src/sorted-entries.ts';
import {
  transformResponseMessageSchema,
  type ErroredQuery,
  type TransformRequestBody,
  type TransformRequestMessage,
  type TransformResponseBody,
} from '../../../zero-protocol/src/custom-queries.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import {
  isProtocolError,
  type TransformFailedBody,
} from '../../../zero-protocol/src/error.ts';
import {hashOfAST} from '../../../zero-protocol/src/query-hash.ts';
import type {TransformedAndHashed} from '../auth/read-authorizer.ts';
import {fetchFromAPIServer} from '../custom/fetch.ts';
import type {
  ConnectionContext,
  HeaderOptions,
} from '../services/view-syncer/connection-context-manager.ts';
import type {CustomQueryRecord} from '../services/view-syncer/schema/types.ts';
import type {ShardID} from '../types/shards.ts';

const tracer = trace.getTracer('custom-query-transformer');

type TransformResponse = TransformResponseBody | TransformFailedBody;
export type TransformAttempt = {
  result: (TransformedAndHashed | ErroredQuery)[] | TransformFailedBody;
  cached: boolean;
};

/**
 * Transforms a custom query by calling the user's API server.
 * Caches the transformed queries for 5 seconds to avoid unnecessary API calls.
 *
 * Error responses are not cached as the user may want to retry the query
 * and the error may be transient.
 *
 * The TTL was chosen to be 5 seconds since custom query requests come with
 * a token which itself may have a short TTL (e.g., 10 seconds).
 *
 * Token expiration isn't expected to be exact so this 5 second
 * caching shouldn't cause unexpected behavior. E.g., many JWT libraries
 * implement leeway for expiration checks: https://github.com/panva/jose/blob/main/docs/jwt/verify/interfaces/JWTVerifyOptions.md#clocktolerance
 *
 * The ViewSyncer will call the API server 3-4 times with the exact same queries
 * if we do not cache requests.
 *
 * Caching is safe here because the cache key encodes the effective request
 * identity used for `/query`: auth, userID, forwarded cookies, origin,
 * custom headers, target URL, and the query hash itself.
 */
export class CustomQueryTransformer {
  readonly #shard: ShardID;
  readonly #cache: TimedCache<TransformedAndHashed>;
  readonly #lc: LogContext;

  constructor(lc: LogContext, shard: ShardID) {
    this.#shard = shard;
    this.#lc = lc;
    this.#cache = new TimedCache(5000); // 5 seconds cache TTL
  }

  /**
   * Forces the empty `/query` validation request used by auth maintenance.
   *
   * This stays separate from `transform()` because `transform([], ...)`
   * short-circuits locally and never hits the API server, while validation
   * still needs to make the request so auth failures are surfaced.
   * Successful validation is intentionally opaque because callers only care
   * whether the request succeeded or failed.
   */
  async validate(
    ctx: ConnectionContext,
  ): Promise<TransformFailedBody | undefined> {
    const response = await this.#requestTransform(ctx, [], 'validate');

    return Array.isArray(response) ? undefined : response;
  }

  async transform(
    ctx: ConnectionContext,
    queries: Iterable<CustomQueryRecord>,
  ): Promise<TransformAttempt> {
    const request: TransformRequestBody = [];
    const cachedResponses: TransformedAndHashed[] = [];

    // split queries into cached and uncached
    for (const query of queries) {
      const cacheKey = getCacheKey(ctx, query.id);
      const cached = this.#cache.get(cacheKey);
      if (cached) {
        cachedResponses.push(cached);
      } else {
        request.push({
          id: query.id,
          name: query.name,
          args: query.args,
        });
      }
    }

    let cached = true;

    if (request.length === 0) {
      return {
        result: cachedResponses,
        cached: true,
      };
    } else {
      // we are hitting the server with at least one uncached query
      cached = false;
    }

    const response = await this.#requestTransform(ctx, request, 'transform');
    if (!Array.isArray(response)) {
      return {
        result: response,
        cached,
      };
    }

    const newResponses = response.map(transformed => {
      if ('error' in transformed) {
        return transformed;
      }
      return {
        id: transformed.id,
        transformedAst: transformed.ast,
        transformationHash: hashOfAST(transformed.ast),
      } satisfies TransformedAndHashed;
    });

    for (const transformed of newResponses) {
      if ('error' in transformed) {
        // do not cache error responses
        continue;
      }
      const cacheKey = getCacheKey(ctx, transformed.id);
      this.#cache.set(cacheKey, transformed);
    }

    return {
      result: [...newResponses, ...cachedResponses],
      cached,
    };
  }

  async #requestTransform(
    ctx: ConnectionContext,
    request: TransformRequestBody,
    operation: 'validate' | 'transform',
  ): Promise<TransformResponse> {
    const queryIDs = request.map(({id}) => id);

    try {
      const transformResponse = await startAsyncSpan(
        tracer,
        'customQueryTransformer.fetchFromAPIServer',
        () =>
          fetchFromAPIServer(
            transformResponseMessageSchema,
            'transform',
            this.#lc,
            ctx,
            this.#shard,
            ['transform', request] satisfies TransformRequestMessage,
          ),
      );

      return transformResponse[1];
    } catch (e) {
      if (
        isProtocolError(e) &&
        e.errorBody.kind === ErrorKind.TransformFailed
      ) {
        return {
          ...e.errorBody,
          queryIDs,
        } as const satisfies TransformFailedBody;
      }

      return {
        kind: ErrorKind.TransformFailed,
        origin: ErrorOrigin.ZeroCache,
        reason: ErrorReason.Internal,
        message: `Failed to ${operation} queries: ${getErrorMessage(e)}`,
        queryIDs,
      } as const satisfies TransformFailedBody;
    }
  }
}

function getCacheKey(ctx: ConnectionContext, queryID: string) {
  // For custom queries, queryID is a hash of the name + args.
  // The apiKey is static for a given transformer instance.
  return JSON.stringify({
    queryID,
    token: ctx.auth?.raw,
    cookie: ctx.queryContext.headerOptions.cookie,
    origin: ctx.queryContext.headerOptions.origin,
    userID: ctx.userID,
    url: ctx.queryContext.url,
    customHeaders: normalizedForwardedHeaders(ctx.queryContext.headerOptions),
  });
}

function normalizedForwardedHeaders(headerOptions: HeaderOptions) {
  const {allowedClientHeaders, customHeaders} = headerOptions;
  if (
    !customHeaders ||
    !allowedClientHeaders ||
    allowedClientHeaders.length === 0
  ) {
    return undefined;
  }

  const allowedHeaders = new Set(
    allowedClientHeaders.map(header => header.toLowerCase()),
  );
  const forwardedHeaders = sortedEntries(customHeaders).filter(([header]) =>
    allowedHeaders.has(header.toLowerCase()),
  );

  return forwardedHeaders.length === 0
    ? undefined
    : JSON.stringify(forwardedHeaders);
}
