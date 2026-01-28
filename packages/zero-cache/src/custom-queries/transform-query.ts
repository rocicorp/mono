import type {LogContext} from '@rocicorp/logger';
import {TimedCache} from '../../../shared/src/cache.ts';
import {getErrorMessage} from '../../../shared/src/error.ts';
import {must} from '../../../shared/src/must.ts';
import {
  transformResponseMessageSchema,
  type ErroredQuery,
  type TransformRequestBody,
  type TransformRequestMessage,
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
import {
  compileUrlPattern,
  fetchFromAPIServer,
  type HeaderOptions,
} from '../custom/fetch.ts';
import type {CustomQueryRecord} from '../services/view-syncer/schema/types.ts';
import type {ShardID} from '../types/shards.ts';

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
 * Caching is safe here because the cache key encodes both
 * the user's cookies and auth token. A user cannot see another user's
 * transformed queries unless they share the same token and cookies.
 */
export class CustomQueryTransformer {
  readonly #shard: ShardID;
  readonly #cache: TimedCache<TransformedAndHashed>;
  readonly #config: {
    url: string[];
    forwardCookies: boolean;
  };
  readonly #urlPatterns: URLPattern[];
  readonly #lc: LogContext;

  constructor(
    lc: LogContext,
    config: {
      url: string[];
      forwardCookies: boolean;
    },
    shard: ShardID,
  ) {
    this.#config = config;
    this.#shard = shard;
    this.#lc = lc;
    this.#urlPatterns = config.url.map(compileUrlPattern);
    this.#cache = new TimedCache(5000); // 5 seconds cache TTL
  }

  async transform(
    headerOptions: HeaderOptions,
    queries: Iterable<CustomQueryRecord>,
    userQueryURL: string | undefined,
  ): Promise<(TransformedAndHashed | ErroredQuery)[] | TransformFailedBody> {
    const request: TransformRequestBody = [];
    const cachedResponses: TransformedAndHashed[] = [];

    if (!this.#config.forwardCookies && headerOptions.cookie) {
      headerOptions = {
        ...headerOptions,
        cookie: undefined, // remove cookies if not forwarded
      };
    }

    // split queries into cached and uncached
    for (const query of queries) {
      const cacheKey = getCacheKey(headerOptions, query.id);
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

    if (request.length === 0) {
      return cachedResponses;
    }

    const queryIDs = request.map(r => r.id);

    try {
      const transformResponse = await fetchFromAPIServer(
        transformResponseMessageSchema,
        'transform',
        this.#lc,
        userQueryURL ??
          must(
            this.#config.url[0],
            'A ZERO_QUERY_URL must be configured for custom queries',
          ),
        this.#urlPatterns,
        this.#shard,
        headerOptions,
        ['transform', request] satisfies TransformRequestMessage,
      );

      if (transformResponse[0] === 'transformFailed') {
        return transformResponse[1];
      }

      const newResponses = transformResponse[1].map(transformed => {
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
        const cacheKey = getCacheKey(headerOptions, transformed.id);
        this.#cache.set(cacheKey, transformed);
      }

      return newResponses.concat(cachedResponses);
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
        message: `Failed to transform queries: ${getErrorMessage(e)}`,
        queryIDs,
      } as const satisfies TransformFailedBody;
    }
  }
}

function getCacheKey(headerOptions: HeaderOptions, queryID: string) {
  // For custom queries, queryID is a hash of the name + args.
  // the APIKey from headerOptions is static. Not needed for the cache key.
  // The token is used to identify the user and should be included in the cache key.
  return `${headerOptions.token}:${headerOptions.cookie}:${queryID}`;
}
