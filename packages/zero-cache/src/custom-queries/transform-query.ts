import type {LogContext} from '@rocicorp/logger';
import type {TransformedAndHashed} from '../auth/read-authorizer.ts';
import type {CustomQueryRecord} from '../services/view-syncer/schema/types.ts';
import {
  transformResponseMessageSchema,
  type ErroredQuery,
  type TransformRequestBody,
  type TransformRequestMessage,
} from '../../../zero-protocol/src/custom-queries.ts';
import {fetchFromAPIServer, type HeaderOptions} from '../custom/fetch.ts';
import type {ShardID} from '../types/shards.ts';
import * as v from '../../../shared/src/valita.ts';
import {hashOfAST} from '../../../zero-protocol/src/query-hash.ts';
import {TimedCache} from '../../../shared/src/cache.ts';
import {must} from '../../../shared/src/must.ts';

export type HttpError = {
  error: 'http';
  status: number;
  details: string;
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
 */
export class CustomQueryTransformer {
  readonly #shard: ShardID;
  readonly #cache: TimedCache<TransformedAndHashed>;
  readonly #config: {
    url: string[];
    forwardCookies: boolean;
  };
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
    this.#cache = new TimedCache(5000); // 5 seconds cache TTL
  }

  async transform(
    headerOptions: HeaderOptions,
    queries: Iterable<CustomQueryRecord>,
    userQueryURL: string | undefined,
  ): Promise<(TransformedAndHashed | ErroredQuery)[]> {
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

    let response: Response | undefined;
    try {
      response = await fetchFromAPIServer(
        this.#lc,
        userQueryURL ??
          must(
            this.#config.url[0],
            'A ZERO_GET_QUERIES_URL must be configured for custom queries',
          ),
        this.#config.url,
        this.#shard,
        headerOptions,
        ['transform', request] satisfies TransformRequestMessage,
      );
    } catch (e) {
      return request.map(r => ({
        error: 'zero',
        details: e instanceof Error ? e.message : String(e),
        id: r.id,
        name: r.name,
      }));
    }

    if (!response.ok) {
      const details = await response.text();
      return request.map(r => ({
        error: 'http',
        status: response.status,
        details,
        id: r.id,
        name: r.name,
      }));
    }

    const body = await response.json();
    const msg = v.parse(body, transformResponseMessageSchema);

    const newResponses = msg[1].map(transformed => {
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
  }
}

function getCacheKey(headerOptions: HeaderOptions, queryID: string) {
  // For custom queries, queryID is a hash of the name + args.
  // the APIKey from headerOptions is static. Not needed for the cache key.
  // The token is used to identify the user and should be included in the cache key.
  return `${headerOptions.token}:${headerOptions.cookie}:${queryID}`;
}
