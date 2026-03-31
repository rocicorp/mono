import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {TimedCache} from '../../../shared/src/cache.ts';
import {getErrorMessage} from '../../../shared/src/error.ts';
import {must} from '../../../shared/src/must.ts';
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
import {
  compileUrlPattern,
  fetchFromAPIServer,
  type HeaderOptions,
} from '../custom/fetch.ts';
import type {CustomQueryRecord} from '../services/view-syncer/schema/types.ts';
import type {ShardID} from '../types/shards.ts';

type PrincipalID = string | null | undefined;

export type CustomQueryValidationResult = {
  readonly principalID: PrincipalID;
};

export type CustomQueryTransformResult = {
  readonly queries: readonly (TransformedAndHashed | ErroredQuery)[];
  readonly principalID: PrincipalID;
};

type CachedTransform = {
  readonly transformed: TransformedAndHashed;
  readonly principalID: PrincipalID;
};

type FetchedTransform = {
  readonly queries: TransformResponseBody;
  readonly principalID: PrincipalID;
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
 * identity used for `/query`: auth, forwarded cookies, origin, custom
 * headers, target URL, and the query hash itself.
 */
export class CustomQueryTransformer {
  readonly #shard: ShardID;
  readonly #cache: TimedCache<CachedTransform>;
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

  async validate(
    headerOptions: HeaderOptions,
    userQueryURL: string | undefined,
  ): Promise<CustomQueryValidationResult | TransformFailedBody> {
    const result = await this.#fetchTransform(
      normalizeHeaderOptions(headerOptions, this.#config.forwardCookies),
      [],
      userQueryURL,
    );
    if ('kind' in result) {
      return result;
    }
    return {principalID: result.principalID};
  }

  async transformAndValidate(
    headerOptions: HeaderOptions,
    queries: Iterable<CustomQueryRecord>,
    userQueryURL: string | undefined,
  ): Promise<CustomQueryTransformResult | TransformFailedBody> {
    const normalizedHeaderOptions = normalizeHeaderOptions(
      headerOptions,
      this.#config.forwardCookies,
    );
    const effectiveQueryURL = userQueryURL ?? this.#config.url[0];
    const request: TransformRequestBody = [];
    const cachedResponses: TransformedAndHashed[] = [];
    let principalID: PrincipalID = undefined;

    for (const query of queries) {
      const cacheKey = getCacheKey(
        normalizedHeaderOptions,
        query.id,
        effectiveQueryURL,
      );
      const cached = this.#cache.get(cacheKey);
      if (cached) {
        cachedResponses.push(cached.transformed);
        principalID = mergePrincipalID(principalID, cached.principalID);
      } else {
        request.push({
          id: query.id,
          name: query.name,
          args: query.args,
        });
      }
    }

    if (request.length === 0) {
      return {
        queries: cachedResponses,
        principalID,
      };
    }

    const transformResponse = await this.#fetchTransform(
      normalizedHeaderOptions,
      request,
      userQueryURL,
    );
    if ('kind' in transformResponse) {
      return transformResponse;
    }

    principalID = mergePrincipalID(principalID, transformResponse.principalID);

    const newResponses = transformResponse.queries.map(transformed => {
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
        continue;
      }
      const cacheKey = getCacheKey(
        normalizedHeaderOptions,
        transformed.id,
        effectiveQueryURL,
      );
      this.#cache.set(cacheKey, {
        transformed,
        principalID: transformResponse.principalID,
      });
    }

    return {
      queries: newResponses.concat(cachedResponses),
      principalID,
    };
  }

  /**
   * Convenience wrapper for callers that only need the transformed query list
   * and do not consume validation metadata.
   */
  async transform(
    headerOptions: HeaderOptions,
    queries: Iterable<CustomQueryRecord>,
    userQueryURL: string | undefined,
  ): Promise<(TransformedAndHashed | ErroredQuery)[] | TransformFailedBody> {
    const result = await this.transformAndValidate(
      headerOptions,
      queries,
      userQueryURL,
    );
    if ('kind' in result) {
      return result;
    }
    return [...result.queries];
  }

  async #fetchTransform(
    headerOptions: HeaderOptions,
    request: TransformRequestBody,
    userQueryURL: string | undefined,
  ): Promise<FetchedTransform | TransformFailedBody> {
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

      return {
        queries: transformResponse[1],
        principalID: transformResponse[2]?.principalID,
      };
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

function normalizeHeaderOptions(
  headerOptions: HeaderOptions,
  forwardCookies: boolean,
) {
  if (!forwardCookies && headerOptions.cookie) {
    return {
      ...headerOptions,
      cookie: undefined,
    };
  }
  return headerOptions;
}

function mergePrincipalID(
  current: PrincipalID,
  next: PrincipalID,
): PrincipalID {
  if (current === undefined) {
    return next;
  }
  assert(
    next === undefined || current === next,
    () =>
      `conflicting principalID metadata from custom query responses: ${current} !== ${next}`,
  );
  return current;
}

function getCacheKey(
  headerOptions: HeaderOptions,
  queryID: string,
  effectiveQueryURL: string | undefined,
) {
  // For custom queries, queryID is a hash of the name + args.
  // The apiKey is static for a given transformer instance.
  return JSON.stringify({
    queryID,
    token: headerOptions.token,
    cookie: headerOptions.cookie,
    origin: headerOptions.origin,
    url: effectiveQueryURL,
    customHeaders: normalizedForwardedHeaders(headerOptions),
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
  const forwardedHeaders = Object.entries(customHeaders)
    .filter(([header]) => allowedHeaders.has(header.toLowerCase()))
    .sort((left, right) => left[0].localeCompare(right[0]));

  return forwardedHeaders.length === 0
    ? undefined
    : JSON.stringify(forwardedHeaders);
}
