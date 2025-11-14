import type {LogContext} from '@rocicorp/logger';
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
 * Always calls the API server to ensure proper authorization validation.
 */
export class CustomQueryTransformer {
  readonly #shard: ShardID;
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
  }

  async transform(
    headerOptions: HeaderOptions,
    queries: Iterable<CustomQueryRecord>,
    userQueryURL: string | undefined,
  ): Promise<(TransformedAndHashed | ErroredQuery)[] | TransformFailedBody> {
    if (!this.#config.forwardCookies && headerOptions.cookie) {
      headerOptions = {
        ...headerOptions,
        cookie: undefined, // remove cookies if not forwarded
      };
    }

    const request: TransformRequestBody = [];
    for (const query of queries) {
      request.push({
        id: query.id,
        name: query.name,
        args: query.args,
      });
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
            'A ZERO_GET_QUERIES_URL must be configured for custom queries',
          ),
        this.#urlPatterns,
        this.#shard,
        headerOptions,
        ['transform', request] satisfies TransformRequestMessage,
      );

      if (transformResponse[0] === 'transformFailed') {
        return transformResponse[1];
      }

      return transformResponse[1].map(transformed => {
        if ('error' in transformed) {
          return transformed;
        }
        return {
          id: transformed.id,
          transformedAst: transformed.ast,
          transformationHash: hashOfAST(transformed.ast),
        } satisfies TransformedAndHashed;
      });
    } catch (e) {
      this.#lc.error?.('failed to transform queries', e);

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
