import type {LogContext, LogLevel} from '@rocicorp/logger';
import 'urlpattern-polyfill';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {getErrorMessage} from '../../../shared/src/error.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import {type Type} from '../../../shared/src/valita.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import {isProtocolError} from '../../../zero-protocol/src/error.ts';
import {ProtocolErrorWithLevel} from '../types/error-with-level.ts';
import {upstreamSchema, type ShardID} from '../types/shards.ts';
import {randInt} from '../../../shared/src/rand.ts';

const reservedParams = ['schema', 'appID'];

/**
 * Compiles and validates a URLPattern from configuration.
 *
 * Patterns must be full URLs (e.g., "https://api.example.com/endpoint").
 * URLPattern automatically sets search and hash to wildcard ('*'),
 * which means query parameters and fragments are ignored during matching.
 *
 * @throws Error if the pattern is an invalid URLPattern
 */
export function compileUrlPattern(pattern: string): URLPattern {
  try {
    return new URLPattern(pattern);
  } catch (e) {
    throw new Error(
      `Invalid URLPattern in URL configuration: "${pattern}". Error: ${e instanceof Error ? e.message : String(e)}`,
    );
  }
}

export type HeaderOptions = {
  apiKey?: string | undefined;
  customHeaders?: Record<string, string> | undefined;
  token?: string | undefined;
  cookie?: string | undefined;
};

export const getBodyPreview = async (
  res: Response,
  lc: LogContext,
): Promise<string | undefined> => {
  try {
    const body = await res.clone().text();
    if (body.length > 512) {
      return body.slice(0, 512) + '...';
    }
    return body;
  } catch (e) {
    lc.warn?.(
      'failed to get body preview',
      {
        url: res.url,
      },
      e,
    );
  }

  return undefined;
};

const MAX_ATTEMPTS = 4;

export async function fetchFromAPIServer<TValidator extends Type>(
  validator: TValidator,
  source: 'push' | 'transform',
  lc: LogContext,
  url: string,
  allowedUrlPatterns: URLPattern[],
  shard: ShardID,
  headerOptions: HeaderOptions,
  body: ReadonlyJSONValue,
) {
  const fetchFromAPIServerID = randInt(1, Number.MAX_SAFE_INTEGER).toString(36);
  lc = lc.withContext('fetchFromAPIServerID', fetchFromAPIServerID);

  lc.debug?.('fetchFromAPIServer called', {
    url,
  });

  if (!urlMatch(url, allowedUrlPatterns)) {
    throw new ProtocolErrorWithLevel(
      source === 'push'
        ? {
            kind: ErrorKind.PushFailed,
            origin: ErrorOrigin.ZeroCache,
            reason: ErrorReason.Internal,
            message: `URL "${url}" is not allowed by the ZERO_MUTATE_URL configuration`,
            mutationIDs: [],
          }
        : {
            kind: ErrorKind.TransformFailed,
            origin: ErrorOrigin.ZeroCache,
            reason: ErrorReason.Internal,
            message: `URL "${url}" is not allowed by the ZERO_QUERY_URL configuration`,
            queryIDs: [],
          },
      'warn',
    );
  }
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  };

  if (headerOptions.apiKey) {
    headers['X-Api-Key'] = headerOptions.apiKey;
  }
  if (headerOptions.customHeaders) {
    Object.assign(headers, headerOptions.customHeaders);
  }
  if (headerOptions.token) {
    headers['Authorization'] = `Bearer ${headerOptions.token}`;
  }
  if (headerOptions.cookie) {
    headers['Cookie'] = headerOptions.cookie;
  }

  const urlObj = new URL(url);
  const params = new URLSearchParams(urlObj.search);

  for (const reserved of reservedParams) {
    assert(
      !params.has(reserved),
      `The push URL cannot contain the reserved query param "${reserved}"`,
    );
  }

  params.append('schema', upstreamSchema(shard));
  params.append('appID', shard.appID);

  urlObj.search = params.toString();

  const finalUrl = urlObj.toString();

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    lc = lc.withContext('fetchFromAPIServerAttempt', attempt);
    lc.debug?.('fetch from API server attempt');
    const shouldRetry = async () => {
      if (attempt < MAX_ATTEMPTS) {
        const delayMs = getBackoffDelayMs(attempt);
        lc.debug?.(`fetch from API server retrying in ${delayMs} ms`);
        await sleep(delayMs);
        return true;
      }
      lc.debug?.('fetch from API server reached max attempts, not retrying');
      return false;
    };
    try {
      const response = await fetch(finalUrl, {
        method: 'POST',
        headers,
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        const bodyPreview = await getBodyPreview(response, lc);
        lc.warn?.('fetch from API server returned non-OK status', {
          url: finalUrl,
          status: response.status,
          bodyPreview,
        });
        // Bad Gateway or Gateway Timeout indicate the server was not reached
        // We retry these if we have retries remaining.
        if (
          (response.status === 502 || response.status === 504) &&
          (await shouldRetry())
        ) {
          continue;
        }

        throw new ProtocolErrorWithLevel(
          source === 'push'
            ? {
                kind: ErrorKind.PushFailed,
                origin: ErrorOrigin.ZeroCache,
                reason: ErrorReason.HTTP,
                status: response.status,
                bodyPreview,
                message: `Fetch from API server returned non-OK status ${response.status}`,
                mutationIDs: [],
              }
            : {
                kind: ErrorKind.TransformFailed,
                origin: ErrorOrigin.ZeroCache,
                reason: ErrorReason.HTTP,
                status: response.status,
                bodyPreview,
                message: `Fetch from API server returned non-OK status ${response.status}`,
                queryIDs: [],
              },
          'warn',
        );
      }

      try {
        const json = await response.json();
        const result = validator.parse(json);
        lc.debug?.('fetch from API server succeeded');
        return result;
      } catch (error) {
        lc.warn?.(
          'failed to parse response',
          {
            url: finalUrl,
          },
          error,
        );

        throw new ProtocolErrorWithLevel(
          source === 'push'
            ? {
                kind: ErrorKind.PushFailed,
                origin: ErrorOrigin.ZeroCache,
                reason: ErrorReason.Parse,
                message: `Failed to parse response from API server: ${getErrorMessage(error)}`,
                mutationIDs: [],
              }
            : {
                kind: ErrorKind.TransformFailed,
                origin: ErrorOrigin.ZeroCache,
                reason: ErrorReason.Parse,
                message: `Failed to parse response from API server: ${getErrorMessage(error)}`,
                queryIDs: [],
              },
          'warn',
          {cause: error},
        );
      }
    } catch (error) {
      if (isProtocolError(error)) {
        throw error;
      }

      const isFetchFailed =
        error instanceof TypeError && error.message === 'fetch failed';
      // unexpected/unknown errors should be logged at 'error' level so they
      // are investigated
      let logLevel: LogLevel = isFetchFailed ? 'warn' : 'error';
      lc[logLevel]?.(
        'fetch from API server threw error',
        {url: finalUrl},
        error,
      );

      if (isFetchFailed && (await shouldRetry())) {
        continue;
      }

      throw new ProtocolErrorWithLevel(
        source === 'push'
          ? {
              kind: ErrorKind.PushFailed,
              origin: ErrorOrigin.ZeroCache,
              reason: ErrorReason.Internal,
              message: `Fetch from API server threw error: ${getErrorMessage(error)}`,
              mutationIDs: [],
            }
          : {
              kind: ErrorKind.TransformFailed,
              origin: ErrorOrigin.ZeroCache,
              reason: ErrorReason.Internal,
              message: `Fetch from API server threw error: ${getErrorMessage(error)}`,
              queryIDs: [],
            },
        logLevel,
        {cause: error},
      );
    }
  }
  unreachable();
}

/**
 * Returns true if the url matches one of the allowedUrlPatterns.
 *
 * URLPattern automatically ignores query parameters and hash fragments during matching
 * because it sets search and hash to wildcard ('*') by default.
 *
 * Example URLPattern patterns:
 * - "https://api.example.com/endpoint" - Exact match for a specific URL
 * - "https://*.example.com/endpoint" - Matches any single subdomain (e.g., "https://api.example.com/endpoint")
 * - "https://*.*.example.com/endpoint" - Matches two subdomains (e.g., "https://api.v1.example.com/endpoint")
 * - "https://api.example.com/*" - Matches any path under /
 * - "https://api.example.com/:version/endpoint" - Matches with named parameter (e.g., "https://api.example.com/v1/endpoint")
 */
export function urlMatch(
  url: string,
  allowedUrlPatterns: URLPattern[],
): boolean {
  for (const pattern of allowedUrlPatterns) {
    if (pattern.test(url)) {
      return true;
    }
  }
  return false;
}

/**
 * Returns the delay in milliseconds for the next retry attempt using exponential backoff with jitter.
 *
 * The delay assumes the first retry is attempt 1.
 * The formula is: `min(1000, 100 * 2^(attempt - 1) + jitter)` where jitter is between 0 and 100ms.
 */
function getBackoffDelayMs(attempt: number): number {
  return Math.min(1000, 100 * Math.pow(2, attempt - 1) + Math.random() * 100);
}
