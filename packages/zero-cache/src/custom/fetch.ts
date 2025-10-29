import 'urlpattern-polyfill';
import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {upstreamSchema, type ShardID} from '../types/shards.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {Type} from '../../../shared/src/valita.ts';
import {
  isProtocolError,
  ProtocolError,
} from '../../../zero-protocol/src/error.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';

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
    lc.error?.('failed to get body preview', {
      url: res.url,
      error: e instanceof Error ? e.message : String(e),
    });
  }

  return undefined;
};

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
  lc.debug?.('fetchFromAPIServer called', {
    url,
  });

  if (!urlMatch(url, allowedUrlPatterns)) {
    throw new Error(
      `URL "${url}" is not allowed by the ZERO_MUTATE/GET_QUERIES_URL configuration`,
    );
  }
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  };

  if (headerOptions.apiKey !== undefined) {
    headers['X-Api-Key'] = headerOptions.apiKey;
  }
  if (headerOptions.token !== undefined) {
    headers['Authorization'] = `Bearer ${headerOptions.token}`;
  }
  if (headerOptions.cookie !== undefined) {
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

  try {
    const response = await fetch(finalUrl, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const bodyPreview = await getBodyPreview(response, lc);

      lc.info?.('fetch from API server returned non-OK status', {
        url: finalUrl,
        status: response.status,
        bodyPreview,
      });

      throw new ProtocolError(
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
      );
    }

    try {
      const json = await response.json();

      return validator.parse(json);
    } catch (error) {
      lc.error?.('failed to parse response', {
        url: finalUrl,
        error,
      });

      throw new ProtocolError(
        source === 'push'
          ? {
              kind: ErrorKind.PushFailed,
              origin: ErrorOrigin.ZeroCache,
              reason: ErrorReason.Parse,
              message: `Failed to parse response from API server: ${error instanceof Error ? error.message : String(error)}`,
              mutationIDs: [],
            }
          : {
              kind: ErrorKind.TransformFailed,
              origin: ErrorOrigin.ZeroCache,
              reason: ErrorReason.Parse,
              message: `Failed to parse response from API server: ${error instanceof Error ? error.message : String(error)}`,
              queryIDs: [],
            },
        {cause: error},
      );
    }
  } catch (error) {
    if (isProtocolError(error)) {
      throw error;
    }

    lc.error?.('failed to fetch from API server with unknown error', {
      url: finalUrl,
      error,
    });

    throw new ProtocolError(
      source === 'push'
        ? {
            kind: ErrorKind.PushFailed,
            origin: ErrorOrigin.ZeroCache,
            reason: ErrorReason.Internal,
            message: `Fetch from API server failed with unknown error: ${error instanceof Error ? error.message : String(error)}`,
            mutationIDs: [],
          }
        : {
            kind: ErrorKind.TransformFailed,
            origin: ErrorOrigin.ZeroCache,
            reason: ErrorReason.Internal,
            message: `Fetch from API server failed with unknown error: ${error instanceof Error ? error.message : String(error)}`,
            queryIDs: [],
          },
      {cause: error},
    );
  }
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
