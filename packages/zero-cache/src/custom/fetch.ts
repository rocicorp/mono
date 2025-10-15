/// <reference path="../types/urlpattern.d.ts" />
import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {upstreamSchema, type ShardID} from '../types/shards.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {ErrorForClient} from '../types/error-for-client.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';

const reservedParams = ['schema', 'appID'];

/**
 * Compiles and validates URLPattern objects from configuration.
 *
 * @throws Error if any pattern is an invalid URLPattern
 */
export function compileUrlPatterns(patterns: string[]): URLPattern[] {
  const compiled: URLPattern[] = [];

  for (const pattern of patterns) {
    try {
      compiled.push(
        new URLPattern({pathname: '*', ...parseUrlPattern(pattern)}),
      );
    } catch (e) {
      throw new Error(
        `Invalid URLPattern in URL configuration: "${pattern}". Error: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  return compiled;
}

/**
 * Parses a URL pattern string into URLPattern init object.
 * Supports both full URLs and path-only patterns.
 */
function parseUrlPattern(pattern: string): URLPatternInit {
  // If pattern looks like a full URL (has protocol), parse it
  if (pattern.includes('://')) {
    try {
      const url = new URL(pattern);
      return {
        protocol: url.protocol.slice(0, -1), // Remove trailing ':'
        hostname: url.hostname,
        pathname: url.pathname === '/' ? '*' : url.pathname,
        search: '*',
        hash: '*',
      };
    } catch {
      // If URL parsing fails, treat as pattern string
      return {pathname: pattern};
    }
  }

  // Otherwise treat as a pathname pattern
  return {pathname: pattern};
}

export type HeaderOptions = {
  apiKey?: string | undefined;
  token?: string | undefined;
  cookie?: string | undefined;
};

export async function fetchFromAPIServer(
  lc: LogContext,
  url: string,
  allowedUrlPatterns: URLPattern[],
  shard: ShardID,
  headerOptions: HeaderOptions,
  body: ReadonlyJSONValue,
) {
  lc.info?.('fetchFromAPIServer called', {
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
  const response = await fetch(finalUrl, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    // Zero currently handles all auth errors this way (throws ErrorForClient).
    // Continue doing that until we have an `onError` callback exposed on the top level Zero instance.
    // This:
    // 1. Keeps the API the same for those migrating to custom mutators from CRUD
    // 2. Ensures we only churn the API once, when we have `onError` available.
    //
    // When switching to `onError`, we should stop disconnecting the websocket
    // on auth errors and instead let the token be updated
    // on the existing WS connection. This will give us the chance to skip
    // re-hydrating queries that do not use the modified fields of the token.
    if (response.status === 401) {
      throw new ErrorForClient({
        kind: ErrorKind.AuthInvalidated,
        message: await response.text(),
      });
    }
  }

  return response;
}

/**
 * Returns true if the url matches one of the allowedUrlPatterns.
 *
 * Query parameters and hash fragments are automatically ignored by URLPattern matching.
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
