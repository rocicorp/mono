import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {upstreamSchema, type ShardID} from '../types/shards.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {ErrorForClient} from '../types/error-for-client.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';

const reservedParams = ['schema', 'appID'];

/**
 * Escapes special regex characters in a string to treat it as a literal.
 */
function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Checks if a string is a regex pattern (wrapped in forward slashes).
 */
export function isRegexPattern(str: string): boolean {
  return str.startsWith('/') && str.endsWith('/') && str.length > 2;
}

/**
 * Validates that a URL is not a regex pattern and throws if it is.
 * Regex patterns are meant for validation only, not for making HTTP requests.
 *
 * @param url - The URL to validate
 * @param envVarName - The environment variable name for error messages
 * @throws Error if the URL is a regex pattern
 */
export function assertNotRegexPattern(url: string, envVarName: string): void {
  if (isRegexPattern(url)) {
    throw new Error(
      `Cannot use regex pattern as default URL for ${envVarName}. Regex patterns (wrapped in /.../) are for validation only. ` +
        `Please either: (1) provide a URL via the client connection, or (2) configure a literal URL (not wrapped in /.../) as the first entry in ${envVarName}.`,
    );
  }
}

/**
 * Compiles and validates URL patterns from configuration.
 * Supports both regex patterns (wrapped in /) and string literals.
 * Automatically anchors patterns with ^ and $ if not already present for security.
 *
 * Pattern syntax:
 * - Regex: /pattern/ - e.g., /http:\/\/(www|api)\.example\.com\/mutate/
 * - Literal: plain string - e.g., http://localhost:5173/api/mutate
 *
 * @throws Error if any pattern is an invalid regex
 */
export function compileUrlPatterns(
  lc: LogContext,
  patterns: string[],
): RegExp[] {
  const compiled: RegExp[] = [];

  for (const pattern of patterns) {
    let regexPattern: string;
    let isExplicitRegex = false;

    // Check if pattern is wrapped in forward slashes (regex syntax)
    if (
      pattern.startsWith('/') &&
      pattern.endsWith('/') &&
      pattern.length > 2
    ) {
      // Extract regex pattern (remove surrounding slashes)
      regexPattern = pattern.slice(1, -1);
      isExplicitRegex = true;
      lc.debug?.('Detected regex pattern', {pattern, extracted: regexPattern});
    } else {
      // Treat as string literal - escape all regex special characters
      regexPattern = escapeRegex(pattern);
      lc.debug?.('Treating as literal URL', {pattern, escaped: regexPattern});
    }

    // Auto-anchor if not already anchored (for security)
    let anchoredPattern = regexPattern;
    const needsStartAnchor = !regexPattern.startsWith('^');
    const needsEndAnchor = !regexPattern.endsWith('$');

    if (needsStartAnchor || needsEndAnchor) {
      if (needsStartAnchor) {
        anchoredPattern = '^' + anchoredPattern;
      }
      if (needsEndAnchor) {
        anchoredPattern = anchoredPattern + '$';
      }

      lc.info?.(
        isExplicitRegex
          ? 'Auto-anchored regex pattern for security'
          : 'Auto-anchored literal URL pattern for security',
        {
          original: pattern,
          anchored: anchoredPattern,
        },
      );
    }

    try {
      compiled.push(new RegExp(anchoredPattern));
    } catch (e) {
      throw new Error(
        `Invalid ${isExplicitRegex ? 'regex' : 'URL'} pattern in URL configuration: "${pattern}". Error: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  return compiled;
}

export type HeaderOptions = {
  apiKey?: string | undefined;
  token?: string | undefined;
  cookie?: string | undefined;
};

export async function fetchFromAPIServer(
  lc: LogContext,
  url: string,
  allowedUrlPatterns: RegExp[],
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
 * Query parameters and hash fragments are ignored when matching.
 *
 * Example regex patterns:
 * - "^https://api\\.example\\.com/endpoint$" - Exact match for a specific URL
 * - "^https://[^.]+\\.example\\.com/endpoint$" - Matches any single subdomain (e.g., "https://api.example.com/endpoint")
 * - "^https://[^.]+\\.[^.]+\\.example\\.com/endpoint$" - Matches two subdomains (e.g., "https://api.v1.example.com/endpoint")
 * - "^https://(api|www)\\.example\\.com/" - Matches specific subdomains
 * - "^https://api\\.v\\d+\\.example\\.com/" - Matches versioned subdomains (e.g., "https://api.v1.example.com", "https://api.v2.example.com")
 */
export function urlMatch(url: string, allowedUrlPatterns: RegExp[]): boolean {
  // ignore query parameters and hash in the URL using proper URL parsing
  const urlObj = new URL(url);
  let urlWithoutQuery = urlObj.origin + urlObj.pathname;

  // Normalize: remove trailing slash
  // This ensures 'http://example.com' and 'http://example.com/' are treated as equivalent
  if (urlWithoutQuery.endsWith('/')) {
    urlWithoutQuery = urlWithoutQuery.slice(0, -1);
  }

  for (const pattern of allowedUrlPatterns) {
    if (pattern.test(urlWithoutQuery)) {
      return true;
    }
  }
  return false;
}
