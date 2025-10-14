import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {upstreamSchema, type ShardID} from '../types/shards.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {ErrorForClient} from '../types/error-for-client.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';

const reservedParams = ['schema', 'appID'];

// Cache for compiled regex patterns to avoid recompilation on every urlMatch call
const regexCache = new Map<string, RegExp>();

export type HeaderOptions = {
  apiKey?: string | undefined;
  token?: string | undefined;
  cookie?: string | undefined;
};

export async function fetchFromAPIServer(
  lc: LogContext,
  url: string,
  allowedUrls: string[],
  shard: ShardID,
  headerOptions: HeaderOptions,
  body: ReadonlyJSONValue,
) {
  lc.info?.('fetchFromAPIServer called', {
    url,
    allowedUrls,
  });

  if (!urlMatch(url, allowedUrls)) {
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
 * Returns true if the url matches one of the allowedUrls, where each allowedUrl is a regex pattern string.
 *
 * Query parameters are ignored when matching.
 *
 * Example regex patterns:
 * - "^https://api\\.example\\.com/endpoint$" - Exact match for a specific URL
 * - "^https://[^.]+\\.example\\.com/endpoint$" - Matches any single subdomain (e.g., "https://api.example.com/endpoint")
 * - "^https://[^.]+\\.[^.]+\\.example\\.com/endpoint$" - Matches two subdomains (e.g., "https://api.v1.example.com/endpoint")
 * - "^https://(api|www)\\.example\\.com/" - Matches specific subdomains
 * - "^https://api\\.v\\d+\\.example\\.com/" - Matches versioned subdomains (e.g., "https://api.v1.example.com", "https://api.v2.example.com")
 */
export function urlMatch(url: string, allowedUrls: string[]): boolean {
  // ignore query parameters in the URL
  url = url.split('?')[0];

  for (const allowedUrl of allowedUrls) {
    try {
      // Get or create cached regex pattern
      let regex = regexCache.get(allowedUrl);
      if (!regex) {
        regex = new RegExp(allowedUrl);
        regexCache.set(allowedUrl, regex);
      }
      if (regex.test(url)) {
        return true;
      }
    } catch (e) {
      // If the regex is invalid, log and skip it
      throw new Error(
        `Invalid regex pattern in allowedUrls: "${allowedUrl}". Error: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }
  return false;
}
