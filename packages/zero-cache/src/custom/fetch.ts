import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {upstreamSchema, type ShardID} from '../types/shards.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {ErrorForClient} from '../types/error-for-client.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';

const reservedParams = ['schema', 'appID'];
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
  lc.info?.('Executing fetch', {finalUrl});

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
 * Returns true if:
 * 1. the url is an exact match with one of the allowedUrls
 * 2. an "allowedUrl" has a wildcard for a subdomain, e.g. "https://*.example.com" and the url matches that pattern
 *
 * Valid wildcard patterns:
 * - "https://*.example.com" matches "https://api.example.com" and "https://www.example.com"
 * - "https://*.example.com" does not match "https://example.com" (no subdomain)
 * - "https://*.example.com" does not match "https://api.example.com/path" (no trailing path)
 * - "https://*.*.example.com" matches "https://api.v1.example.com" and "https://www.v2.example.com"
 * - "https://*.*.example.com" does not match "https://api.example.com" (only one subdomain)
 */
export function urlMatch(url: string, allowedUrls: string[]): boolean {
  assert(url.includes('*') === false, 'URL to fetch may not include `*`');
  // ignore query parameters in the URL
  url = url.split('?')[0];

  for (let allowedUrl of allowedUrls) {
    // ignore query parameters in the allowed URL
    allowedUrl = allowedUrl.split('?')[0];
    if (url === allowedUrl) {
      return true; // exact match
    }

    const parts = allowedUrl.split('*');

    if (parts.length === 1) {
      continue; // no wildcard, already checked above
    }

    let currentStr = url;
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      if (!currentStr.startsWith(part)) {
        break;
      }

      currentStr = currentStr.slice(part.length);
      if (currentStr === '' && i < parts.length - 1) {
        // if we reach the end of the string but still have more parts to match, it's not a match
        break;
      } else if (currentStr === '' && i === parts.length - 1) {
        // if we reach the end of the string and this is the last part, it's a match
        return true;
      }

      // consume the rest of the string up to a .
      const nextDotIndex = currentStr.indexOf('.');
      if (nextDotIndex === -1) {
        // no dot? then the wildcard rules don't apply, so we can stop checking
        break;
      }
      currentStr = currentStr.slice(nextDotIndex);
    }
  }
  return false;
}
