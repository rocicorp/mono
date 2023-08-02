import type {LogContext} from '@rocicorp/logger';

export function populateLogContextFromRequest(
  lc: LogContext,
  request: Request,
) {
  const url = new URL(request.url);
  // TODO: All the "id" suffixes seem useless
  lc = maybeAddContext(lc, url.searchParams, 'wsid');
  lc = maybeAddContext(lc, url.searchParams, 'requestID');
  lc = maybeAddContext(lc, url.searchParams, 'clientID');
  lc = maybeAddContext(lc, url.searchParams, 'clientGroupID');
  lc = maybeAddContext(lc, url.searchParams, 'roomID');
  lc = maybeAddContext(lc, url.searchParams, 'userID');

  const ip = request.headers.get('CF-Connecting-IP');
  // We use the same attribute path that the datadog RUM does for the ip collected
  // on client side so that we can tie them together.
  return ip ? lc.withContext('network.client.clientIP', ip) : lc;
}

function maybeAddContext(lc: LogContext, qs: URLSearchParams, key: string) {
  const val = qs.get(key);
  if (val) {
    lc = lc.withContext(key, val);
  }
  return lc;
}
