import type {LogContext} from '@rocicorp/logger';
import {ROOM_ID_HEADER_NAME} from '../server/internal-headers.js';
import {decodeHeaderValue} from './headers.js';

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
  const roomIDHeaderValue = request.headers.get(ROOM_ID_HEADER_NAME);
  if (roomIDHeaderValue !== null) {
    lc = lc.withContext('roomID', decodeHeaderValue(roomIDHeaderValue));
  } else {
    lc = maybeAddContext(lc, url.searchParams, 'roomID');
  }
  lc = maybeAddContext(lc, url.searchParams, 'userID');

  const ip = request.headers.get('CF-Connecting-IP');
  // We use the same attribute path that the datadog RUM does for ip
  // and UserAgent
  lc = ip ? lc.withContext('network.client.ip', ip) : lc;
  const userAgent = request.headers.get('User-Agent');
  lc = userAgent ? lc.withContext('http.useragent', userAgent) : lc;
  return lc;
}

function maybeAddContext(lc: LogContext, qs: URLSearchParams, key: string) {
  const val = qs.get(key);
  if (val) {
    lc = lc.withContext(key, val);
  }
  return lc;
}
