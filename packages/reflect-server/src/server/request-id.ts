import type {LogContext} from '@rocicorp/logger';
import {randomID} from '../util/rand.js';

const REQUEST_ID_HEADER_NAME = 'X-Replicache-RequestID';

/**
 * Adds a requestID to the LogContext. If the request has a
 * REQUEST_ID_HEADER_NAME header, that is used. Otherwise a random ID is
 * generated.
 */
export function addRequestIDFromHeadersOrRandomID(
  lc: LogContext,
  request: Request,
): LogContext {
  return lc.withContext(
    'requestID',
    getRequestIDFromRequest(request) ?? randomID(),
  );
}

function getRequestIDFromRequest(request: Request): string | null {
  return request.headers.get(REQUEST_ID_HEADER_NAME);
}
