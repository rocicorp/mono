import * as v from 'shared/src/valita.js';

// Include all characters that are encoded by encodeURIComponent
// so that they can be used as non-ID delimiters in the future.
const ID_REGEX = '[^:/@$^&=\\[\\];|,]+';

export const HELLO = '/';
export const CANARY_GET = '/api/canary/v0/get';
export const REPORT_METRICS_PATH = '/api/metrics/v0/report';
export const LOG_LOGS_PATH = '/api/logs/v0/log';

export const CONNECT_URL_PATTERN = '/api/sync/:version/connect';

export const AUTH_CONNECTIONS_PATH = '/api/auth/v0/connections';

export const LIST_ROOMS_PATH = '/api/v1/rooms';
export const LEGACY_GET_ROOM_PATH = `/api/v1/rooms/:roomID(${ID_REGEX})`;

export const CREATE_ROOM_PATH = `/api/v1/rooms\\:create`;
export const CLOSE_ROOM_PATH = `/api/v1/rooms\\:close`;
export const DELETE_ROOM_PATH = `/api/v1/rooms\\:delete`;
export const GET_CONTENTS_ROOM_PATH = `/api/v1/rooms/contents`;

export const LEGACY_CREATE_ROOM_PATH = `/api/v1/rooms/:roomID(${ID_REGEX})\\:create`;
export const LEGACY_CLOSE_ROOM_PATH = `/api/v1/rooms/:roomID(${ID_REGEX})\\:close`;
export const LEGACY_DELETE_ROOM_PATH = `/api/v1/rooms/:roomID(${ID_REGEX})\\:delete`;

export const INVALIDATE_ALL_CONNECTIONS_PATH =
  '/api/v1/connections/all\\:invalidate';
export const INVALIDATE_ROOM_CONNECTIONS_PATH = `/api/v1/connections/rooms\\:invalidate`;
export const INVALIDATE_USER_CONNECTIONS_PATH = `/api/v1/connections/users\\:invalidate`;

export const LEGACY_INVALIDATE_ROOM_CONNECTIONS_PATH = `/api/v1/connections/rooms/:roomID(${ID_REGEX})\\:invalidate`;
export const LEGACY_INVALIDATE_USER_CONNECTIONS_PATH = `/api/v1/connections/users/:userID(${ID_REGEX})\\:invalidate`;

export const TAIL_URL_PATH = '/api/debug/v0/tail';

export const roomIDParams = v.object({
  roomID: v.string(),
});

export const userIDParams = v.object({
  userID: v.string(),
});

export function fmtPath(
  pathPattern: string,
  placeholdersOrQuery?: Record<string, string> | URLSearchParams,
): string {
  // TODO: Get rid of placeholders when legacy URLs are retired.
  const placeholders =
    placeholdersOrQuery instanceof URLSearchParams
      ? undefined
      : placeholdersOrQuery;
  const query =
    placeholdersOrQuery instanceof URLSearchParams
      ? placeholdersOrQuery
      : undefined;
  Object.entries(placeholders ?? {}).forEach(([placeholder, value]) => {
    pathPattern = pathPattern.replaceAll(
      `:${placeholder}`,
      encodeURIComponent(value),
    );
  });
  // Strip regex patterns, then escape characters.
  const path = pathPattern.replaceAll(/\([^)]+\)/g, '').replaceAll('\\', '');
  return query ? `${path}?${query}` : path;
}
