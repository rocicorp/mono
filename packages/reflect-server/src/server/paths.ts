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
export const GET_ROOM_PATH = `/api/v1/rooms/:roomID(${ID_REGEX})`;

export const CREATE_ROOM_PATH = `/api/v1/rooms/:roomID(${ID_REGEX})\\:create`;
export const CLOSE_ROOM_PATH = `/api/v1/rooms/:roomID(${ID_REGEX})\\:close`;
export const DELETE_ROOM_PATH = `/api/v1/rooms/:roomID(${ID_REGEX})\\:delete`;

export const INVALIDATE_ALL_CONNECTIONS_PATH =
  '/api/v1/connections/all\\:invalidate';
export const INVALIDATE_ROOM_CONNECTIONS_PATH = `/api/v1/connections/rooms/:roomID(${ID_REGEX})\\:invalidate`;
export const INVALIDATE_USER_CONNECTIONS_PATH = `/api/v1/connections/users/:userID(${ID_REGEX})\\:invalidate`;

export const TAIL_URL_PATH = '/api/debug/v0/tail';

export function fmtPath(
  pathPattern: string,
  placeholders?: Record<string, string>,
): string {
  Object.entries(placeholders ?? {}).forEach(([placeholder, value]) => {
    pathPattern = pathPattern.replaceAll(
      `:${placeholder}`,
      encodeURIComponent(value),
    );
  });
  // Strip regex patterns, then escape characters.
  return pathPattern.replaceAll(/\([^)]+\)/g, '').replaceAll('\\', '');
}
