export const HELLO = '/';
export const CANARY_GET = '/api/canary/v0/get';
export const REPORT_METRICS_PATH = '/api/metrics/v0/report';
export const LOG_LOGS_PATH = '/api/logs/v0/log';

export const CONNECT_URL_PATTERN = '/api/sync/:version/connect';
export const DISCONNECT_BEACON_PATH = '/api/sync/v1/disconnect';

export const AUTH_CONNECTIONS_PATH = '/api/auth/v0/connections';

export const CREATE_ROOM_PATH = '/api/v1/rooms/:roomID\\:create';
export const CLOSE_ROOM_PATH = '/api/v1/rooms/:roomID\\:close';
export const DELETE_ROOM_PATH = '/api/v1/rooms/:roomID\\:delete';

export const INVALIDATE_ALL_CONNECTIONS_PATH =
  '/api/v1/connections\\:invalidate';
export const INVALIDATE_ROOM_CONNECTIONS_PATH =
  '/api/v1/connections/rooms/:roomID\\:invalidate';
export const INVALIDATE_USER_CONNECTIONS_PATH =
  '/api/v1/connections/users/:userID\\:invalidate';

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
  return pathPattern.replaceAll('\\', '');
}
