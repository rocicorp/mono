import {encodeHeaderValue} from '../util/headers.js';

/**
 * Name of header used to pass `AuthData` received by AuthDO from the
 * authHandler to the RoomDO.
 *
 * Value should be an `AuthData` value JSON stringified and encoded
 * with `encodeUrlComponent`.
 *
 * Should be present on all connect requests.
 */
export const AUTH_DATA_HEADER_NAME = 'x-reflect-auth-data';

/**
 * Name of header used to pass the room's roomID from the AuthDO to the
 * RoomDO.  Should be present on all requests.
 */
export const ROOM_ID_HEADER_NAME = 'x-reflect-room-id';

export function addRoomIDHeader(request: Request, roomID: string) {
  request.headers.set(ROOM_ID_HEADER_NAME, encodeHeaderValue(roomID));
  return request;
}
