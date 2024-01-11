export const ROOM_ID_REGEX = /^[A-Za-z0-9_/-]+$/;

export function isValidRoomID(roomID: string) {
  return ROOM_ID_REGEX.test(roomID);
}

export function makeInvalidRoomIDMessage(roomID: string) {
  return `Invalid roomID "${roomID}" (must match ${ROOM_ID_REGEX})`;
}
