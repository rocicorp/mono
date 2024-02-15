import {
  INVALIDATE_ALL_CONNECTIONS_PATH,
  INVALIDATE_ROOM_CONNECTIONS_PATH,
  INVALIDATE_USER_CONNECTIONS_PATH,
  fmtPath,
} from '../server/paths.js';
import {newAuthedPostRequest} from './authedpost.js';

export function newInvalidateAllAuthRequest(
  reflectServerURL: string,
  authApiKey: string,
) {
  const path = fmtPath(INVALIDATE_ALL_CONNECTIONS_PATH);
  const url = new URL(path, reflectServerURL);
  return newAuthedPostRequest(url, authApiKey);
}

export function newInvalidateForUserAuthRequest(
  reflectServerURL: string,
  authApiKey: string,
  userID: string,
) {
  const path = fmtPath(
    INVALIDATE_USER_CONNECTIONS_PATH,
    new URLSearchParams({userID}),
  );
  const url = new URL(path, reflectServerURL);
  return newAuthedPostRequest(url, authApiKey);
}

export function newInvalidateForRoomAuthRequest(
  reflectServerURL: string,
  authApiKey: string,
  roomID: string,
) {
  const path = fmtPath(
    INVALIDATE_ROOM_CONNECTIONS_PATH,
    new URLSearchParams({roomID}),
  );
  const url = new URL(path, reflectServerURL);
  return newAuthedPostRequest(url, authApiKey);
}
