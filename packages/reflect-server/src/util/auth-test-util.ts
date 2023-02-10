import {newAuthedPostRequest} from '../client/authedpost.js';
import {AUTH_ROUTES} from '../server/auth-do.js';
import {ROOM_ROUTES} from '../server/room-do.js';

export function newAuthConnectionsRequest(
  reflectServerURL: string,
  authApiKey: string,
) {
  const path = ROOM_ROUTES.authConnections;
  const url = new URL(path, reflectServerURL);
  return newAuthedPostRequest(url, authApiKey);
}

export function newAuthRevalidateConnections(
  reflectServerURL: string,
  authApiKey: string,
) {
  const path = AUTH_ROUTES.authRevalidateConnections;
  const url = new URL(path, reflectServerURL);
  return newAuthedPostRequest(url, authApiKey);
}
