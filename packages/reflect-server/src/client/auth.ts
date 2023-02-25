import type {
  InvalidateForRoomRequest,
  InvalidateForUserRequest,
} from 'reflect-protocol';
import {AUTH_ROUTES} from '../server/auth-do.js';
import {newAuthedPostRequest} from './authedpost.js';

export function newInvalidateAllAuthRequest(
  reflectServerURL: string,
  authApiKey: string,
) {
  const path = AUTH_ROUTES.authInvalidateAll;
  const url = new URL(path, reflectServerURL);
  return newAuthedPostRequest(url, authApiKey);
}

export function newInvalidateForUserAuthRequest(
  reflectServerURL: string,
  authApiKey: string,
  userID: string,
) {
  const path = AUTH_ROUTES.authInvalidateForUser;
  const url = new URL(path, reflectServerURL);
  const req: InvalidateForUserRequest = {userID};
  return newAuthedPostRequest(url, authApiKey, req);
}

export function newInvalidateForRoomAuthRequest(
  reflectServerURL: string,
  authApiKey: string,
  roomID: string,
) {
  const path = AUTH_ROUTES.authInvalidateForRoom;
  const url = new URL(path, reflectServerURL);
  const req: InvalidateForRoomRequest = {roomID};
  return newAuthedPostRequest(url, authApiKey, req);
}
