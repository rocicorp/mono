import type {MaybePromise} from 'replicache';
import type {UserData} from 'reflect-types';

/**
 * An `AuthHandler` should validate that the user authenticated by `auth` is
 * authorized to access the room with `roomID`. By 'access' we mean create or
 * connect to the room.
 * @return A promise which resolves to `UserData` for the user if authentication
 * and authorization is successful. If authentication fails you can return
 * `null`. Exceptions and promise rejections are treated as authentication
 * failures.
 */
export type AuthHandler = (
  auth: string,
  roomID: string,
) => MaybePromise<UserData | null>;

/**
 * Value should be a `UserData` value JSON stringified and encoded
 * with `encodeUrlComponent`.
 */
export const USER_DATA_HEADER_NAME = 'x-reflect-user-data';
