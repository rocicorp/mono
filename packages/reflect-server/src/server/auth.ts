import type {AuthData} from 'reflect-shared';
import type {MaybePromise} from 'replicache';

/**
 * An `AuthHandler` should validate that the user authenticated by `auth` is
 * authorized to access the room with `roomID`. By 'access' we mean create or
 * connect to the room with `roomID`.
 * @return A promise which resolves to `AuthData` for the user if authentication
 * and authorization is successful. If authentication fails you can return
 * `null`. Exceptions and promise rejections are treated as authentication
 * failures.  The returned `AuthData` is passed via
 * {@link WriteTransaction.auth} to mutators when they are run on the server,
 * and can be used to implement fine-grained server-side authorization of
 * mutations.
 */
export type AuthHandler = (
  auth: string,
  roomID: string,
) => MaybePromise<AuthData | null>;

/**
 * Value should be an `AuthData` value JSON stringified and encoded
 * with `encodeUrlComponent`.
 */
export const AUTH_DATA_HEADER_NAME = 'x-reflect-auth-data';
