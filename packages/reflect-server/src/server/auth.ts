import type {ReadonlyJSONObject} from 'replicache';

/**
 * An `AuthHandler` should validate that the user authenticated by `auth` is
 * authorized to access the room with `roomID`. By 'access' we mean create
 * or connect to the room.
 * @return A promise which resolves to `UserData` for the user if authentication
 * and authorization is successful, or rejects (throws) if authentication or
 * authorization fail.
 */
export type AuthHandler = (auth: string, roomID: string) => Promise<UserData>;

/**
 * `UserData` must include a `userID` which is unique stable identifier
 * for the user.
 * `UserData` has a size limit of 6 KB.
 * Currently only `userID` is used, but in the future `UserData` may
 * be passed through to mutators which could use it to supplement
 * mutator args and to validate the mutation.
 */
export type UserData = ReadonlyJSONObject & {userID: string};

/**
 * Value should be a `UserData` value JSON stringified and encoded
 * with `encodeUrlComponent`.
 */
export const USER_DATA_HEADER_NAME = 'x-reflect-user-data';
