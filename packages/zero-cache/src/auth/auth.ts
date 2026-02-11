import type {LogContext} from '@rocicorp/logger';
import type {JWTPayload} from 'jose';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {
  isProtocolError,
  ProtocolError,
  type ErrorBody,
} from '../../../zero-protocol/src/error.ts';

/** @deprecated JWT auth is deprecated */
export type JWTAuth = {
  readonly type: 'jwt';
  readonly raw: string;
  readonly decoded: JWTPayload;
};

export type OpaqueAuth = {
  readonly type: 'opaque';
  readonly raw: string;
};

export type Auth = OpaqueAuth | JWTAuth;

export interface AuthSession {
  /** Update the auth session with a new userID and token from the client */
  update(
    userID: string,
    wireAuth: string | undefined,
  ): Promise<AuthUpdateResult>;

  /** The revision of the auth state */
  get revision(): number;

  /** The auth state for the session */
  get auth(): Auth | undefined;

  /** Clear the auth session, removing any stored auth and allowing a new userID to be bound on the next update. */
  clear(): void;
}

export type AuthUpdateResult =
  | {
      readonly ok: true;
    }
  | {
      readonly ok: false;
      readonly error: ErrorBody;
    };

export type ValidateLegacyJWT = (
  token: string,
  ctx: {readonly userID: string},
) => Promise<JWTAuth>;

function isProvidedAuth(wireAuth: string | undefined): wireAuth is string {
  return wireAuth !== undefined && wireAuth !== '';
}

function authEquals(a: Auth | null | undefined, b: Auth | null | undefined) {
  if (a === b) {
    return true;
  }
  if (!a || !b) {
    return false;
  }
  return a.type === b.type && a.raw === b.raw;
}

export class AuthSessionImpl implements AuthSession {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #validateLegacyJWT: ValidateLegacyJWT | undefined;
  #auth: Auth | undefined = undefined;
  #boundUserID: string | undefined;
  #revision = 0;

  constructor(
    lc: LogContext,
    clientGroupID: string,
    validateLegacyJWT: ValidateLegacyJWT | undefined,
  ) {
    this.id = clientGroupID;
    this.#lc = lc;
    this.#validateLegacyJWT = validateLegacyJWT;
  }

  get auth(): Auth | undefined {
    return this.#auth;
  }

  get revision(): number {
    return this.#revision;
  }

  clear(): void {
    this.#auth = undefined;
    this.#boundUserID = undefined;
    this.#revision = 0;
  }

  async update(
    userID: string,
    wireAuth: string | undefined,
  ): Promise<AuthUpdateResult> {
    try {
      // check if the auth update is trying to change the bound userID for this client group
      if (this.#boundUserID && this.#boundUserID !== userID) {
        return {
          ok: false,
          error: {
            kind: ErrorKind.Unauthorized,
            message:
              'Client groups are pinned to a single user. Connection userID does not match existing client group userID.',
            origin: ErrorOrigin.ZeroCache,
          },
        };
      }

      const previousAuth = this.#auth;
      const hasProvidedAuth = isProvidedAuth(wireAuth);
      let nextAuth = previousAuth;

      if (!hasProvidedAuth && previousAuth) {
        return {
          ok: false,
          error: {
            kind: ErrorKind.Unauthorized,
            message:
              'No token provided. An unauthenticated client cannot connect to an authenticated client group.',
            origin: ErrorOrigin.ZeroCache,
          },
        };
      }

      if (!hasProvidedAuth) {
        nextAuth = undefined;
      } else if (this.#validateLegacyJWT !== undefined) {
        const verifiedToken = await this.#validateLegacyJWT(wireAuth, {userID});
        nextAuth = pickToken(this.#lc, this.#auth, verifiedToken);
      } else {
        if (this.#auth?.type === 'jwt') {
          throw new Error(
            'Cannot change auth type from legacy to opaque token',
          );
        }
        nextAuth = {
          type: 'opaque',
          raw: wireAuth,
        };
      }

      this.#auth = nextAuth;
      this.#boundUserID ??= userID;

      if (!authEquals(previousAuth, nextAuth)) {
        this.#revision++;
      }
    } catch (e) {
      if (isProtocolError(e)) {
        return {
          ok: false,
          error: e.errorBody,
        };
      }
      return {
        ok: false,
        error: {
          kind: ErrorKind.AuthInvalidated,
          message: `Failed to decode auth token: ${String(e)}`,
          origin: ErrorOrigin.ZeroCache,
        },
      };
    }

    return {ok: true};
  }
}

/** @deprecated used only in old JWT validation/rotation auth */
export function pickToken(
  lc: LogContext,
  previousToken: Auth | undefined,
  newToken: Auth | undefined | null,
) {
  if (newToken === null) {
    return undefined;
  }

  if (
    previousToken?.type &&
    newToken?.type &&
    previousToken?.type !== newToken?.type
  ) {
    throw new ProtocolError({
      kind: ErrorKind.Unauthorized,
      message:
        'Token type cannot change. Client groups are pinned to a single token type.',
      origin: ErrorOrigin.ZeroCache,
    });
  }

  if (previousToken === undefined) {
    lc.debug?.(`No previous token, using new token`);
    return newToken;
  }

  if (newToken?.type === 'opaque') {
    return newToken;
  }

  if (previousToken.type === 'opaque') {
    throw new ProtocolError({
      kind: ErrorKind.Unauthorized,
      message:
        'Token type cannot change from opaque to JWT. Client groups are pinned to a single token type.',
      origin: ErrorOrigin.ZeroCache,
    });
  }

  if (newToken) {
    if (previousToken.decoded.sub !== newToken.decoded.sub) {
      throw new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message:
          'The user id in the new token does not match the previous token. Client groups are pinned to a single user.',
        origin: ErrorOrigin.ZeroCache,
      });
    }

    if (previousToken.decoded.iat === undefined) {
      lc.debug?.(`No issued at time for the existing token, using new token`);
      // No issued at time for the existing token? We take the most recently received token.
      return newToken;
    }

    if (newToken.decoded.iat === undefined) {
      throw new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message:
          'The new token does not have an issued at time but the prior token does. Tokens for a client group must either all have issued at times or all not have issued at times',
        origin: ErrorOrigin.ZeroCache,
      });
    }

    // The new token is newer, so we take it.
    if (previousToken.decoded.iat < newToken.decoded.iat) {
      lc.debug?.(`New token is newer, using it`);
      return newToken;
    }

    // if the new token is older or the same, we keep the existing token.
    lc.debug?.(`New token is older or the same, using existing token`);
    return previousToken;
  }

  // previousToken !== undefined but newToken is undefined
  throw new ProtocolError({
    kind: ErrorKind.Unauthorized,
    message:
      'No token provided. An unauthenticated client cannot connect to an authenticated client group.',
    origin: ErrorOrigin.ZeroCache,
  });
}
