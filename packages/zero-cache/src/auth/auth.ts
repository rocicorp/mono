import type {LogContext} from '@rocicorp/logger';
import type {JWTPayload} from 'jose';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {
  isProtocolError,
  ProtocolError,
  type ErrorBody,
} from '../../../zero-protocol/src/error.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import type {PushError} from '../../../zero-protocol/src/push.ts';

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

export type ValidateLegacyJWT = (
  token: string,
  ctx: {readonly userID: string | undefined},
) => Promise<JWTAuth>;

function isProvidedAuth(wireAuth: string | undefined): wireAuth is string {
  return wireAuth !== undefined && wireAuth !== '';
}

export function authEquals(a: Auth | undefined, b: Auth | undefined) {
  if (a === b) {
    return true;
  }
  if (!a || !b) {
    return false;
  }
  return a.type === b.type && a.raw === b.raw;
}

/**
 * Resolves one auth snapshot transition without binding it to a client group.
 */
export async function resolveAuth(
  lc: LogContext,
  previousAuth: Auth | undefined,
  userID: string | undefined,
  wireAuth: string | undefined,
  validateLegacyJWT: ValidateLegacyJWT | undefined,
): Promise<Auth | undefined> {
  try {
    const hasProvidedAuth = isProvidedAuth(wireAuth);

    if (previousAuth) {
      lc.debug?.(`Attempting to update auth from previous value`);
    } else {
      lc.debug?.(`Attempting to initialize auth`);
    }

    if (!hasProvidedAuth && previousAuth) {
      throw new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message:
          'No token provided. An unauthenticated client cannot connect to an authenticated client group.',
        origin: ErrorOrigin.ZeroCache,
      });
    }

    if (!hasProvidedAuth) {
      lc.debug?.(`Cleared auth`);
      return undefined;
    }

    if (userID === undefined) {
      throw new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message: 'Authenticated connections require a userID.',
        origin: ErrorOrigin.ZeroCache,
      });
    }

    if (validateLegacyJWT !== undefined) {
      const verifiedToken = await validateLegacyJWT(wireAuth, {userID});
      const nextAuth = pickToken(lc, previousAuth, verifiedToken);
      lc.debug?.(`Updated auth with JWT`);
      return nextAuth;
    }

    if (previousAuth?.type === 'jwt') {
      throw new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message:
          'Token type cannot change from JWT to opaque. Connections are pinned to a single token type.',
        origin: ErrorOrigin.ZeroCache,
      });
    }

    if (previousAuth?.type === 'opaque' && previousAuth.raw === wireAuth) {
      lc.debug?.(`Opaque auth unchanged, reusing previous snapshot`);
      return previousAuth;
    }

    lc.debug?.(`Updated auth with opaque token`);
    return {
      type: 'opaque',
      raw: wireAuth,
    };
  } catch (e) {
    if (isProtocolError(e)) {
      throw e;
    }
    throw new ProtocolError({
      kind: ErrorKind.AuthInvalidated,
      message: `Failed to decode auth token: ${String(e)}`,
      origin: ErrorOrigin.ZeroCache,
    });
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

export function isAuthErrorBody(ex: unknown): ex is ErrorBody | PushError {
  if (typeof ex !== 'object' || ex === null) {
    return false;
  }

  if ('error' in ex) {
    return (
      ex.error === 'http' &&
      'status' in ex &&
      (ex.status === 401 || ex.status === 403)
    );
  }

  if (!('kind' in ex)) {
    return false;
  }

  if (
    ex.kind === ErrorKind.AuthInvalidated ||
    ex.kind === ErrorKind.Unauthorized
  ) {
    return true;
  }

  return (
    (ex.kind === ErrorKind.PushFailed ||
      ex.kind === ErrorKind.TransformFailed) &&
    'reason' in ex &&
    ex.reason === ErrorReason.HTTP &&
    'status' in ex &&
    (ex.status === 401 || ex.status === 403)
  );
}
