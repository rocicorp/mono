import {assert} from 'shared/asserts.js';

function isError(obj: unknown, type: string): boolean {
  return (
    typeof obj === 'object' &&
    obj !== null &&
    (obj as {error: unknown}).error === type
  );
}

type ErrorResponse = {error: string};

export function isErrorResponse(obj: object): obj is ErrorResponse {
  return typeof (obj as {error: unknown}).error === 'string';
}

/**
 * In certain scenarios the server can signal that it does not know about the
 * client. For example, the server might have lost all of its state (this might
 * happen during the development of the server).
 */
export type ClientStateNotFoundResponse = {
  error: 'ClientStateNotFound';
};

export function isClientStateNotFoundResponse(
  v: unknown,
): v is ClientStateNotFoundResponse {
  return isError(v, 'ClientStateNotFound');
}

/**
 * The server endpoint may respond with a `VersionNotSupported` error if it does
 * not know how to handle the {@link pullVersion}, {@link pushVersion} or the
 * {@link schemaVersion}.
 */
export type VersionNotSupportedResponse = {
  error: 'VersionNotSupported';
  versionType?: 'pull' | 'push' | 'schema' | undefined;
};

export function isVersionNotSupportedResponse(
  v: unknown,
): v is VersionNotSupportedResponse {
  if (!isError(v, 'VersionNotSupported')) {
    return false;
  }

  const {versionType} = v as Record<string, unknown>;
  switch (versionType) {
    case undefined:
    case 'pull':
    case 'push':
    case 'schema':
      return true;
  }

  return false;
}

export function assertVersionNotSupportedResponse(
  v: unknown,
): asserts v is VersionNotSupportedResponse {
  assert(isVersionNotSupportedResponse(v));
}
