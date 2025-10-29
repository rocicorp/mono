import {ProtocolError} from '../../../zero-protocol/src/error.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';

export type SchemaVersions = {
  readonly minSupportedVersion: number;
  readonly maxSupportedVersion: number;
};

export function throwProtocolErrorIfSchemaVersionNotSupported(
  schemaVersion: number,
  schemaVersions: SchemaVersions,
) {
  const error = getProtocolErrorIfSchemaVersionNotSupported(
    schemaVersion,
    schemaVersions,
  );
  if (error) {
    throw error;
  }
}

export function getProtocolErrorIfSchemaVersionNotSupported(
  schemaVersion: number,
  schemaVersions: SchemaVersions,
) {
  const {minSupportedVersion, maxSupportedVersion} = schemaVersions;
  if (
    schemaVersion < minSupportedVersion ||
    schemaVersion > maxSupportedVersion
  ) {
    return new ProtocolError({
      kind: ErrorKind.SchemaVersionNotSupported,
      message: `Schema version ${schemaVersion} is not in range of supported schema versions [${minSupportedVersion}, ${maxSupportedVersion}].`,
      origin: ErrorOrigin.ZeroCache,
    });
  }
  return undefined;
}
