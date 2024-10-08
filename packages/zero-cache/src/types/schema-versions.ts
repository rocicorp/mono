import {ErrorKind} from '../../../zero-protocol/src/mod.js';
import {ErrorForClient} from './error-for-client.js';

export type SchemaVersions = {
  readonly minSupportedVersion: number;
  readonly maxSupportedVersion: number;
};

export function throwErrorForClientIfSchemaVersionNotSupported(
  schemaVersion: number,
  schemaVersions: SchemaVersions,
) {
  const error = getErrorForClientIfSchemaVersionNotSupported(
    schemaVersion,
    schemaVersions,
  );
  if (error) {
    throw error;
  }
}

export function getErrorForClientIfSchemaVersionNotSupported(
  schemaVersion: number,
  schemaVersions: SchemaVersions,
) {
  const {minSupportedVersion, maxSupportedVersion} = schemaVersions;
  if (
    schemaVersion < minSupportedVersion ||
    schemaVersion > maxSupportedVersion
  ) {
    return new ErrorForClient([
      'error',
      ErrorKind.SchemaVersionNotSupported,
      `Schema version ${schemaVersion} is not in range of supported schema versions [${minSupportedVersion}, ${maxSupportedVersion}].`,
    ]);
  }
  return undefined;
}
