import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorForClient} from './error-for-client.ts';

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
    return new ErrorForClient({
      kind: ErrorKind.SchemaVersionNotSupported,
      message: `Schema version ${schemaVersion} is not in range of supported schema versions [${minSupportedVersion}, ${maxSupportedVersion}].`,
    });
  }
  return undefined;
}
