/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
