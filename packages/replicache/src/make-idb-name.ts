import * as FormatVersion from './format-version-enum.ts';

/**
 * Returns the name of the IDB database that will be used for a particular Replicache instance.
 * @param name The name of the Replicache instance (i.e., the `name` field of `ReplicacheOptions`).
 * @param schemaVersion The schema version of the database (i.e., the `schemaVersion` field of `ReplicacheOptions`).
 * @returns
 */
export function makeIDBName(name: string, schemaVersion?: string): string {
  return makeIDBNameInternal(name, schemaVersion, FormatVersion.Latest);
}

function makeIDBNameInternal(
  name: string,
  schemaVersion: string | undefined,
  formatVersion: number,
): string {
  const n = `rep:${name}:${formatVersion}`;
  return schemaVersion ? `${n}:${schemaVersion}` : n;
}

export {makeIDBNameInternal as makeIDBNameForTesting};
