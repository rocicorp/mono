import type {Version} from 'reflect-protocol';
import {assert} from 'shared/src/asserts.js';

/**
 * A LexiVersion is a lexicographically sortable representation of
 * numbers from 0 to Number.MAX_SAFE_INTEGER (which is the safe range of
 * Version values used in Reflect).
 *
 * The Version is first encoded in base36, and then prepended by a single
 * base36 character representing the length (of the base36 version) minus 1.
 *
 * Examples:
 * * 0 => "00"
 * * 10 => "0a"
 * * 35 => "0z"
 * * 36 => "110"
 * * 46655 => "2zzz"
 * * Number.MAX_SAFE_INTEGER => "a2gosa7pa2gv"
 *
 * Note that although the format technically supports encoding numbers
 * from 0 to 1.0638735892371651e+56 ("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"),
 * the library will assert if attempting to encode a Version larger than
 * Number.MAX_SAFE_INTEGER.
 */
export type LexiVersion = string;

export function versionToLexi(version: Version): LexiVersion {
  assert(
    version >= 0 &&
      version <= Number.MAX_SAFE_INTEGER &&
      Number.isInteger(version),
    `Invalid or unsafe version ${version}`,
  );
  const base36Version = version.toString(36);
  const length = (base36Version.length - 1).toString(36);
  return `${length}${base36Version}`;
}

export function versionFromLexi(lexiVersion: LexiVersion): Version {
  assert(lexiVersion.length >= 2);
  const length = lexiVersion.substring(0, 1);
  const base36Version = lexiVersion.substring(1);
  assert(
    base36Version.length === parseInt(length, 36) + 1,
    `Invalid LexiVersion: ${lexiVersion}`,
  );
  return parseInt(base36Version, 36);
}
