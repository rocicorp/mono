import {assert} from '../../../shared/src/asserts.ts';
import {parseBigInt} from '../../../shared/src/parse-big-int.ts';

/**
 * A LexiVersion is a lexicographically sortable representation of
 * numbers from 0 to Number.MAX_SAFE_INTEGER (which is the safe range of
 * Version values used in Zero).
 *
 * The Version is first encoded in base36, and then prepended by a single
 * base36 character representing the length (of the base36 version) minus 1.
 * This encoding can encode numbers up to 185 bits, with the maximum encoded
 * number being `"z".repeat(37)`, or 36^36-1 (approximately 1.0638735892371651e+56).
 *
 * Examples:
 * * 0 => "00"
 * * 10 => "0a"
 * * 35 => "0z"
 * * 36 => "110"
 * * 46655 => "2zzz"
 * * 2^64 => "c3w5e11264sgsg"
 *
 * Note that when using the `number` type, the library will assert if attempting
 * to encode a Version larger than Number.MAX_SAFE_INTEGER. For large numbers,
 * use the `bigint` type.
 */
export type LexiVersion = string;

export function versionToLexi(v: number | bigint): LexiVersion {
  assert(v >= 0, 'Negative versions are not supported');
  assert(
    typeof v === 'bigint' ||
      (v <= Number.MAX_SAFE_INTEGER && Number.isInteger(v)),
    `Invalid or unsafe version ${v}`,
  );
  const base36Version = BigInt(v).toString(36);
  const length = BigInt(base36Version.length - 1).toString(36);
  assert(
    length.length === 1,
    `Value is too large to be encoded as a LexiVersion: ${v.toString()}`,
  );
  return `${length}${base36Version}`;
}

export function versionFromLexi(lexiVersion: LexiVersion): bigint {
  assert(
    lexiVersion.length >= 2,
    () =>
      `LexiVersion must have at least 2 characters, got ${lexiVersion.length}`,
  );
  const length = lexiVersion.substring(0, 1);
  const base36Version = lexiVersion.substring(1);
  assert(
    base36Version.length === parseInt(length, 36) + 1,
    `Invalid LexiVersion: ${lexiVersion}`,
  );
  return parseBigInt(base36Version, 36);
}

export type AtLeastOne<T> = [T, ...T[]];

export function max(...versions: AtLeastOne<LexiVersion>): LexiVersion {
  let winner = versions[0];
  for (let i = 1; i < versions.length; i++) {
    const b = versions[i];
    winner = winner > b ? winner : b;
  }
  return winner;
}

export function min(...versions: AtLeastOne<LexiVersion>): LexiVersion {
  let winner = versions[0];
  for (let i = 1; i < versions.length; i++) {
    const b = versions[i];
    winner = winner < b ? winner : b;
  }
  return winner;
}

export function oneAfter(version: LexiVersion) {
  return versionToLexi(versionFromLexi(version) + 1n);
}
