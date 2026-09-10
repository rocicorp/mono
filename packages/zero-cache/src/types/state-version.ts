import {assert} from '../../../shared/src/asserts.ts';
import {versionFromLexi, versionToLexi} from './lexi-version.ts';

/**
 * Identifies the version of the data on the replica, corresponding to the
 * stream of changes produced by the change-source and change-streamer.
 *
 * The `major` version directly tracks the watermark of the replication
 * stream (e.g. the Postgres LSN).
 *
 * The `minor` version is optional and used to auxiliary state changes,
 * such as writes from pending backfills.
 *
 * StateVersions are persisted and passed as lexicographically ordered
 * strings, using the LexiVersion format for major and minor versions,
 * separated by a dot. If there is no minor version, the StateVersion
 * is represented by a single LexiVersion.
 */
export type StateVersion = {
  major: bigint;
  minor?: bigint | undefined;
};

type StateVersionInput = {
  major: bigint | number;
  minor?: bigint | number | undefined;
};

export function stateVersionFromString(ver: string): StateVersion {
  if (!ver.includes('.')) {
    return {major: versionFromLexi(ver)};
  }
  const parts = ver.split('.');
  assert(parts.length === 2, () => `Invalid stateVersion ${ver}`);
  return {
    major: versionFromLexi(parts[0]),
    minor: versionFromLexi(parts[1]),
  };
}

export function stateVersionToString(ver: StateVersionInput) {
  return ver.minor === undefined
    ? versionToLexi(ver.major)
    : `${versionToLexi(ver.major)}.${versionToLexi(ver.minor)}`;
}

export function majorVersionFromString(ver: string): bigint {
  if (!ver.includes('.')) {
    return versionFromLexi(ver);
  }
  const {major} = stateVersionFromString(ver);
  return major;
}

export function majorVersionToString(major: number | bigint) {
  return versionToLexi(major);
}

/**
 * The major portion of a state version, as it appears in the string.
 *
 * Note that this is not `majorVersionToString(majorVersionFromString(ver))`:
 * a LexiVersion can be non-canonical (`"101"` and `"01"` both decode to 1),
 * and a change source's watermarks are its own to choose. Splitting the string
 * preserves whatever encoding the change source used, which is what a
 * subscriber has to hand back to it.
 */
export function majorVersionOf(ver: string): string {
  const dot = ver.indexOf('.');
  return dot < 0 ? ver : ver.slice(0, dot);
}
