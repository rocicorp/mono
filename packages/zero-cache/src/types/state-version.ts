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
