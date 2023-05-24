export const REPLICACHE_FORMAT_VERSION_SDD = 4;

export const REPLICACHE_FORMAT_VERSION_DD31 = 5;

// V6 added refreshHashes and persistHash to Client to fix ChunkNotFound errors
export const REPLICACHE_FORMAT_VERSION_V6 = 6;

// V7 added sizeOfEntry to the BTree chunk data.
export const REPLICACHE_FORMAT_VERSION_V7 = 7;

export const REPLICACHE_FORMAT_VERSION = REPLICACHE_FORMAT_VERSION_V7;

export type ReplicacheFormatVersion =
  | typeof REPLICACHE_FORMAT_VERSION_SDD
  | typeof REPLICACHE_FORMAT_VERSION_DD31
  | typeof REPLICACHE_FORMAT_VERSION_V6
  | typeof REPLICACHE_FORMAT_VERSION_V7;

export function parseReplicacheFormatVersion(
  v: number,
): ReplicacheFormatVersion {
  if (
    v !== (v | 0) ||
    v < REPLICACHE_FORMAT_VERSION_SDD ||
    v > REPLICACHE_FORMAT_VERSION
  ) {
    throw new Error(`Unsupported format version: ${v}`);
  }
  return v as ReplicacheFormatVersion;
}
