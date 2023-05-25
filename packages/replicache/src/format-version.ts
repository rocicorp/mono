export const enum FormatVersion {
  SDD = 4,
  DD31 = 5,
  // V6 added refreshHashes and persistHash to Client to fix ChunkNotFound errors
  V6 = 6,
  // V7 added sizeOfEntry to the BTree chunk data.
  V7 = 7,
  Latest = V7,
}

export function parseReplicacheFormatVersion(v: number): FormatVersion {
  if (v !== (v | 0) || v < FormatVersion.SDD || v > FormatVersion.Latest) {
    throw new Error(`Unsupported format version: ${v}`);
  }
  return v as FormatVersion;
}
