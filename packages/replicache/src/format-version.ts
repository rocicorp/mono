import * as FormatVersion from './format-version-enum.js';

export function parseReplicacheFormatVersion(v: number): FormatVersion.Type {
  if (v !== (v | 0) || v < FormatVersion.SDD || v > FormatVersion.Latest) {
    throw new Error(`Unsupported format version: ${v}`);
  }
  return v as FormatVersion.Type;
}
