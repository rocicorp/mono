import type {Enum} from '../../shared/src/enum.js';
import * as FormatVersion from './format-version-enum.js';

type FormatVersion = Enum<typeof FormatVersion>;

export function parseReplicacheFormatVersion(v: number): FormatVersion {
  if (v !== (v | 0) || v < FormatVersion.SDD || v > FormatVersion.Latest) {
    throw new Error(`Unsupported format version: ${v}`);
  }
  return v as FormatVersion;
}
