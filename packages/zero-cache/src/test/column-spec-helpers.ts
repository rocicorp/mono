/**
 * Test helpers for working with ColumnSpec objects during the migration
 * from pipe-delimited dataType strings to structured ColumnMetadata.
 */

import type {ColumnMetadata} from '../services/change-source/column-metadata.ts';
import type {ColumnSpec} from '../db/specs.ts';

/**
 * Helper to parse pipe-delimited dataType string into ColumnMetadata for tests.
 */
export function parseDataTypeToMetadata(
  dataType: string,
  characterMaximumLength?: number | null,
): ColumnMetadata {
  // Parse pipe-delimited attributes
  const hasNotNull = dataType.includes('|NOT_NULL');
  const hasTextEnum = dataType.includes('|TEXT_ENUM');
  const hasTextArray = dataType.includes('|TEXT_ARRAY');

  // Extract base type (everything before first |)
  const delimIndex = dataType.indexOf('|');
  let baseType = delimIndex > 0 ? dataType.substring(0, delimIndex) : dataType;

  // Check for array notation
  const hasArrayBrackets = baseType.includes('[]') || hasTextArray;

  // If it has TEXT_ARRAY but no [], add []
  if (hasTextArray && !baseType.includes('[]')) {
    baseType = baseType + '[]';
  }

  return {
    upstreamType: baseType,
    isNotNull: hasNotNull,
    isEnum: hasTextEnum,
    isArray: hasArrayBrackets,
    characterMaxLength: characterMaximumLength ?? null,
  };
}

/**
 * Helper to create a ColumnSpec with the new metadata format from old test data.
 */
export function makeColumnSpec(spec: {
  pos: number;
  dataType: string;
  characterMaximumLength?: number | null;
  notNull?: boolean | null;
  dflt?: string | null;
  pgTypeClass?: string;
  elemPgTypeClass?: string | null;
}): ColumnSpec {
  return {
    pos: spec.pos,
    metadata: parseDataTypeToMetadata(
      spec.dataType,
      spec.characterMaximumLength,
    ),
    notNull: spec.notNull ?? null,
    dflt: spec.dflt ?? null,
    pgTypeClass: spec.pgTypeClass as any,
    elemPgTypeClass: spec.elemPgTypeClass as any,
  };
}
