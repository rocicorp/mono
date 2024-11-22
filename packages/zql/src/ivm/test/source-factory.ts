import type {SchemaValue} from '../../../../zero-schema/src/table-schema.js';
import {MemorySource} from '../memory-source.js';
import type {Source} from '../source.js';

export type SourceFactory = (
  tableName: string,
  columns: Record<string, SchemaValue>,
  primaryKey: readonly [string, ...string[]],
) => Source;

export const createSource: SourceFactory = (
  tableName: string,
  columns: Record<string, SchemaValue>,
  primaryKey: readonly [string, ...string[]],
): Source => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const {sourceFactory} = globalThis as any;
  if (sourceFactory) {
    return sourceFactory(tableName, columns, primaryKey);
  }

  return new MemorySource(tableName, columns, primaryKey);
};
