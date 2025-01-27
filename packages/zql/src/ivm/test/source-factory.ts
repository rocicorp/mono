import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../../zero-schema/src/table-schema.ts';
import {MemorySource} from '../memory-source.ts';
import type {Source} from '../source.ts';

export type SourceFactory = (
  tableName: string,
  columns: Record<string, SchemaValue>,
  primaryKey: PrimaryKey,
) => Source;

export const createSource: SourceFactory = (
  tableName: string,
  columns: Record<string, SchemaValue>,
  primaryKey: PrimaryKey,
): Source => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const {sourceFactory} = globalThis as any;
  if (sourceFactory) {
    return sourceFactory(tableName, columns, primaryKey);
  }

  return new MemorySource(tableName, columns, primaryKey);
};
