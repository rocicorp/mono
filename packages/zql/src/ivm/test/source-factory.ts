import type {LogContext} from '@rocicorp/logger';
import type {LogConfig} from '../../../../otel/src/log-options.ts';
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
  const {sourceFactory} = globalThis as {
    sourceFactory?: SourceFactory;
  };
  if (sourceFactory) {
    return sourceFactory(tableName, columns, primaryKey);
  }

  return new MemorySource(tableName, columns, primaryKey);
};
