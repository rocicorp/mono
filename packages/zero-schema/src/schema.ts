import {assert} from '../../shared/src/asserts.js';
import type {TableSchema} from './table-schema.js';

export type Schema = {
  readonly version: number;
  readonly tables: {readonly [table: string]: TableSchema};
};

export function createSchema<const S extends Schema>(schema: S): S {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    assert(
      tableName === table.tableName,
      `createSchema tableName mismatch, expected ${tableName} === ${table.tableName}`,
    );
  }
  return schema as S;
}
