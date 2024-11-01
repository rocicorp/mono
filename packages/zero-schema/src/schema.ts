import type {TableSchema} from './table-schema.js';

export type Schema = {
  readonly version: number;
  readonly tables: {readonly [table: string]: TableSchema};
};

export function createSchema<const S extends Schema>(schema: S): S {
  return schema as S;
}
