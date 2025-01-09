import {normalizeSchema} from './normalized-schema.js';
import {type TableSchema} from './table-schema.js';

export type Schema = {
  readonly version: number;
  readonly tables: {readonly [table: string]: TableSchema};
};

export function createSchema<const S extends Schema>(schema: S): S {
  normalizeSchema(schema);
  return schema as S;
}
