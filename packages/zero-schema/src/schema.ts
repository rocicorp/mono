import {normalizeSchema} from './normalized-schema.js';
import {type TableSchema} from './table-schema.js';

export type Schema = {
  readonly version: number;
  readonly tables: {readonly [table: string]: TableSchema};
};

export function createSchema<const S extends Schema>(schema: S): S {
  // normalizeSchema will throw if the schema is invalid.
  normalizeSchema(schema);
  // We still want to return s to cause less surprises.
  return schema as S;
}
