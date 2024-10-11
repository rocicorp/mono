import * as v from '../../shared/src/valita.js';
import type {Row, Value} from '../../zql/src/zql/ivm/data.js';

export type {Row, Value};

export const valueSchema: v.Type<Value> = v.union(
  v.null(),
  v.boolean(),
  v.number(),
  v.string(),
  v.undefined(),
);

export const rowSchema: v.Type<Row> = v.record(valueSchema);
