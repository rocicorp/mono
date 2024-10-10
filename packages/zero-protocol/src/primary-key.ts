import * as v from '../../shared/src/valita.js';
import type {PrimaryKey} from '../../zql/src/zql/ivm/schema.js';

// TODO: It would be cleaner to not have zero-protocol depend on zql. This means
// that we would need to move the ast types out of zql and into zero-protocol
// (or into a different package that both depend on).
export type {PrimaryKey};

export const primaryKeySchema = v
  .array(v.string())
  .assert(arr => arr.length > 0) as v.Type<
  readonly string[]
> as v.Type<PrimaryKey>;

export const primaryKeyValueSchema = v.union(
  v.string(),
  v.number(),
  v.boolean(),
);

export type PrimaryKeyValue = v.Infer<typeof primaryKeyValueSchema>;

export const primaryKeyValueRecordSchema = v.readonlyRecord(
  primaryKeyValueSchema,
);

export type PrimaryKeyValueRecord = v.Infer<typeof primaryKeyValueRecordSchema>;
