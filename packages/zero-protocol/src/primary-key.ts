import * as v from '../../shared/src/valita.ts';

export const primaryKeySchema = v.readonly(
  v.tuple([v.string()]).concat(v.array(v.string())),
);

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
