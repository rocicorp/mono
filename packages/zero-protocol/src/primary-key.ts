import * as v from '../../shared/src/valita.ts';

export const primaryKeySchema = v.readonly(
  // oxlint-disable-next-line e18e/prefer-spread-syntax
  v.tuple([v.string()]).concat(v.array(v.string())),
);

export type PrimaryKey = v.Infer<typeof primaryKeySchema>;

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
