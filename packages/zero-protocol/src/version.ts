import * as v from '../../shared/src/valita.ts';

export const versionSchema = v.string();
export const nullableVersionSchema = v.union(versionSchema, v.null());

export type Version = v.Infer<typeof versionSchema>;
export type NullableVersion = v.Infer<typeof nullableVersionSchema>;
