import * as v from '@badrap/valita';

export const versionSchema = v.number();
export const nullableVersionSchema = v.union(versionSchema, v.null());

export type Version = v.Infer<typeof versionSchema>;
export type NullableVersion = v.Infer<typeof nullableVersionSchema>;
