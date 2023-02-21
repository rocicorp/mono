import * as s from 'superstruct';

export const versionSchema = s.number();
export const nullableVersionSchema = s.union([versionSchema, s.literal(null)]);

export type Version = s.Infer<typeof versionSchema>;
export type NullableVersion = s.Infer<typeof nullableVersionSchema>;
