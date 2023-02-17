import {z} from 'zod';

export const versionSchema = z.number();
export const nullableVersionSchema = z.union([versionSchema, z.null()]);

export type Version = z.infer<typeof versionSchema>;
export type NullableVersion = z.infer<typeof nullableVersionSchema>;
