import * as s from "superstruct";
import type { Storage } from "../storage/storage.js";

export const versionSchema = s.number();
export const nullableVersionSchema = s.union([versionSchema, s.literal(null)]);

export type Version = s.Infer<typeof versionSchema>;
export type NullableVersion = s.Infer<typeof nullableVersionSchema>;

export const versionKey = "version";

export async function putVersion(
  version: Version,
  storage: Storage
): Promise<void> {
  await storage.put(versionKey, version);
}

export async function getVersion(
  storage: Storage
): Promise<Version | undefined> {
  return await storage.get(versionKey, versionSchema);
}
