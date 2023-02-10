import { jsonSchema } from "../protocol/json";
import { versionSchema } from "./version";
import { Storage } from "../storage/storage";
import { z } from "zod";

export const userValueSchema = z.object({
  version: versionSchema,
  deleted: z.boolean(),
  value: jsonSchema,
});

export type UserValue = z.infer<typeof userValueSchema>;

export const userValuePrefix = "user/";

export function userValueKey(key: string): string {
  return `${userValuePrefix}${key}`;
}

export async function getUserValue(
  key: string,
  storage: Storage
): Promise<UserValue | undefined> {
  return await storage.get(userValueKey(key), userValueSchema);
}

export async function putUserValue(
  key: string,
  value: UserValue,
  storage: Storage
): Promise<void> {
  return await storage.put(userValueKey(key), value);
}
