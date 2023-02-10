import { nullableVersionSchema } from "./version";
import { z } from "zod";
import { ClientID } from "./client-state";
import { Storage } from "../storage/storage";

export const clientRecordSchema = z.object({
  lastMutationID: z.number(),
  baseCookie: nullableVersionSchema,
});

export type ClientRecord = z.infer<typeof clientRecordSchema>;

export const clientRecordPrefix = "client/";

export function clientRecordKey(clientID: ClientID): string {
  return `${clientRecordPrefix}${clientID}`;
}

export async function getClientRecord(
  clientID: ClientID,
  storage: Storage
): Promise<ClientRecord | undefined> {
  return await storage.get(clientRecordKey(clientID), clientRecordSchema);
}

export async function putClientRecord(
  clientID: ClientID,
  record: ClientRecord,
  storage: Storage
): Promise<void> {
  return await storage.put(clientRecordKey(clientID), record);
}
