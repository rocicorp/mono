import * as s from 'superstruct';
import {nullableVersionSchema} from 'reflect-protocol';
import type {ClientID} from './client-state.js';
import type {Storage} from '../storage/storage.js';

export const clientRecordSchema = s.type({
  clientGroupID: s.string(),
  baseCookie: nullableVersionSchema,
  lastMutationID: s.number(),
  // Room version that last updated lastMutationID for this client
  // or null if no mutations have been applied for this client
  // (i.e. lastMutationID is 0).
  lastMutationIDVersion: nullableVersionSchema,
});

export type ClientRecord = s.Infer<typeof clientRecordSchema>;
export type ClientRecordMap = Map<ClientID, ClientRecord>;

// Note: old (pre-dd31, conceptually V0) client records were stored with key
// prefix "client/""
export const clientRecordPrefix = 'clientV1/';

export function clientRecordKey(clientID: ClientID): string {
  return `${clientRecordPrefix}${clientID}`;
}

export function getClientRecord(
  clientID: ClientID,
  storage: Storage,
): Promise<ClientRecord | undefined> {
  return storage.get(clientRecordKey(clientID), clientRecordSchema);
}

export async function listClientRecords(
  storage: Storage,
): Promise<ClientRecordMap> {
  const withPrefix = await storage.list(
    {prefix: clientRecordPrefix},
    clientRecordSchema,
  );
  const clientRecords = new Map();
  for (const [key, record] of withPrefix) {
    clientRecords.set(key.substring(clientRecordPrefix.length), record);
  }
  return clientRecords;
}

export function putClientRecord(
  clientID: ClientID,
  record: ClientRecord,
  storage: Storage,
): Promise<void> {
  return storage.put(clientRecordKey(clientID), record);
}
