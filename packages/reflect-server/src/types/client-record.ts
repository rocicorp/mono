import {nullableVersionSchema} from 'reflect-protocol';
import * as valita from 'shared/src/valita.js';
import type {Storage} from '../storage/storage.js';
import type {ClientID} from './client-state.js';

export const clientRecordSchema = valita.object({
  clientGroupID: valita.string(),
  baseCookie: nullableVersionSchema,
  lastMutationID: valita.number(),
  // Room version that last updated lastMutationID for this client
  // or null if no mutations have been applied for this client
  // (i.e. lastMutationID is 0).
  lastMutationIDVersion: nullableVersionSchema,

  // Used for garbage collection of old clients.
  lastSeen: valita.number().optional(),

  // This gets sent by the client (browser) when it sends the close beacon.
  lastMutationIDAtClose: valita.number().optional(),
});

export type ClientRecord = valita.Infer<typeof clientRecordSchema>;
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
  const entries = await storage.list(
    {prefix: clientRecordPrefix},
    clientRecordSchema,
  );
  return toClientRecordMap(entries);
}

export async function getClientRecords(
  clientIDs: ClientID[],
  storage: Storage,
): Promise<ClientRecordMap> {
  const entries = await storage.getEntries(
    clientIDs.map(clientRecordKey),
    clientRecordSchema,
  );
  return toClientRecordMap(entries);
}

export function putClientRecord(
  clientID: ClientID,
  record: ClientRecord,
  storage: Storage,
): Promise<void> {
  return storage.put(clientRecordKey(clientID), record);
}

export function delClientRecords(
  clientIDs: ClientID[],
  storage: Storage,
): Promise<void> {
  // TODO(arv): Create tombstones for the deleted client records.
  return storage.delEntries(
    clientIDs.map(clientID => clientRecordKey(clientID)),
  );
}

export function delClientRecord(
  clientID: ClientID,
  storage: Storage,
): Promise<void> {
  // TODO(arv): Create tombstones for the deleted client records.
  return storage.del(clientRecordKey(clientID));
}

function toClientRecordMap(
  entries: Map<string, ClientRecord>,
): ClientRecordMap {
  const clientRecords = new Map();
  for (const [key, record] of entries) {
    clientRecords.set(key.substring(clientRecordPrefix.length), record);
  }
  return clientRecords;
}
