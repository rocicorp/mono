import {nullableVersionSchema} from 'reflect-protocol';
import {must} from 'shared/src/must.js';
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

  // The user ID of the user who was using this client.
  // This is optional because old records did not have this field.
  userID: valita.string().optional(),
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

/**
 * Deletes the client records and puts tombstones for them.
 */
export async function delClientRecords(
  clientIDs: ClientID[],
  storage: Storage,
): Promise<void> {
  const recordKeys = clientIDs.map(clientID => clientRecordKey(clientID));
  const records = await storage.getEntries(recordKeys, clientRecordSchema);
  await Promise.all([
    putClientTombstones(toClientRecordMap(records), storage),
    storage.delEntries(recordKeys),
  ]);
}

/** Deletes the client record and puts a tombstone for it. */
export async function delClientRecord(
  clientID: ClientID,
  storage: Storage,
): Promise<void> {
  const {userID} = must(await getClientRecord(clientID, storage));
  await Promise.all([
    putClientTombstone(clientID, userID, storage),
    storage.del(clientRecordKey(clientID)),
  ]);
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

const clientTombstonePrefix = 'clientTombstone/';

export function clientTombstoneKey(clientID: ClientID): string {
  return `${clientTombstonePrefix}${clientID}`;
}

function putClientTombstone(
  clientID: ClientID,
  userID: string | undefined,
  storage: Storage,
): Promise<void> {
  return storage.put(clientTombstoneKey(clientID), {userID});
}

function putClientTombstones(
  records: ClientRecordMap,
  storage: Storage,
): Promise<void> {
  const entries: Record<string, {userID?: string | undefined}> = {};
  for (const [clientID, {userID}] of records) {
    entries[clientTombstoneKey(clientID)] = {userID};
  }
  return storage.putEntries(entries);
}
