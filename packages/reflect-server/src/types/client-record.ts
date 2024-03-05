import {nullableVersionSchema} from 'reflect-protocol';
import {wrapIterable} from 'shared/src/iterables.js';
import * as valita from 'shared/src/valita.js';
import type {Storage} from '../storage/storage.js';
import type {ClientGroupID, ClientID} from './client-state.js';

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

  // Whether the client has been deleted due to it being collected
  deleted: valita.boolean().optional(),
});

export type ClientRecord = valita.Infer<typeof clientRecordSchema>;
export type ClientRecordMap = Map<ClientID, ClientRecord>;

// Note: old (pre-dd31, conceptually V0) client records were stored with key
// prefix "client/""
export const clientRecordPrefix = 'clientV1/';

export function clientRecordKey(clientID: ClientID): string {
  return `${clientRecordPrefix}${clientID}`;
}

export const enum IncludeDeleted {
  Exclude,
  Include,
}

export async function getClientRecord(
  clientID: ClientID,
  includeDeleted: IncludeDeleted,
  storage: Storage,
): Promise<ClientRecord | undefined> {
  const record = await storage.get(
    clientRecordKey(clientID),
    clientRecordSchema,
  );
  if (includeDeleted === IncludeDeleted.Exclude && record?.deleted) {
    return undefined;
  }

  return record;
}

export async function listClientRecords(
  includeDeleted: IncludeDeleted,
  storage: Storage,
): Promise<ClientRecordMap> {
  const entries = await storage.list(
    {prefix: clientRecordPrefix},
    clientRecordSchema,
  );
  return convertToClientRecordMapAndFilterDeleted(entries, includeDeleted);
}

export async function listClientRecordsForClientGroup(
  clientGroupID: ClientGroupID,
  includeDeleted: IncludeDeleted,
  storage: Storage,
): Promise<ClientRecordMap> {
  // TODO(arv): Restructure the key space to make this more efficient.
  const entries = await storage.list(
    {prefix: clientRecordPrefix},
    clientRecordSchema,
  );
  return new Map(
    wrapIterable(entries)
      .filter(includeDeletedPredicate(includeDeleted))
      .filter(entry => entry[1].clientGroupID === clientGroupID)
      .map(convertKeyToClientID),
  );
}

export async function getClientRecords(
  clientIDs: Iterable<ClientID>,
  includedDeleted: IncludeDeleted,
  storage: Storage,
): Promise<ClientRecordMap> {
  const entries = await storage.getEntries(
    Array.from(clientIDs, clientRecordKey),
    clientRecordSchema,
  );
  return convertToClientRecordMapAndFilterDeleted(entries, includedDeleted);
}

export function putClientRecord(
  clientID: ClientID,
  record: ClientRecord,
  storage: Storage,
): Promise<void> {
  return storage.put(clientRecordKey(clientID), record);
}

function includeDeletedPredicate(includedDeleted: IncludeDeleted) {
  return (entry: [string, ClientRecord]) =>
    !(includedDeleted === IncludeDeleted.Exclude && entry[1].deleted);
}

function convertKeyToClientID<T>(entry: [key: string, value: T]): [string, T] {
  return [entry[0].substring(clientRecordPrefix.length), entry[1]];
}

function convertToClientRecordMapAndFilterDeleted(
  entries: Map<string, ClientRecord>,
  includeDeleted: IncludeDeleted,
): ClientRecordMap {
  return new Map(
    wrapIterable(entries)
      .filter(includeDeletedPredicate(includeDeleted))
      .map(convertKeyToClientID),
  );
}

/**
 * Marks the client records as deleted.
 */
export function deleteClientRecords(
  records: Map<ClientID, ClientRecord>,
  storage: Storage,
): Promise<void> {
  const entries: Record<ClientID, ClientRecord> = {};
  for (const [clientID, record] of records) {
    entries[clientRecordKey(clientID)] = {...record, deleted: true};
  }
  return storage.putEntries(entries);
}

/**
 * Marks the client record as deleted.
 */
export function deleteClientRecord(
  clientID: ClientID,
  record: ClientRecord,
  storage: Storage,
): Promise<void> {
  return putClientRecord(
    clientID,
    {
      ...record,
      deleted: true,
    },
    storage,
  );
}
