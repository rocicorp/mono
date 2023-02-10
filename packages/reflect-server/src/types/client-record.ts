import * as s from 'superstruct';
import {nullableVersionSchema} from './version.js';
import type {ClientID} from './client-state.js';
import type {Storage} from '../storage/storage.js';

export const clientRecordSchema = s.type({
  lastMutationID: s.number(),
  baseCookie: nullableVersionSchema,
});

export type ClientRecord = s.Infer<typeof clientRecordSchema>;

export const clientRecordPrefix = 'client/';

export function clientRecordKey(clientID: ClientID): string {
  return `${clientRecordPrefix}${clientID}`;
}

export function getClientRecord(
  clientID: ClientID,
  storage: Storage,
): Promise<ClientRecord | undefined> {
  return storage.get(clientRecordKey(clientID), clientRecordSchema);
}

export function putClientRecord(
  clientID: ClientID,
  record: ClientRecord,
  storage: Storage,
): Promise<void> {
  return storage.put(clientRecordKey(clientID), record);
}
