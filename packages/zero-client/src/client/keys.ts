import {assert} from 'shared/src/asserts.js';
import type {EntityID} from 'zero-protocol/src/entity.js';

export const CLIENTS_KEY_PREFIX = 'c/';
export const DESIRED_QUERIES_KEY_PREFIX = 'd/';
export const GOT_QUERIES_KEY_PREFIX = 'g/';
export const ENTITIES_KEY_PREFIX = 'e/';

export function toClientsKey(clientID: string): string {
  return CLIENTS_KEY_PREFIX + clientID;
}

export function toDesiredQueriesKey(clientID: string, hash: string): string {
  return DESIRED_QUERIES_KEY_PREFIX + clientID + '/' + hash;
}

export function toGotQueriesKey(hash: string): string {
  return GOT_QUERIES_KEY_PREFIX + hash;
}

export function toEntitiesKey(entityType: string, entityID: EntityID): string {
  const entries = Object.entries(entityID);
  assert(entries.length > 0);
  entries.sort(([keyA], [keyB]) => (keyA < keyB ? -1 : 1));
  let idSegment = entries[0][1];
  for (let i = 1; i < entries.length; i++) {
    idSegment += '_' + entries[i][1];
  }
  return ENTITIES_KEY_PREFIX + entityType + '/' + idSegment;
}
