import {assert} from '../../../shared/src/asserts.js';
import type {EntityID} from '../../../zero-protocol/src/entity.js';

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

export function desiredQueriesPrefixForClient(clientID: string): string {
  return DESIRED_QUERIES_KEY_PREFIX + clientID + '/';
}

export function toGotQueriesKey(hash: string): string {
  return GOT_QUERIES_KEY_PREFIX + hash;
}

export function toEntitiesKey(entityType: string, entityID: EntityID): string {
  const idKeys = Object.keys(entityID);
  assert(idKeys.length > 0);
  // The common case of a non-composite primary key (i.e.
  // single entry entityID) is optimized to just use the single
  // id value.
  const idSegment =
    idKeys.length === 1
      ? entityID[idKeys[0]]
      : JSON.stringify(entityID, idKeys.sort());
  return ENTITIES_KEY_PREFIX + entityType + '/' + idSegment;
}
