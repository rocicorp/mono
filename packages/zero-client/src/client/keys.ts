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

export function toEntitiesKey(key: string): string {
  return ENTITIES_KEY_PREFIX + key;
}
