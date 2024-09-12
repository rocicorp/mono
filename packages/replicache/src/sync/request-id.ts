import {getNonCryptoRandomValues} from 'shared/src/random-values.js';

import type {ClientID} from './ids.js';

let sessionID = '';
function getSessionID() {
  if (sessionID === '') {
    const buf = new Uint8Array(4);
    getNonCryptoRandomValues(buf);
    sessionID = Array.from(buf, x => x.toString(16)).join('');
  }
  return sessionID;
}

const REQUEST_COUNTERS: Map<string, number> = new Map();

/**
 * Returns a new requestID of the form <client ID>-<session ID>-<request
 * count>. The request count enables one to find the request following or
 * preceding a given request. The sessionid scopes the request count, ensuring
 * the requestID is probabilistically unique across restarts (which is good
 * enough).
 */
export function newRequestID(clientID: ClientID): string {
  let counter = REQUEST_COUNTERS.get(clientID);
  if (!counter) {
    REQUEST_COUNTERS.set(clientID, 0);
    counter = 0;
  } else {
    counter++;
    REQUEST_COUNTERS.set(clientID, counter);
  }
  return `${clientID}-${getSessionID()}-${counter}`;
}
