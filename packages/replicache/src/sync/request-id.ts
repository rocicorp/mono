/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {getNonCryptoRandomValues} from '../../../shared/src/random-values.ts';
import type {ClientID} from './ids.ts';

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
  const counter = REQUEST_COUNTERS.get(clientID) ?? 0;
  REQUEST_COUNTERS.set(clientID, counter + 1);
  return `${clientID}-${getSessionID()}-${counter}`;
}
