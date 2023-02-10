let sessionID = '';
function getSessionID() {
  if (sessionID === '') {
    const buf = new Uint8Array(4);
    crypto.getRandomValues(buf);
    sessionID = Array.from(buf, x => x.toString(16)).join('');
  }
  return sessionID;
}

const REQUEST_COUNTERS: Map<string, number> = new Map();

/**
 * Returns a new request_id of the form <clientid>-<sessionid>-<request
 * count>. The request count enables one to find the request following or
 * preceeding a given request. The sessionid scopes the request count, ensuring
 * the request_id is probabilistically unique across restarts (which is good
 * enough).
 */
export function newRequestID(clientID: string): string {
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
