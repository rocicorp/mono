import {assert} from '../../shared/src/asserts.js';
import * as v from '../../shared/src/valita.js';
import {queriesPatchSchema} from './queries-patch.js';

/**
 * After opening a websocket the client waits for a `connected` message
 * from the server.  It then sends an `initConnection` message to the
 * server.  The server waits for the `initConnection` message before
 * beginning to send pokes to the newly connected client, so as to avoid
 * syncing lots of queries which are no longer desired by the client.
 */

export const connectedBodySchema = v.object({
  wsid: v.string(),
  timestamp: v.number().optional(),
});

export const connectedMessageSchema = v.tuple([
  v.literal('connected'),
  connectedBodySchema,
]);

const initConnectionBodySchema = v.object({
  desiredQueriesPatch: queriesPatchSchema,
});

export const initConnectionMessageSchema = v.tuple([
  v.literal('initConnection'),
  initConnectionBodySchema,
]);

export type ConnectedBody = v.Infer<typeof connectedBodySchema>;
export type ConnectedMessage = v.Infer<typeof connectedMessageSchema>;

export type InitConnectionBody = v.Infer<typeof initConnectionBodySchema>;
export type InitConnectionMessage = v.Infer<typeof initConnectionMessageSchema>;

export function encodeProtocols(
  initConnectionMessage: InitConnectionMessage,
  authToken?: string | undefined,
) {
  const protocols: string[] = [btoa(JSON.stringify(initConnectionMessage))];
  if (authToken) {
    protocols.push(authToken);
  }
  return encodeURIComponent(protocols.join(','));
}

export function decodeProtocols(
  secProtocol: string,
): [initConnectionMessage: string, maybeAuthToken: string | undefined] {
  const ret = decodeURIComponent(secProtocol)
    .split(',')
    // base64 encoding the JSON before URI encoding it results in a smaller payload.
    .map((s, i) => (i === 0 ? atob(s) : s));

  assert(ret.length === 1 || ret.length === 2);
  return ret.length === 1 ? [ret[0], undefined] : [ret[0], ret[1]];
}
