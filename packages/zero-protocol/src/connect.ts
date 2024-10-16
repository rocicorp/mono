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

export function encodeSecProtocols(
  initConnectionMessage: InitConnectionMessage | undefined,
  authToken: string | undefined,
) {
  const protocols = {
    initConnectionMessage,
    authToken,
  };
  // base64 encoding the JSON before URI encoding it results in a smaller payload.
  return encodeURIComponent(btoa(JSON.stringify(protocols)));
}

export function decodeSecProtocols(secProtocol: string): {
  initConnectionMessage: InitConnectionMessage | undefined;
  authToken: string | undefined;
} {
  return JSON.parse(atob(decodeURIComponent(secProtocol)));
}
