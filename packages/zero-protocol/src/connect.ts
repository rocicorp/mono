import * as v from '../../shared/src/valita.ts';
import {clientSchemaSchema} from './client-schema.ts';
import {deleteClientsBodySchema} from './delete-clients.ts';
import {upQueriesPatchSchema} from './queries-patch.ts';

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

const userPushParamsSchema = v.object({
  queryParams: v.record(v.string()).optional(),
});

const initConnectionBodySchema = v.object({
  desiredQueriesPatch: upQueriesPatchSchema,
  clientSchema: clientSchemaSchema.optional(),
  deleted: deleteClientsBodySchema.optional(),
  userPushParams: userPushParamsSchema.optional(),

  /**
   * `activeClients` is an optional array of client IDs that are currently active
   * in the client group. This is used to inform the server about the clients
   * that are currently active (aka running, aka alive), so it can inactive
   * queries from inactive clients.
   */
  activeClients: v.array(v.string()).optional(),
});

export const initConnectionMessageSchema = v.tuple([
  v.literal('initConnection'),
  initConnectionBodySchema,
]);

export type ConnectedBody = v.Infer<typeof connectedBodySchema>;
export type ConnectedMessage = v.Infer<typeof connectedMessageSchema>;
export type UserPushParams = v.Infer<typeof userPushParamsSchema>;

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
