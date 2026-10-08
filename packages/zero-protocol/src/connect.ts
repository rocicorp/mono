import {fromBase64url, toBase64url} from '../../shared/src/base64.ts';
import {
  packBooleanMap,
  unpackBooleanMap,
} from '../../shared/src/packed-boolean-map.ts';
import * as v from '../../shared/src/valita.ts';
import {clientSchemaSchema} from './client-schema.ts';
import {deleteClientsBodySchema} from './delete-clients.ts';
import {FeatureFlag} from './feature-flag.ts';
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

const initConnectionBodySchema = v.object({
  desiredQueriesPatch: upQueriesPatchSchema,
  // As the schema can be large, client only sends when it does not have a
  // server snapshot (i.e. a snapshot with a cookie).  Once it has a server
  // snapshot it will assume the zero-cache already has the schema for this
  // client's client group in the CVR store.
  clientSchema: clientSchemaSchema.optional(),
  deleted: deleteClientsBodySchema.optional(),
  // parameters to configure the mutate endpoint
  userPushURL: v.string().optional(),
  userPushHeaders: v.record(v.string()).optional(),
  // parameters to configure the query endpoint
  userQueryURL: v.string().optional(),
  userQueryHeaders: v.record(v.string()).optional(),

  /**
   * `activeClients` is an optional array of client IDs that are currently active
   * in the client group. This is used to inform the server about the clients
   * that are currently active (aka running, aka alive), so it can inactive
   * queries from inactive clients.
   */
  activeClients: v.array(v.string()).optional(),
  /** W3C traceparent header for distributed tracing. */
  traceparent: v.string().optional(),
});

export const initConnectionMessageSchema = v.tuple([
  v.literal('initConnection'),
  initConnectionBodySchema,
]);

export type ConnectedBody = v.Infer<typeof connectedBodySchema>;
export type ConnectedMessage = v.Infer<typeof connectedMessageSchema>;
export type InitConnectionBody = v.Infer<typeof initConnectionBodySchema>;
export type InitConnectionMessage = v.Infer<typeof initConnectionMessageSchema>;

/**
 * The feature flags a client sent. A missing flag means the server's default.
 */
export type FeatureFlags = ReadonlyMap<FeatureFlag, boolean>;

// The connect URL parameter that carries the feature flags: a byte array with
// two bits per flag, the low bit saying the client sent the flag and the high
// bit its value (see packed-boolean-map.ts), in unpadded base64url.
const FEATURE_FLAGS_PARAM = 'f';

/**
 * Adds the client's feature flags to the connect URL: every flag this client
 * knows, turned on.
 */
export function setFeatureFlags(params: URLSearchParams): void {
  const flags = new Map(Object.values(FeatureFlag).map(flag => [flag, true]));
  params.set(FEATURE_FLAGS_PARAM, toBase64url(packBooleanMap(flags)));
}

/**
 * Reads the feature flags a client sent in the connect URL. Flags this server
 * doesn't know are dropped, and a malformed value counts as no flags, so newer
 * clients can always connect.
 */
export function getFeatureFlags(params: URLSearchParams): FeatureFlags {
  let bytes: Uint8Array;
  try {
    bytes = fromBase64url(params.get(FEATURE_FLAGS_PARAM) ?? '');
  } catch {
    return new Map();
  }
  return unpackBooleanMap(bytes, Object.values(FeatureFlag));
}

export function encodeSecProtocols(
  initConnectionMessage: InitConnectionMessage | undefined,
  authToken: string | undefined,
): string {
  const protocols = {
    initConnectionMessage,
    authToken,
  };
  // WS sec protocols needs to be URI encoded. To save space, we base64 encode
  // the JSON before URI encoding it. But InitConnectionMessage can contain
  // arbitrary unicode strings, so we need to encode the JSON as UTF-8 first.
  // Phew!
  const bytes = new TextEncoder().encode(JSON.stringify(protocols));

  // Convert bytes to string without spreading all bytes as arguments
  // to avoid "Maximum call stack size exceeded" error with large data
  const s = Array.from(bytes, byte => String.fromCharCode(byte)).join('');

  return encodeURIComponent(btoa(s));
}

export function decodeSecProtocols(secProtocol: string): {
  initConnectionMessage: InitConnectionMessage | undefined;
  authToken: string | undefined;
} {
  const binString = atob(decodeURIComponent(secProtocol));
  const bytes = Uint8Array.from(binString, c => c.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes));
}
