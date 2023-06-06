import * as v from 'shared/valita.js';
import * as path from './path.js';

export const releaseChannelSchema = v.union(
  v.literal('canary'),
  v.literal('stable'),
);

export const serverSchema = v.object({
  major: v.number(),
  minor: v.number(),
  patch: v.number(),
  module: v.string(),
  channel: releaseChannelSchema,
});

export type Server = v.Infer<typeof serverSchema>;

export const SERVER_COLLECTION = 'servers';

export function serverPath(serverID: string): string {
  return path.join(SERVER_COLLECTION, serverID);
}
