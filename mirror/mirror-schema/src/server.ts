import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import {moduleRefSchema} from './module.js';
import * as path from './path.js';

export const releaseChannelSchema = v.union(
  v.literal('canary'),
  v.literal('stable'),
);

export type ReleaseChannel = v.Infer<typeof releaseChannelSchema>;

export const serverSchema = v.object({
  major: v.number(),
  minor: v.number(),
  patch: v.number(),
  modules: v.array(moduleRefSchema),
  channel: releaseChannelSchema,
});

export type Server = v.Infer<typeof serverSchema>;

export const serverDataConverter = firestoreDataConverter(serverSchema);

export const SERVER_COLLECTION = 'servers';

export function serverPath(version: string): string {
  return path.join(SERVER_COLLECTION, version);
}
