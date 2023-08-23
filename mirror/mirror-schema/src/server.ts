import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import {moduleRefSchema} from './module.js';
import * as path from './path.js';

export const CANARY_RELEASE_CHANNEL = 'canary';
export const STABLE_RELEASE_CHANNEL = 'stable';

export const serverSchema = v.object({
  major: v.number(),
  minor: v.number(),
  patch: v.number(),
  modules: v.array(moduleRefSchema),

  // The channels to which the server should be deployed to (unless there's
  // a newer version within a app deployment's compatible version range).
  //
  // The standard channel names are "canary" and "stable", but they can be arbitrarily
  // created/used for pushing builds to particular apps or sets of them. Note that
  // custom channels should be used sparingly and temporarily, as they run the risk
  // of being missed in the standard canary / stable release process.
  channels: v.array(v.string()),
});

export type Server = v.Infer<typeof serverSchema>;

export const serverDataConverter = firestoreDataConverter(serverSchema);

export const SERVER_COLLECTION = 'servers';

export function serverPath(version: string): string {
  return path.join(SERVER_COLLECTION, version);
}
