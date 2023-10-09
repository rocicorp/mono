import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import {moduleRefSchema} from './module.js';
import * as path from './path.js';

export const STABLE_RELEASE_CHANNEL = 'stable';
export const CANARY_RELEASE_CHANNEL = 'canary';

export const STANDARD_RELEASE_CHANNELS: readonly string[] = [
  STABLE_RELEASE_CHANNEL,
  CANARY_RELEASE_CHANNEL,
] as const;

export const standardReleaseChannelSchema = v.union(
  v.literal(STABLE_RELEASE_CHANNEL),
  v.literal(CANARY_RELEASE_CHANNEL),
);

// Defines the StandardReleaseChannels to which a newly created app should be limited
// to. Custom release channels are only for internal use.
export type StandardReleaseChannel = v.Infer<
  typeof standardReleaseChannelSchema
>;

export const serverSchema = v.object({
  major: v.number(),
  minor: v.number(),
  patch: v.number(),
  modules: v.array(moduleRefSchema),

  // The channels to which the server should be deployed to (unless there's
  // a newer version within a app deployment's compatible version range).
  //
  // Apps can only be created with a `StandardReleaseChannel` (i.e. "canary" and "stable"),
  // but custom channels can be arbitrarily created/used for pushing builds to particular
  // apps or sets of them. Note that custom channels should be used sparingly and
  // temporarily, as they run the risk of being missed in the standard release process.
  channels: v.array(v.string()),
});

export type Server = v.Infer<typeof serverSchema>;

export const serverDataConverter = firestoreDataConverter(serverSchema);

export const SERVER_COLLECTION = 'servers';

export function serverPath(version: string): string {
  return path.join(SERVER_COLLECTION, version);
}
