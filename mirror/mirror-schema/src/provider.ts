import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import * as path from './path.js';

export const zoneSchema = v.object({
  id: v.string(),
  name: v.string(), // e.g. 'reflect.net'
});

/**
 * A Provider hosts an App's workers and hostname(s). Each provider is associated
 * with an api token that is stored in the Secret manager, and a namespace in which
 * Workers (for Platforms) are run.
 *
 * Each Mirror stack has a "default" Provider (id = "default") in which Teams and
 * Apps are created by default. When implemented, creation of an App in a non-default
 * Provider (e.g. "monday") will require some sort of user or team-based authorization
 * scheme.
 */
export const providerSchema = v.object({
  accountID: v.string(), // Cloudfare Account ID
  dispatchNamespace: v.string(),

  // The zone in which workers are hosted by default. It is named "defaultZone" to
  // allow for supporting multiple domains in the future.
  defaultZone: zoneSchema,

  defaultMaxApps: v.number(),
});

export type Provider = v.Infer<typeof providerSchema>;

export const providerDataConverter = firestoreDataConverter(providerSchema);

export const PROVIDER_COLLECTION = 'providers';

export const DEFAULT_PROVIDER_ID = 'default';

export function providerPath(id: string): string {
  return path.join(PROVIDER_COLLECTION, id);
}
