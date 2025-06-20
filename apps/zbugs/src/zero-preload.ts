import {Zero} from '@rocicorp/zero';
import {type Schema} from '../shared/schema.ts';
import {type Mutators} from '../shared/mutators.ts';
import {CACHE_FOREVER} from './query-cache-policy.ts';
import {allLabels, allUsers, issuePreload} from '../shared/queries.ts';

export function preload(z: Zero<Schema, Mutators>) {
  // Preload all issues and first 10 comments from each.
  z.preload(issuePreload(z.userID), CACHE_FOREVER);
  z.preload(allUsers(), CACHE_FOREVER);
  z.preload(allLabels(), CACHE_FOREVER);
}
