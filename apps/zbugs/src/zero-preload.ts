import {Zero} from '@rocicorp/zero';
import {type Mutators} from '../shared/mutators.ts';
import {queries} from '../shared/queries.ts';
import {type Schema} from '../shared/schema.ts';
import {CACHE_PRELOAD} from './query-cache-policy.ts';

export function preload(z: Zero<Schema, Mutators>) {
  // Preload all issues and first 10 comments from each.
  z.preload(queries.issuePreload(z.userID), CACHE_PRELOAD);
  z.preload(queries.allUsers(), CACHE_PRELOAD);
  z.preload(queries.allLabels(), CACHE_PRELOAD);
  z.preload(queries.allProjects(), CACHE_PRELOAD);
}
