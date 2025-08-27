import {Zero} from '@rocicorp/zero';
import {type Schema} from '../shared/schema.ts';
import {type Mutators} from '../shared/mutators.ts';
import {CACHE_PRELOAD} from './query-cache-policy.ts';
import {queries} from '../shared/queries.ts';
import type {AuthData} from '../shared/auth.ts';

export function preload(auth: AuthData | undefined, z: Zero<Schema, Mutators>) {
  // Preload all issues and first 10 comments from each.
  z.preload(queries.issuePreload(auth, z.userID), CACHE_PRELOAD);
  z.preload(queries.allUsers(), CACHE_PRELOAD);
  z.preload(queries.allLabels(), CACHE_PRELOAD);
}
