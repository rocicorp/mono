import type {Zero} from '@rocicorp/zero';
import type {AuthData} from '../shared/auth.ts';
import type {Mutators} from '../shared/mutators.ts';
import type {Queries} from '../shared/queries.ts';
import type {Schema} from '../shared/schema.ts';
import {CACHE_PRELOAD} from './query-cache-policy.ts';

export function preload(
  z: Zero<Schema, Mutators, AuthData | undefined, Queries>,
  projectName: string,
) {
  // Preload all issues and first 10 comments from each.
  const q = z.query.issuePreloadV2({userID: z.userID, projectName});
  z.preload(q, CACHE_PRELOAD);
  z.preload(z.query.allUsers(), CACHE_PRELOAD);
  z.preload(z.query.allLabels(), CACHE_PRELOAD);
  z.preload(z.query.allProjects(), CACHE_PRELOAD);
}
