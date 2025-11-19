import type {ZeroBugs} from '../shared/zero-type.ts';
import {CACHE_PRELOAD} from './query-cache-policy.ts';

export function preload(z: ZeroBugs, projectName: string) {
  // Preload all issues and first 10 comments from each.
  const q = z.query.issuePreloadV2({userID: z.userID, projectName});
  z.preload(q, CACHE_PRELOAD);
  z.preload(z.query.allUsers(), CACHE_PRELOAD);
  z.preload(z.query.allLabels(), CACHE_PRELOAD);
  z.preload(z.query.allProjects(), CACHE_PRELOAD);
}
