import type {Zero} from '@rocicorp/zero';
import {queries} from '../shared/queries.ts';
import {CACHE_PRELOAD} from './query-cache-policy.ts';

export function preload(z: Zero, projectName: string) {
  // Preload all issues and first 10 comments from each.
  z.preload(queries.issuePreloadV2({projectName}), CACHE_PRELOAD);
  z.preload(queries.allUsers(), CACHE_PRELOAD);
  z.preload(queries.allLabels(), CACHE_PRELOAD);
  z.preload(queries.allProjects(), CACHE_PRELOAD);
}
