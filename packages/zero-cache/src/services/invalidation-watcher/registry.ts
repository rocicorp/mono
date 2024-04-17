import type {InvalidationWatcher} from './invalidation-watcher.js';

export interface InvalidationWatcherRegistry {
  /**
   * Gets the InvalidationWatcher running in the current Service Runner.
   */
  getInvalidationWatcher(): Promise<InvalidationWatcher>;
}
