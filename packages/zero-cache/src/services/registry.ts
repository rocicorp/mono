import type {Replicator} from './replicator/replicator.js';
import type {ViewSyncer} from './view-syncer/view-syncer.js';

export interface ServiceRegistry {
  getReplicator(): Replicator;

  getViewSyncer(id: string): ViewSyncer;
}
