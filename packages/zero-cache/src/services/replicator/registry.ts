import type {Replicator} from './replicator.js';

export interface ReplicatorRegistry {
  /**
   * Gets the global Replicator.
   *
   * In v0, everything is running in a single ServiceRunnerDO and thus this will always be
   * an in memory object.
   *
   * When sharding is added, a stub object that communicates with the Replicator in
   * another DO (via rpc / websocket) may be returned.
   *
   * Note that callers should be wary of caching the returned object, as the Replicator may
   * shut down and restart, etc. Generally, the registry should be queried from the registry
   * whenever attempting to communicate with it.
   */
  getReplicator(): Promise<Replicator>;
}
