/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import type {Replicator} from './replicator.ts';

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
