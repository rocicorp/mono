import type {LogContext} from '@rocicorp/logger';

/**
 * Releases the resources acquired by an initialization attempt (e.g. a
 * claimed replication slot, or a purge lock on the change-log) if the attempt
 * fails, e.g. before it is retried after an AutoResetSignal, or when a restore
 * is abandoned in favor of an initial sync.
 *
 * On success, the resources are handed over (e.g. the replication slot is
 * taken over by the change source, and the purge lock by the change-streamer)
 * and the cleanup is simply dropped.
 */
export class InitCleanup {
  readonly #lc: LogContext;
  #releasers: {name: string; release: () => unknown}[] = [];

  constructor(lc: LogContext) {
    this.#lc = lc;
  }

  /** Registers a resource to be released if the attempt fails. */
  onFailure(name: string, release: () => unknown) {
    this.#releasers.push({name, release});
  }

  /**
   * Releases the registered resources in the reverse order of their
   * registration. Errors are logged rather than thrown so as not to mask the
   * failure of the attempt.
   */
  async release() {
    const releasers = this.#releasers.reverse();
    this.#releasers = [];
    for (const {name, release} of releasers) {
      try {
        await release();
        this.#lc.info?.(`released ${name}`);
      } catch (e) {
        this.#lc.warn?.(`error releasing ${name}`, e);
      }
    }
  }
}
