import {getOrCreateGauge} from '../../observability/metrics.ts';
import type {InitializeKind} from '../change-source/common/replica-restore.ts';

/**
 * The phases of a replication-manager's start:
 * - `slot_create`: creating the new replica's slot, including retries while
 *   it is blocked by upstream transactions. This is separate from `fork_wait`
 *   because the upstream (e.g. a long-running transaction), not zero-cache or
 *   its capacity, determines how long it takes.
 * - `fork_wait`: selecting the replica to restore from (and claiming its
 *   slot), including the wait for a forked replica to pass the new slot's
 *   position, excluding `slot_create`
 * - `restore`: restoring the replica from its backup
 * - `catch_up`: from initialization to the backup-replicator being caught up
 *   (i.e. its replication lag readiness)
 * - `backup_confirm`: waiting for the first backup to cover the replica
 * - `total`: from the start of the (change-streamer) process to readiness
 *
 * The phases are not exhaustive (e.g. connecting to and preparing the
 * upstream), and `catch_up` and `backup_confirm` overlap.
 */
export type ServerStartPhase =
  | 'slot_create'
  | 'fork_wait'
  | 'restore'
  | 'catch_up'
  | 'backup_confirm'
  | 'total';

/**
 * Records where a replication-manager's start goes, reported once it is
 * ready by the `zero.replication.server_start` gauge, in seconds, labeled
 * by `phase` and `kind` (see {@link InitializeKind}). This is for diagnosis,
 * e.g. of why a start is slow, as the process only starts once.
 */
export class ServerStartMetrics {
  readonly #phasesMs = new Map<ServerStartPhase, number>();
  #kind: InitializeKind | undefined;
  #ready = false;

  setKind(kind: InitializeKind | undefined) {
    this.#kind = kind;
  }

  record(phase: ServerStartPhase, ms: number | undefined) {
    if (ms !== undefined && !this.#ready) {
      this.#phasesMs.set(phase, ms);
    }
  }

  /**
   * Records the `total` (by default, the time since the process started) and
   * starts reporting the gauge. Phases recorded afterwards are ignored.
   */
  ready(totalMs = performance.now()) {
    if (this.#ready) {
      return;
    }
    this.record('total', totalMs);
    this.#ready = true;
    const kind = this.#kind ?? 'unknown';
    getOrCreateGauge('replication', 'server_start', {
      description:
        'The duration of each phase of the replication-manager start ' +
        '(slot_create, fork_wait, restore, catch_up, backup_confirm, and the total ' +
        'from process start to readiness), by the kind of initialization ' +
        '(fork, resume, rmv1, initial_sync).',
      unit: 's',
    }).addCallback(o => {
      for (const [phase, ms] of this.#phasesMs) {
        o.observe(ms / 1000, {phase, kind});
      }
    });
  }
}
