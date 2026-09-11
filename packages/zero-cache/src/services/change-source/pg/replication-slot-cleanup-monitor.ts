import type {LogContext} from '@rocicorp/logger';
import {PG_17} from '../../../types/pg-versions.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {ShardID} from '../../../types/shards.ts';
import {dropInactiveSlotsAndReplicas} from './replication-slots.ts';
import {replicationSlotPrefix} from './schema/shard.ts';

type Options = {
  inactiveSlotCleanupTimeoutMs?: number | undefined;
  pollIntervalMs?: number | undefined;
};

const DEFAULT_INACTIVE_SLOT_CLEANUP_TIMEOUT_MS = 5 * 60 * 1000;
const DEFAULT_POLL_INTERVAL_MS = 60 * 1000;

export class ReplicationSlotCleanupMonitor {
  readonly #lc: LogContext;

  /** The shard constrains which slots are monitored (e.g. zero_0_*). */
  readonly #shard: ShardID;
  readonly #upstream: PostgresDB;
  /**
   * This slot that this task is using. Cleanup of slots is predicated on
   * this slot remaining active, in order to guarantee at least one healthy
   * slot in the shard.
   */
  readonly #currentSlot: string;

  /**
   * How long a slot is inactive (while the currentSlot is active) before it
   * is eligible to be cleaned up. Because new slots are immediately connected
   * to, even while an initial-sync or replica-restore is in progress, slots
   * should only be inactive if no server is using it, or if a server was
   * disconnected (e.g. a pg restart) and is in the process of reconnecting
   * to it (see `keepSlotActiveUntilTakenOver`).
   *
   * As such, this timeout should be long enough to account for a server
   * reconnecting to upstream. Note again that the cleanup monitor only runs
   * while at least one replication slot is active, guaranteeing that upstream
   * replication is working.
   */
  readonly #inactiveSlotCleanupTimeoutMs: number;
  readonly #pollIntervalMs: number;

  /** Manually tracked inactive times for PG < 17. null for PG 17+ */
  readonly #firstInactiveTimes: Map<string, Date> | null;

  #pollTimer: NodeJS.Timeout | undefined;

  constructor(
    lc: LogContext,
    shard: ShardID,
    upstream: PostgresDB,
    upstreamPgServerVersionNum: number,
    currentSlot: string,
    {inactiveSlotCleanupTimeoutMs, pollIntervalMs}: Options = {},
  ) {
    this.#lc = lc.withContext('component', 'replication-slot-cleanup-monitor');
    this.#shard = shard;
    this.#upstream = upstream;
    this.#currentSlot = currentSlot;
    this.#inactiveSlotCleanupTimeoutMs =
      inactiveSlotCleanupTimeoutMs ?? DEFAULT_INACTIVE_SLOT_CLEANUP_TIMEOUT_MS;
    this.#pollIntervalMs = pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.#firstInactiveTimes =
      upstreamPgServerVersionNum >= PG_17 ? null : new Map();
  }

  start() {
    if (this.#pollTimer) {
      this.#lc.warn?.(`monitor already started`);
      return;
    }
    this.#pollTimer = setInterval(
      () =>
        this.#poll(new Date()).catch(e =>
          this.#lc.warn?.(`error checking replication slots`, e),
        ),
      this.#pollIntervalMs,
    );
  }

  stop() {
    clearInterval(this.#pollTimer);
    this.#pollTimer = undefined;
    this.#firstInactiveTimes?.clear();
  }

  async #poll(now: Date) {
    if (!this.#pollTimer) {
      return; // concurrently stopped
    }
    const slots = await this.getSlotsToCleanup(now);
    if (slots.length) {
      await dropInactiveSlotsAndReplicas(
        this.#lc,
        this.#upstream,
        this.#shard,
        slots,
      );
    }
  }

  async getSlotsToCleanup(now: Date): Promise<string[]> {
    const slotPoolPrefix = replicationSlotPrefix(this.#shard);

    const slotsToCleanUp: string[] = [];
    const trackedInactiveTimes = this.#firstInactiveTimes;

    if (trackedInactiveTimes) {
      // For PG <17, each poll tracks and updates earliest time the slot was
      // observed to be inactive.
      const shardSlots = await this.#upstream<{slot: string; active: boolean}[]>
      /*sql*/ `
        SELECT slot_name AS slot, active FROM pg_replication_slots
          WHERE slot_name LIKE ${slotPoolPrefix + '%'};
      `;
      for (const {slot, active} of shardSlots) {
        if (slot === this.#currentSlot && !active) {
          this.#lc.warn?.(
            `current slot ${this.#currentSlot} is not active. disabling cleanup.`,
          );
          this.stop();
          return [];
        }
        if (active) {
          trackedInactiveTimes.delete(slot);
        } else {
          const inactiveSince = trackedInactiveTimes.get(slot);
          if (!inactiveSince) {
            trackedInactiveTimes.set(slot, now);
          } else if (
            now.getTime() - inactiveSince.getTime() >=
            this.#inactiveSlotCleanupTimeoutMs
          ) {
            this.#lc.info?.(
              `slot ${slot} has been inactive since ${inactiveSince.toISOString()} and is eligible for cleanup`,
            );
            slotsToCleanUp.push(slot);
          }
        }
      }
    } else {
      // PG 17+ is simpler as inactivity time is tracked in the table.
      const expiration = now.getTime() - this.#inactiveSlotCleanupTimeoutMs;
      const inactiveSlots = await this.#upstream<
        {slot: string; inactiveSince: number}[]
      > /*sql*/ `
        SELECT slot_name AS slot, inactive_since AS "inactiveSince" FROM pg_replication_slots
          WHERE slot_name LIKE ${slotPoolPrefix + '%'}
            AND active = false
            AND inactive_since IS NOT NULL;
      `;
      for (const {slot, inactiveSince} of inactiveSlots) {
        if (slot === this.#currentSlot) {
          this.#lc.warn?.(
            `current slot ${this.#currentSlot} is not active. disabling cleanup.`,
          );
          this.stop();
          return [];
        }
        // The slot has been inactive since at or before the expiration cutoff
        // (now - timeout), i.e. inactive for at least the timeout duration.
        if (inactiveSince <= expiration) {
          this.#lc.info?.(
            `slot ${slot} has been inactive since ${new Date(inactiveSince).toISOString()} and is eligible for cleanup`,
          );
          slotsToCleanUp.push(slot);
        }
      }
    }
    return slotsToCleanUp;
  }
}
