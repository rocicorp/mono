import {existsSync} from 'node:fs';
import {stat} from 'node:fs/promises';
import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {Database} from '../../../../packages/zqlite/src/db.ts';
import type {SoakConfig} from './config.ts';
import {backupSize} from './infra.ts';
import type {SoakLog} from './logs.ts';

/**
 * Section 7.7, failure class 4: unbounded resource growth when the purge
 * floor is pinned.
 *
 * The floor is `min(backupWatermark, ...acks, ...reservations)`. Two
 * independent things accumulate whenever it is pinned, and neither is visible
 * to the oracle or to the tripwires:
 *
 *  - the change log's local disk, which is excluded from the backup; and
 *  - the upstream replication slot, because with litestream v5 the upstream
 *    ACK is `min(pgChangeLogWatermark, backupWatermark)` -- a stalled backup
 *    stops acking upstream and the slot grows without bound. Chaos action C9
 *    is that test.
 */

export type SlotSample = {
  readonly slotName: string;
  readonly active: boolean;
  readonly retainedBytes: number;
  readonly unconfirmedBytes: number;
};

export type ResourceSample = {
  readonly atMs: number;
  readonly phase: string;
  readonly changeLogBytes: number;
  /** SQLite pages that hold live data, excluding pages on the freelist. */
  readonly changeLogLiveBytes: number | undefined;
  /** Reusable pages retained in the physical SQLite database file. */
  readonly changeLogFreeBytes: number | undefined;
  readonly replicaBytes: Readonly<Record<string, number>>;
  readonly slots: readonly SlotSample[];
  readonly backupObjects: number;
  readonly backupBytes: number;
};

/** Main file plus `-wal`/`-wal2`; `-shm` is a shared-memory index, not bytes. */
export async function sqliteFootprint(file: string): Promise<number> {
  const sizes = await Promise.all(
    [file, `${file}-wal`, `${file}-wal2`].map(async path => {
      try {
        return (await stat(path)).size;
      } catch {
        return 0;
      }
    }),
  );
  return sizes.reduce((a, b) => a + b, 0);
}

export type SQLitePageUsage = {
  readonly liveBytes: number;
  readonly freeBytes: number;
};

/**
 * Physical SQLite files do not normally shrink after DELETE. Pages move to the
 * freelist and are reused by later writes, so live pages are the recovery
 * signal while {@link sqliteFootprint} remains the disk-capacity signal.
 */
export function sqlitePageUsage(
  lc: LogContext,
  file: string,
): SQLitePageUsage | undefined {
  if (!existsSync(file)) {
    return undefined;
  }
  let db: Database | undefined;
  try {
    try {
      db = new Database(lc, file, {readonly: true});
    } catch {
      // Match the oracle: a WAL database can reject a read-only connection
      // when its shared-memory file is not usable. This fallback never writes.
      db = new Database(lc, file);
    }
    const [{page_count: pageCount}] = db.pragma<{page_count: number}>(
      'page_count',
    );
    const [{page_size: pageSize}] = db.pragma<{page_size: number}>('page_size');
    const [{freelist_count: freePages}] = db.pragma<{
      freelist_count: number;
    }>('freelist_count');
    return {
      liveBytes: Math.max(0, pageCount - freePages) * pageSize,
      freeBytes: freePages * pageSize,
    };
  } catch {
    // The change log is deliberately deleted and recreated in C6/C12/C14.
    // A sample racing one of those operations is unavailable, not fatal.
    return undefined;
  } finally {
    db?.close();
  }
}

export async function readSlots(
  sql: postgres.Sql,
  slotPrefix: string,
): Promise<SlotSample[]> {
  const rows = await sql<
    {
      slotName: string;
      active: boolean;
      retainedBytes: string | number | null;
      unconfirmedBytes: string | number | null;
    }[]
  >`
    SELECT slot_name AS "slotName",
           active,
           pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS "retainedBytes",
           pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
             AS "unconfirmedBytes"
      FROM pg_replication_slots
     WHERE slot_type = 'logical'
       AND starts_with(slot_name, ${slotPrefix})`;
  return rows.map(r => ({
    slotName: r.slotName,
    active: r.active,
    retainedBytes: Number(r.retainedBytes ?? 0),
    unconfirmedBytes: Number(r.unconfirmedBytes ?? 0),
  }));
}

export type PurgeStreaks = {
  /** Longest run of consecutive passes that hit the per-pass batch limit. */
  readonly longestBatchLimitStreak: number;
  readonly passes: number;
  readonly batchLimitPasses: number;
  readonly immediateContinuations: number;
  readonly byStopReason: Readonly<Record<string, number>>;
};

/**
 * A run that sits chronically at `continuation: 'immediate'` /
 * `stopped: 'batch-limit'` is a purger losing to the write rate, and the log
 * grows without bound. The streak, not the count, is the signal.
 */
export function purgeStreaks(log: SoakLog, sinceMs = 0): PurgeStreaks {
  let longest = 0;
  let current = 0;
  let passes = 0;
  let batchLimitPasses = 0;
  let immediate = 0;
  const byStopReason: Record<string, number> = {};
  for (const event of log.events) {
    if (event.kind !== 'purge-pass' || event.tsMs < sinceMs) {
      continue;
    }
    passes++;
    const stopped = event.detail.stopped;
    const continuation = event.detail.continuation;
    if (continuation === 'immediate') {
      immediate++;
    }
    const reason = typeof stopped === 'string' ? stopped : 'none';
    byStopReason[reason] = (byStopReason[reason] ?? 0) + 1;
    if (reason === 'batch-limit') {
      batchLimitPasses++;
      current++;
      longest = Math.max(longest, current);
    } else {
      current = 0;
    }
  }
  return {
    longestBatchLimitStreak: longest,
    passes,
    batchLimitPasses,
    immediateContinuations: immediate,
    byStopReason,
  };
}

export class ResourceSampler {
  readonly samples: ResourceSample[] = [];
  readonly #config: SoakConfig;
  readonly #lc: LogContext;
  readonly #sql: postgres.Sql;
  readonly #files: ReadonlyArray<{node: string; replicaFile: string}>;
  readonly #changeLogFile: string;
  #timer: NodeJS.Timeout | undefined;
  #phase = 'startup';
  #sampling = false;

  constructor(
    lc: LogContext,
    config: SoakConfig,
    sql: postgres.Sql,
    changeLogFile: string,
    files: ReadonlyArray<{node: string; replicaFile: string}>,
  ) {
    this.#lc = lc;
    this.#config = config;
    this.#sql = sql;
    this.#changeLogFile = changeLogFile;
    this.#files = files;
  }

  set phase(phase: string) {
    this.#phase = phase;
  }

  start(intervalMs = 5_000): void {
    this.#timer ??= setInterval(() => void this.sample(), intervalMs);
  }

  stop(): void {
    clearInterval(this.#timer);
    this.#timer = undefined;
  }

  async sample(): Promise<ResourceSample | undefined> {
    // S3 listing and a PG round trip can both outrun the interval when minio
    // is down (C9); skipping is better than queueing.
    if (this.#sampling) {
      return undefined;
    }
    this.#sampling = true;
    try {
      const replicaBytes: Record<string, number> = {};
      for (const {node, replicaFile} of this.#files) {
        replicaBytes[node] = await sqliteFootprint(replicaFile);
      }
      const [changeLogBytes, slots, backup] = await Promise.all([
        sqliteFootprint(this.#changeLogFile),
        readSlots(this.#sql, `${this.#config.appID}_0_`).catch(
          () => [] as SlotSample[],
        ),
        backupSize(this.#config).catch(() => ({objects: -1, bytes: -1})),
      ]);
      const pageUsage = sqlitePageUsage(this.#lc, this.#changeLogFile);
      const sample: ResourceSample = {
        atMs: Date.now(),
        phase: this.#phase,
        changeLogBytes,
        changeLogLiveBytes: pageUsage?.liveBytes,
        changeLogFreeBytes: pageUsage?.freeBytes,
        replicaBytes,
        slots,
        backupObjects: backup.objects,
        backupBytes: backup.bytes,
      };
      this.samples.push(sample);
      return sample;
    } finally {
      this.#sampling = false;
    }
  }
}
