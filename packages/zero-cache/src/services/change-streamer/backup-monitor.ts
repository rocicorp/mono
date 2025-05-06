import type {LogContext} from '@rocicorp/logger';
import parsePrometheusTextFormat from 'parse-prometheus-text-format';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import {RunningState} from '../running-state.ts';
import type {Service} from '../service.ts';
import type {ChangeStreamerService} from './change-streamer.ts';

export const CHECK_INTERVAL_MS = 60 * 1000;
const DEFAULT_CLEANUP_DELAY_MS = 60 * 1000;

// The BackupMonitor polls the litestream "/metrics" endpoint to track the
// watermark (label) value of the `litestream_replica_progress` gauge and
// schedule cleanup of change log entries that can be purged as a result.
//
// See: https://github.com/rocicorp/litestream/pull/3
//
// Note that change log purging involves two time values:
// 1. The time at which a watermark has been backed up by litestream
// 2. The time it takes to restore the replica from the backup
//
// Namely, it is not safe to simply purge change log entries as soon as they
// have been applied and backed up by litestream. Consider the case in which
// litestream backs up new wal segments every minute, but it takes 5 minutes
// to restore a replica: if a zero-cache starts restoring a replica at
// minute 0, and new watermarks are replicated at minutes 1, 2, 3, 4, and 5,
// purging changelog records as soon as those watermarks are replicated would
// result in the zero-cache not being able to catch up from minute 0 once it
// has finished restoring the replica.
//
// Consequently, cleanup is delayed by an interval equivalent to the time it
// takes for the litestream restore, which is calculated by the runner (when
// it performed the restore or the initial sync). In addition to this delay,
// the ChangeStreamerService itself adds at least 30 seconds of padding before
// purging records, ensuring all active subscribers are past the watermark
// before purging preceding records.
export class BackupMonitor implements Service {
  readonly id = 'backup-monitor';
  readonly #lc: LogContext;
  readonly #metricsEndpoint: string;
  readonly #changeStreamer: ChangeStreamerService;
  readonly #state = new RunningState(this.id);
  readonly #cleanupDelayMs: number;
  readonly #cleanupTimers = new Map<string, NodeJS.Timeout>();
  #checkMetricsTimer: NodeJS.Timeout | undefined;

  constructor(
    lc: LogContext,
    metricsEndpoint: string,
    changeStreamer: ChangeStreamerService,
    cleanupDelayMs: number | undefined,
  ) {
    this.#lc = lc.withContext('component', this.id);
    this.#metricsEndpoint = metricsEndpoint;
    this.#changeStreamer = changeStreamer;
    this.#cleanupDelayMs = cleanupDelayMs ?? DEFAULT_CLEANUP_DELAY_MS;
  }

  run(): Promise<void> {
    this.#lc.info?.(
      `monitoring backups at ${this.#metricsEndpoint} with ` +
        `${this.#cleanupDelayMs} ms cleanup delay`,
    );
    this.#checkMetricsTimer = setInterval(this.checkMetrics, CHECK_INTERVAL_MS);
    return this.#state.stopped();
  }

  // Exported for testing
  readonly checkMetrics = async () => {
    try {
      const resp = await fetch(this.#metricsEndpoint);
      if (!resp.ok) {
        this.#lc.warn?.(
          `unable to fetch metrics at ${this.#metricsEndpoint}`,
          await resp.text(),
        );
        return;
      }
      const families = parsePrometheusTextFormat(await resp.text());
      for (const family of families) {
        if (
          family.type === 'GAUGE' &&
          family.name === 'litestream_replica_progress'
        ) {
          for (const metric of family.metrics) {
            const watermark = metric.labels?.watermark;
            if (watermark && !this.#cleanupTimers.has(watermark)) {
              const time = new Date(parseFloat(metric.value) * 1000);
              const now = Date.now();
              const cleanupAt = Math.max(
                time.getTime() + this.#cleanupDelayMs,
                now,
              );
              this.#lc.info?.(
                `replicated watermark=${watermark} to ${metric.labels?.name}` +
                  ` at ${time.toISOString()}. scheduling cleanup at ` +
                  new Date(cleanupAt).toISOString() +
                  ` (in ${cleanupAt - now} ms)`,
              );
              this.#cleanupTimers.set(
                watermark,
                setTimeout(() => {
                  this.#changeStreamer.scheduleCleanup(watermark);
                  // Clean up all previous watermarks, leaving the latest one in the
                  // map to avoid redundant scheduling.
                  for (const [key, timer] of this.#cleanupTimers.entries()) {
                    if (key === watermark) {
                      break;
                    }
                    this.#cleanupTimers.delete(key);
                    clearTimeout(timer);
                  }
                }, cleanupAt - now),
              );
              this.#lc.debug?.('timers', [...this.#cleanupTimers.keys()]);
            }
          }
        }
      }
    } catch (e) {
      this.#lc.warn?.(`unable to fetch metrics at ${this.#metricsEndpoint}`, e);
    }
  };

  stop(): Promise<void> {
    clearInterval(this.#checkMetricsTimer);
    for (const cleanupTimer of this.#cleanupTimers.values()) {
      clearTimeout(cleanupTimer);
    }
    this.#state.stop(this.#lc);
    return promiseVoid;
  }
}
