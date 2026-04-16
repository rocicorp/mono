import type {LogContext} from '@rocicorp/logger';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import type {ShardConfig} from '../../types/shards.ts';
import type {ServerContext} from '../change-source/pg/initial-sync.ts';
import {shadowInitialSync} from '../change-source/pg/initial-sync.ts';
import {RunningState} from '../running-state.ts';
import type {Service} from '../service.ts';

export type ShadowSyncOptions = {
  intervalMs: number;
  sampleRate: number;
  maxRowsPerTable: number;
  textCopy?: boolean | undefined;
};

export class ShadowSyncService implements Service {
  readonly id = 'shadow-syncer';

  readonly #lc: LogContext;
  readonly #shard: ShardConfig;
  readonly #upstreamURI: string;
  readonly #context: ServerContext;
  readonly #options: ShadowSyncOptions;
  readonly #state = new RunningState('shadow-syncer');

  constructor(
    lc: LogContext,
    shard: ShardConfig,
    upstreamURI: string,
    context: ServerContext,
    options: ShadowSyncOptions,
  ) {
    this.#lc = lc;
    this.#shard = shard;
    this.#upstreamURI = upstreamURI;
    this.#context = context;
    this.#options = options;
  }

  async run() {
    const {intervalMs, sampleRate, maxRowsPerTable, textCopy} = this.#options;

    // Why: wait at least one full interval before the first run so shadow
    // sync never fires immediately on task startup, and add a random
    // fraction of the interval on top so a fleet-wide restart does not
    // cause every task to canary simultaneously.
    const firstRunDelay = intervalMs + Math.floor(Math.random() * intervalMs);
    this.#lc.info?.(
      `shadow-syncer started; first run in ${firstRunDelay} ms, then every ${intervalMs} ms`,
    );
    await this.#state.sleep(firstRunDelay);

    while (this.#state.shouldRun()) {
      const start = performance.now();
      try {
        await shadowInitialSync(
          this.#lc,
          this.#shard,
          this.#upstreamURI,
          {sampleRate, maxRowsPerTable},
          this.#context,
          textCopy !== undefined ? {textCopy} : undefined,
        );
        const elapsed = performance.now() - start;
        this.#lc.info?.(
          `shadow initial-sync completed (${elapsed.toFixed(0)} ms)`,
        );
      } catch (e) {
        const elapsed = performance.now() - start;
        this.#lc.error?.(
          `shadow initial-sync failed after ${elapsed.toFixed(0)} ms`,
          e,
        );
      }
      await this.#state.sleep(intervalMs);
    }
  }

  stop(): Promise<void> {
    this.#state.stop(this.#lc);
    return promiseVoid;
  }
}
