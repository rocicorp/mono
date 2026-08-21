import {spawn, type ChildProcess} from 'node:child_process';
import {createInterface} from 'node:readline';
import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import * as v from '../../../../shared/src/valita.ts';
import {Database, type Statement} from '../../../../zqlite/src/db.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {RunningState} from '../running-state.ts';
import type {BackedUpWatermark} from './backup-monitor.ts';

const LOCAL_CHECK_INTERVAL_MS = 1_000;

type LocalWatermark = {
  stateVersion: string;
  writeTimeMs: number;
};

type RemoteWatermark = {
  watermark: string;
  writeTimeMs: number;
  backupTimeMs: number;
};

export type VfsPollerConfig = {
  executable: string;
  remotePollIntervalMs: number;
  remoteQueryTimeoutMs?: number;
  backupURL: string;
  region?: string | undefined;
  endpoint?: string | undefined;
  logLevel?: string;
  logFormat?: string;
};

const queryResultRowSchema = v.object({
  stateVersion: v.string(),
  writeTimeMs: v.number(),
});

const remoteQueryMetadataSchema = v.object({
  litestream_time: v
    .string()
    .map(str => new Date(str).getTime())
    .optional(),
});

// Expected format of each line of stdout from the vfs-query, e.g.
// {"query_result":[{"stateVersion":"51y50ko","writeTimeMs":1787259951121}],"metadata":{"litestream_time":"2026-08-20T21:05:57.36439Z"}}
// {"query_result":[{"stateVersion":"51y7360","writeTimeMs":1787259966137}],"metadata":{"litestream_time":"2026-08-20T21:06:12.384531Z"}}
const remoteQueryLineSchema = v.object({
  query_result: v.tuple([queryResultRowSchema]),
  metadata: remoteQueryMetadataSchema,
});

const INITIAL_BACKOFF_MS = 2_000;
const MAX_BACKOFF_MS = 30_000;

export class VfsWatermarkPoller {
  readonly #lc: LogContext;
  readonly #state = new RunningState('vfs-watermark-poller');
  readonly #pollerConfig: VfsPollerConfig;
  readonly #readLocalWatermark: Statement;

  readonly #stream: Subscription<BackedUpWatermark>;

  #localWatermark: LocalWatermark;
  #remoteWatermark: RemoteWatermark | undefined;
  #localPollTimer: NodeJS.Timeout | undefined;

  #shouldPollRemote = false;

  #remotePoller: ChildProcess | undefined;
  #remotePollerBackoffMs = INITIAL_BACKOFF_MS;
  #remotePollerBackoffTimer: NodeJS.Timeout | undefined;

  constructor(
    lc: LogContext,
    replicaFile: string,
    pollerConfig: VfsPollerConfig,
  ) {
    this.#lc = lc.withContext('component', 'vfs-watermark-poller');
    this.#pollerConfig = pollerConfig;

    const db = new Database(this.#lc, replicaFile, {readonly: true});
    this.#readLocalWatermark = db.prepare(
      `SELECT stateVersion, writeTimeMs FROM "_zero.replicationState"`,
    );
    // Purely to type the member variable as always defined
    this.#localWatermark = v.parse(
      this.#readLocalWatermark.get(),
      queryResultRowSchema,
    );
    this.#stream = Subscription.create<BackedUpWatermark>({
      cleanup: () => {
        this.#disableRemotePoller();
        clearInterval(this.#localPollTimer);
        clearTimeout(this.#remotePollerBackoffTimer);
        this.#state.stop(this.#lc);
        db.close();
      },
    });
  }

  /**
   * Starts polling for watermarks and pushes new ones to returned stream.
   * Must only be called once.
   */
  start(): Source<BackedUpWatermark> {
    assert(this.#localPollTimer === undefined, `Already called start()`);
    this.#lc.info?.(`monitoring backups at ${this.#pollerConfig.backupURL}`);
    this.#localPollTimer = setInterval(
      this.checkLocalWatermark,
      LOCAL_CHECK_INTERVAL_MS,
    );
    this.checkLocalWatermark();
    return this.#stream;
  }

  // Exported for testing
  readonly checkLocalWatermark = () => {
    try {
      this.#localWatermark = v.parse(
        this.#readLocalWatermark.get(),
        queryResultRowSchema,
      );
    } catch (e) {
      this.#lc.warn?.(`unable to read local watermark`, e);
      return;
    }
    if (
      this.#localWatermark.stateVersion !== this.#remoteWatermark?.watermark
    ) {
      this.#enableRemotePoller();
    } else {
      this.#disableRemotePoller();
    }
  };

  #enableRemotePoller() {
    if (this.#remotePoller || this.#remotePollerBackoffTimer) {
      return; // already polling or backing off
    }
    this.#shouldPollRemote = true;
    if (this.#remoteWatermark) {
      this.#lc.debug?.(
        `local and remote watermarks differ. resuming backup polling`,
        {local: this.#localWatermark, remote: this.#remoteWatermark},
      );
    }
    const child = spawn(
      this.#pollerConfig.executable,
      [
        '--replica-url',
        buildLitestreamVfsReplicaURL(this.#pollerConfig),
        '--query',
        `SELECT stateVersion, writeTimeMs FROM "_zero.replicationState"`,
        '--remote-poll-interval',
        `${this.#pollerConfig.remotePollIntervalMs}ms`,
        ...(this.#pollerConfig.logLevel
          ? ['--log-level', this.#pollerConfig.logLevel]
          : []),
        ...(this.#pollerConfig.logFormat
          ? ['--log-format', this.#pollerConfig.logFormat]
          : []),
        ...(this.#pollerConfig.remoteQueryTimeoutMs
          ? ['--query-timeout', `${this.#pollerConfig.remoteQueryTimeoutMs}ms`]
          : []),
      ],
      {stdio: ['pipe', 'pipe', 'inherit']},
    )
      .on('error', err => {
        this.#lc.error?.(`received error from vfs-query process`, err);
      })
      .on('close', (code, signal) => {
        this.#remotePoller = undefined;
        if (this.#shouldPollRemote) {
          if (code !== 0) {
            this.#lc.warn?.(
              `vfs-query exited with ${code}, ${signal}. retrying in ${this.#remotePollerBackoffMs}ms`,
            );
          }
          if (!this.#remotePollerBackoffTimer) {
            this.#remotePollerBackoffTimer = setTimeout(() => {
              this.#remotePollerBackoffTimer = undefined;
              if (this.#shouldPollRemote) {
                this.#enableRemotePoller();
              }
            }, this.#remotePollerBackoffMs);
            this.#remotePollerBackoffMs = Math.min(
              this.#remotePollerBackoffMs * 2,
              MAX_BACKOFF_MS,
            );
          }
        }
      });
    createInterface({input: child.stdout}).on(
      'line',
      this.#readRemotePollerLine,
    );
    this.#remotePoller = child;
  }

  readonly #readRemotePollerLine = (line: string) => {
    try {
      const update = v.parse(
        JSON.parse(line),
        remoteQueryLineSchema,
        'passthrough',
      );
      const [{stateVersion: watermark, writeTimeMs}] = update.query_result;
      const remoteWatermark: RemoteWatermark = {
        watermark,
        writeTimeMs,
        backupTimeMs: update.metadata.litestream_time ?? Date.now(),
      };
      if (remoteWatermark.watermark !== this.#remoteWatermark?.watermark) {
        this.#lc.debug?.(
          `backup watermark advanced from ${this.#remoteWatermark?.watermark} to ${remoteWatermark.watermark}`,
          {local: this.#localWatermark, remote: remoteWatermark},
        );
        this.#stream.push(remoteWatermark);
      }
      this.#remoteWatermark = remoteWatermark;

      // Reset backoff on successful output from the poller
      this.#remotePollerBackoffMs = INITIAL_BACKOFF_MS;
    } catch (e) {
      this.#lc.error?.(`unable to parse vfs-query output: ${line}`, e);
    }
  };

  #disableRemotePoller() {
    if (!this.#shouldPollRemote) {
      return;
    }
    this.#shouldPollRemote = false;
    if (this.#remotePoller || this.#remotePollerBackoffTimer) {
      this.#lc.debug?.(
        `local and remote watermarks match. pausing backup polling`,
        {local: this.#localWatermark, remote: this.#remoteWatermark},
      );
    }
    this.#remotePoller?.kill('SIGTERM');
    this.#remotePoller = undefined;
    clearTimeout(this.#remotePollerBackoffTimer);
    this.#remotePollerBackoffTimer = undefined;
  }
}

export function buildLitestreamVfsReplicaURL({
  backupURL,
  endpoint,
  region,
}: {
  readonly backupURL: string;
  readonly endpoint?: string | undefined;
  readonly region?: string | undefined;
}): string {
  if (endpoint === undefined && region === undefined) {
    return backupURL;
  }

  const url = new URL(backupURL);
  if (url.protocol !== 's3:') {
    return backupURL;
  }

  if (endpoint !== undefined && !url.searchParams.has('endpoint')) {
    url.searchParams.set('endpoint', endpoint);
  }
  if (region !== undefined && !url.searchParams.has('region')) {
    url.searchParams.set('region', region);
  }
  return url.toString();
}
