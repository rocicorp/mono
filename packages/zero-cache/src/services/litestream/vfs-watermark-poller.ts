import type {LogContext, LogLevel} from '@rocicorp/logger';
import Sqlite3Database from '@rocicorp/zero-sqlite3';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import * as v from '../../../../shared/src/valita.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {type Worker} from '../../types/processes.ts';
import {RunningState} from '../running-state.ts';
import type {Service} from '../service.ts';

const DEFAULT_VFS_LOG_LEVEL = 'info';

type LoadableDatabase = {
  loadExtension(path: string): unknown;
  close(): void;
};

type QueryDatabase = {
  prepare(sql: string): {get<T>(): T};
  close(): void;
};

const watermarkRowSchema = v.object({
  watermark: v.string(),
  writeTimeMs: v.number(),
});

const watermarkQuerySchema = watermarkRowSchema.extend({
  backupTime: v.string(),
});

type WatermarkQueryResult = v.Infer<typeof watermarkQuerySchema>;

type WatermarkRow = v.Infer<typeof watermarkRowSchema>;

export type BackupWatermarkUpdate = [
  'backupWatermarkUpdate',
  {
    watermark: string;
    writeTimeMs: number;
    backupTimeMs: number;
  },
];

type WatermarkReader<Result extends WatermarkRow> = {
  getWatermark(): Result;
  close(): void;
};

// Values that must be passed via the ENV to the original process (worker).
// Some of these will be configurable via the db URI once
// https://github.com/benbjohnson/litestream/issues/1150 is released.
export type VfsBackupWatermarkPollerEnvOptions = {
  readonly backupURL: string;
  readonly remotePollIntervalMs: number;
  readonly endpoint?: string | undefined;
  readonly region?: string | undefined;

  readonly logLevel?: LogLevel | undefined;
  readonly logFile?: string | undefined;
};

export type VfsBackupWatermarkPollerOptions = {
  readonly localDbFile: string;
  readonly backupURL: string;
  readonly endpoint?: string | undefined;
  readonly region?: string | undefined;

  readonly extensionPath: string;
  readonly remotePollIntervalMs: number;
};

type VfsBackupWatermarkPollerDeps = {
  readonly loaderFactory?: (() => LoadableDatabase) | undefined;
  readonly databaseFactory?:
    | ((lc: LogContext, uri: string) => QueryDatabase)
    | undefined;
};

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

export function getVfsEnv(
  options: VfsBackupWatermarkPollerEnvOptions,
): NodeJS.ProcessEnv {
  return {
    LITESTREAM_REPLICA_URL: buildLitestreamVfsReplicaURL(options),
    LITESTREAM_LOG_LEVEL: options.logLevel ?? DEFAULT_VFS_LOG_LEVEL,
    LITESTREAM_LOG_FILE: options.logFile,

    // Supported by the rocicorp fork: https://github.com/rocicorp/litestream/pull/20
    // TODO: Remove and use official build when
    // https://github.com/benbjohnson/litestream/pull/1157 is released.
    LITESTREAM_POLL_INTERVAL: `${options.remotePollIntervalMs}ms`,
  };
}

const MIN_CHECK_INTERVAL_MS = 1_000;
const MAX_CHECK_INTERVAL_MS = 60_000;

export class VfsBackupWatermarkPoller implements Service {
  readonly id = 'backup-watermark-poller';
  readonly #runningState = new RunningState(this.id);
  readonly #lc: LogContext;
  readonly #parent: Worker;
  readonly #loader: LoadableDatabase;

  readonly #localDb: WatermarkReader<WatermarkRow>;
  readonly #openRemote: () => WatermarkReader<WatermarkQueryResult>;

  #remoteDb: WatermarkReader<WatermarkQueryResult> | null = null;
  #lastRemoteWatermark: WatermarkQueryResult | null = null;

  constructor(
    lc: LogContext,
    parent: Worker,
    options: VfsBackupWatermarkPollerOptions,
    deps: VfsBackupWatermarkPollerDeps = {},
  ) {
    lc = lc.withContext('component', 'vfs-watermark-poller');
    this.#lc = lc;
    this.#parent = parent;

    const {databaseFactory} = deps;
    const openDb = <Result extends WatermarkRow>(
      url: string,
      type: 'local' | 'remote',
      schema: v.Type<Result>,
    ) => {
      try {
        const db = databaseFactory
          ? databaseFactory(lc, url)
          : new Database(lc, url, {readonly: true});
        const stmt = db.prepare(
          type === 'local'
            ? /*sql*/ `
          SELECT stateVersion AS watermark, writeTimeMs FROM "_zero.replicationState"`
            : /*sql*/ `
          SELECT stateVersion AS watermark, writeTimeMs, litestream_time() as backupTime FROM "_zero.replicationState"`,
        );
        return {
          getWatermark: () => v.parse(stmt.get(), schema),
          close: () => db.close(),
        } satisfies WatermarkReader<Result>;
      } catch (e) {
        lc.error?.(`error opening ${url}`, e);
        throw e;
      }
    };

    // Open the local (non-VFS) connection before loading the litestream VFS
    // extension. Loading the extension registers a process-global
    // `sqlite3_auto_extension` hook that SQLite invokes on every connection
    // opened *afterward* in this process (even ones that don't reference
    // `vfs=litestream`), and a signature bug in that hook currently causes
    // it to fail such connections. Opening #localDb first sidesteps it,
    // since already-open connections aren't affected by the hook.
    this.#localDb = openDb(options.localDbFile, 'local', watermarkRowSchema);

    const loaderFactory =
      deps.loaderFactory ?? (() => new Sqlite3Database(':memory:'));

    const dbParams = new URLSearchParams({
      vfs: 'litestream',
      mode: 'ro',
      // Support forthcoming in https://github.com/benbjohnson/litestream/issues/1150
      replica_url: buildLitestreamVfsReplicaURL(options),
      poll_interval: `${options.remotePollIntervalMs}ms`,
    });
    // Note: the filename is arbitrary and only referenced in debug logs.
    const remoteDbURL = `file:zero-backup.db?${dbParams.toString()}`;

    lc.info?.(`Loading VFS ${remoteDbURL} at ${options.extensionPath}`);
    this.#loader = loaderFactory();
    this.#loader.loadExtension(options.extensionPath);

    this.#openRemote = () =>
      openDb(remoteDbURL, 'remote', watermarkQuerySchema);
  }

  // public for testing
  checkWatermarks() {
    const local = this.#localDb.getWatermark();
    if (local.watermark !== this.#lastRemoteWatermark?.watermark) {
      if (!this.#remoteDb) {
        if (this.#lastRemoteWatermark) {
          this.#lc.debug?.(
            `local and remote watermarks differ. resuming backup polling`,
            {local, remote: this.#lastRemoteWatermark},
          );
        }
        this.#remoteDb = this.#openRemote();
      }
      const remote = this.#remoteDb.getWatermark();
      if (remote.watermark !== this.#lastRemoteWatermark?.watermark) {
        this.#lc.debug?.(
          `backup watermark advanced from ${this.#lastRemoteWatermark?.watermark} to ${remote.watermark}`,
          {local, remote},
        );
        this.#lastRemoteWatermark = remote;
        const {watermark, writeTimeMs, backupTime} = remote;
        // Note: litestream_time() returns "latest" if no LTX files were
        // loaded:
        //
        // https://github.com/benbjohnson/litestream/blob/6d61ef5d007756d62e473daee4c760ac395a55c6/vfs.go#L2448
        //
        // but in that case the watermark query would have failed, so it
        // follows that litestream_time() should always return a valid
        // timestamp if the watermark query itself succeeded.
        const backupTimeMs = new Date(backupTime).getTime();
        this.#parent.send<BackupWatermarkUpdate>([
          'backupWatermarkUpdate',
          {watermark, writeTimeMs, backupTimeMs},
        ]);
      }
    }
    // Note: This is not an `else`, as we specifically want to re-check
    // this.#lastRemoteWatermark
    if (local.watermark === this.#lastRemoteWatermark?.watermark) {
      if (this.#remoteDb) {
        this.#lc.debug?.(
          `local and remote watermarks match. pausing backup polling`,
          {local, remote: this.#lastRemoteWatermark},
        );
        this.#remoteDb.close();
        this.#remoteDb = null;
      }
    }
  }

  async run() {
    this.#lc.info?.(`starting backup polling`);
    let interval = MIN_CHECK_INTERVAL_MS;
    while (this.#runningState.shouldRun()) {
      try {
        this.checkWatermarks();
        interval = MIN_CHECK_INTERVAL_MS;
      } catch (e) {
        this.#lc.warn?.(`error checking local and remote watermarks`, e);
        interval = Math.min(MAX_CHECK_INTERVAL_MS, interval * 2);
      }
      await this.#runningState.sleep(interval);
    }
    this.#remoteDb?.close();
    this.#localDb.close();
    this.#loader.close();
  }

  stop(): Promise<void> {
    this.#runningState.stop(this.#lc);
    return promiseVoid;
  }
}
