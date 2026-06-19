import type {LogContext, LogLevel} from '@rocicorp/logger';
import Sqlite3Database from '@rocicorp/zero-sqlite3';
import * as v from '../../../../shared/src/valita.ts';
import {Database} from '../../../../zqlite/src/db.ts';

const DEFAULT_VFS_DATABASE_URI = 'file:zero-backup.db?vfs=litestream&mode=ro';
const DEFAULT_VFS_LOG_LEVEL = 'info';

type LoadableDatabase = {
  loadExtension(path: string): unknown;
  close(): void;
};

type QueryDatabase = {
  prepare(sql: string): {
    get<T>(): T;
  };
  close(): void;
};

export type VfsBackupWatermark = {
  watermark: string;
  writeTimeMs: number | null;
  txid: string;
  lagSeconds: number;
  observedAtMs: number;
};

export type VfsBackupWatermarkReaderOptions = {
  readonly replicaURL: string;
  readonly extensionPath: string;
  readonly logLevel?: LogLevel | undefined;
  readonly logFile?: string | undefined;
  readonly databaseURI?: string | undefined;
  readonly env?: NodeJS.ProcessEnv | undefined;
};

type VfsBackupWatermarkReaderDeps = {
  readonly loaderFactory?: (() => LoadableDatabase) | undefined;
  readonly databaseFactory?:
    | ((lc: LogContext, uri: string) => QueryDatabase)
    | undefined;
};

const backupWatermarkRowSchema = v.object({
  watermark: v.string(),
  writeTimeMs: v.number().nullable(),
  txid: v.string(),
  lagSeconds: v.number(),
});

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

export class VfsBackupWatermarkReader implements Disposable {
  readonly #loader: LoadableDatabase;
  readonly #db: QueryDatabase;
  readonly #stmt: {
    get<T>(): T;
  };

  constructor(
    lc: LogContext,
    options: VfsBackupWatermarkReaderOptions,
    deps: VfsBackupWatermarkReaderDeps = {},
  ) {
    const env = options.env ?? process.env;
    env['LITESTREAM_REPLICA_URL'] = options.replicaURL;
    env['LITESTREAM_LOG_LEVEL'] = (
      options.logLevel ?? DEFAULT_VFS_LOG_LEVEL
    ).toUpperCase();
    setOptionalEnv(env, 'LITESTREAM_LOG_FILE', options.logFile);

    const loaderFactory =
      deps.loaderFactory ?? (() => new Sqlite3Database(':memory:'));
    const databaseFactory =
      deps.databaseFactory ??
      ((lc: LogContext, uri: string) =>
        new Database(lc, uri, {readonly: true}));

    this.#loader = loaderFactory();
    this.#loader.loadExtension(options.extensionPath);

    this.#db = databaseFactory(
      lc.withContext('component', 'vfs-backup-watermark-reader'),
      options.databaseURI ?? DEFAULT_VFS_DATABASE_URI,
    );
    this.#stmt = this.#db.prepare(/*sql*/ `
      SELECT
        stateVersion AS watermark,
        writeTimeMs,
        litestream_txid() AS txid,
        litestream_lag() AS lagSeconds
      FROM "_zero.replicationState"
    `);
  }

  readWatermark(): VfsBackupWatermark {
    const row = v.parse(this.#stmt.get(), backupWatermarkRowSchema);
    return {
      ...row,
      observedAtMs: Date.now(),
    };
  }

  close(): void {
    this.#db.close();
    this.#loader.close();
  }

  [Symbol.dispose](): void {
    this.close();
  }
}

function setOptionalEnv(
  env: NodeJS.ProcessEnv,
  name: string,
  value: string | undefined,
) {
  if (value === undefined) {
    delete env[name];
  } else {
    env[name] = value;
  }
}
