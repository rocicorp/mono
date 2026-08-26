import {mkdirSync} from 'node:fs';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';

export const APP_ROOT = fileURLToPath(new URL('../..', import.meta.url));
export const REPO_ROOT = fileURLToPath(new URL('../../../..', import.meta.url));

export const ZERO_CACHE_MAIN = join(
  REPO_ROOT,
  'packages/zero-cache/src/server/runner/main.ts',
);
export const LITESTREAM_CONFIG_V3 = join(
  REPO_ROOT,
  'packages/zero-cache/src/services/litestream/config.yml',
);
export const LITESTREAM_CONFIG_V5 = join(
  REPO_ROOT,
  'packages/zero-cache/src/services/litestream/config-v5.yml',
);
export const BIN_DIR = join(APP_ROOT, '.litestream/bin');
export const DOCKER_DIR = join(APP_ROOT, 'docker');

/**
 * The change-log mode ladder. `serve` is the subject of the soak; the others
 * are the rollback rungs that chaos actions C10-C12 walk back down.
 */
export type ChangeLogMode = 'off' | 'write' | 'compare' | 'serve';

export type ChangeLogSettings = {
  readonly mode: ChangeLogMode;
  readonly readPercent: number;
  readonly coldReadPercent: number;
  readonly comparePercent: number;
  readonly retentionMs: number;
};

export type SoakConfig = {
  readonly runID: string;
  readonly root: string;
  readonly logsDir: string;
  readonly reportPath: string;

  readonly upstreamDB: string;
  readonly viewSyncers: number;

  readonly rmPort: number;
  readonly vsBasePort: number;
  readonly otlpPort: number;

  readonly backupURL: string;
  readonly s3Endpoint: string;
  readonly s3Region: string;
  readonly accessKeyID: string;
  readonly secretAccessKey: string;

  readonly changeLog: ChangeLogSettings;
  readonly backupIntervalSeconds: number;
  readonly vfsPollIntervalMs: number;
  readonly startupDelayMs: number;
  readonly snapshotBackupIntervalHours: number;

  readonly rmLogLevel: 'debug' | 'info';
  readonly vsLogLevel: 'debug' | 'info';

  /** Chaos actions to run, e.g. ['C1','C3','C6']. */
  readonly chaos: readonly string[];
  /** Run the mode=off A/B control pass before the real run (phase 0). */
  readonly baseline: boolean;
  /** Scales every phase duration; 0.25 makes a smoke run out of a soak. */
  readonly scale: number;
  /** Run `db-migrate` and `db-seed` before the run. */
  readonly seed: boolean;
  /** Leave processes, containers and directories in place on exit. */
  readonly keep: boolean;
  readonly metricExportIntervalMs: number;
};

export function taskIDFor(index: number | 'rm'): string {
  return index === 'rm' ? 'rm' : `vs-${index}`;
}

export function nodeDir(config: SoakConfig, node: string): string {
  const dir = join(config.root, node);
  mkdirSync(dir, {recursive: true});
  return dir;
}

export function replicaFile(config: SoakConfig, node: string): string {
  return join(nodeDir(config, node), 'replica.db');
}

/** The change log lives beside the replica; see `change-log-db.ts`. */
export function changeLogFile(config: SoakConfig, node: string): string {
  return `${replicaFile(config, node)}-change-log`;
}

export function vsPort(config: SoakConfig, index: number): number {
  return config.vsBasePort + index * 10;
}

export function otlpEndpoint(config: SoakConfig, node: string): string {
  return `http://127.0.0.1:${config.otlpPort}/v1/metrics/${node}`;
}

function commonEnv(config: SoakConfig, node: string): NodeJS.ProcessEnv {
  return {
    ...process.env,
    NODE_ENV: 'development',
    DO_NOT_TRACK: '1',
    ZERO_ENABLE_TELEMETRY: 'false',
    ZERO_UPSTREAM_DB: config.upstreamDB,
    ZERO_CVR_DB: config.upstreamDB,
    ZERO_CHANGE_DB: config.upstreamDB,
    ZERO_TASK_ID: node,
    ZERO_LOG_FORMAT: 'json',
    ZERO_SHADOW_SYNC_ENABLED: 'false',
    // Litestream reads these from the child's environment; both the RM (which
    // backs up) and the view-syncers (which restore) need them.
    ZERO_LITESTREAM_EXECUTABLE: join(BIN_DIR, 'litestream-v3'),
    ZERO_LITESTREAM_EXECUTABLE_V5: join(BIN_DIR, 'litestream-v5'),
    ZERO_LITESTREAM_VFS_QUERY_EXECUTABLE: join(BIN_DIR, 'vfs-query'),
    // The defaults are relative to the process's cwd
    // (`./src/services/litestream/...`), which only resolves when zero-cache
    // runs from packages/zero-cache.
    ZERO_LITESTREAM_CONFIG_PATH: LITESTREAM_CONFIG_V3,
    ZERO_LITESTREAM_CONFIG_PATH_V5: LITESTREAM_CONFIG_V5,
    ZERO_LITESTREAM_RESTORE_USING_V5: 'true',
    ZERO_LITESTREAM_BACKUP_USING_V5: 'true',
    ZERO_LITESTREAM_ENDPOINT: config.s3Endpoint,
    ZERO_LITESTREAM_REGION: config.s3Region,
    AWS_ACCESS_KEY_ID: config.accessKeyID,
    AWS_SECRET_ACCESS_KEY: config.secretAccessKey,
    AWS_REGION: config.s3Region,
    // OTel metrics are the census and the resource-bound sampler: every
    // worker in every task exports to the orchestrator's in-process OTLP
    // receiver on a path that names the task. Logs and traces stay off.
    OTEL_METRICS_EXPORTER: 'otlp',
    OTEL_LOGS_EXPORTER: 'none',
    OTEL_TRACES_EXPORTER: 'none',
    OTEL_EXPORTER_OTLP_PROTOCOL: 'http/json',
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: otlpEndpoint(config, node),
    OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE: 'delta',
    OTEL_METRIC_EXPORT_INTERVAL: String(config.metricExportIntervalMs),
    OTEL_METRIC_EXPORT_TIMEOUT: String(config.metricExportIntervalMs * 2),
    OTEL_SERVICE_NAME: `rmv2-soak-${node}`,
  };
}

function changeLogEnv(settings: ChangeLogSettings): NodeJS.ProcessEnv {
  return {
    ZERO_CHANGE_STREAMER_SQLITE_CHANGE_LOG_MODE: settings.mode,
    ZERO_CHANGE_STREAMER_SQLITE_CHANGE_LOG_READ_PERCENT: String(
      settings.readPercent,
    ),
    ZERO_CHANGE_STREAMER_SQLITE_CHANGE_LOG_COLD_READ_PERCENT: String(
      settings.coldReadPercent,
    ),
    ZERO_CHANGE_STREAMER_SQLITE_CHANGE_LOG_COMPARE_PERCENT: String(
      settings.comparePercent,
    ),
    ZERO_CHANGE_STREAMER_SQLITE_CHANGE_LOG_RETENTION_MS: String(
      settings.retentionMs,
    ),
  };
}

export function replicationManagerEnv(
  config: SoakConfig,
  overrides: Partial<ChangeLogSettings> = {},
): NodeJS.ProcessEnv {
  const node = taskIDFor('rm');
  return {
    ...commonEnv(config, node),
    ZERO_PORT: String(config.rmPort),
    ZERO_NUM_SYNC_WORKERS: '0',
    ZERO_REPLICA_FILE: replicaFile(config, node),
    ZERO_LOG_LEVEL: config.rmLogLevel,
    ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS: String(config.startupDelayMs),
    ZERO_LITESTREAM_BACKUP_URL: config.backupURL,
    ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_SECONDS: String(
      config.backupIntervalSeconds,
    ),
    ZERO_LITESTREAM_SNAPSHOT_BACKUP_INTERVAL_HOURS: String(
      config.snapshotBackupIntervalHours,
    ),
    ZERO_LITESTREAM_VFS_POLL_INTERVAL_MS: String(config.vfsPollIntervalMs),
    ...changeLogEnv({...config.changeLog, ...overrides}),
  };
}

export function viewSyncerEnv(
  config: SoakConfig,
  index: number,
): NodeJS.ProcessEnv {
  const node = taskIDFor(index);
  return {
    ...commonEnv(config, node),
    ZERO_PORT: String(vsPort(config, index)),
    ZERO_NUM_SYNC_WORKERS: '1',
    ZERO_REPLICA_FILE: replicaFile(config, node),
    ZERO_LOG_LEVEL: config.vsLogLevel,
    ZERO_CHANGE_STREAMER_URI: `ws://127.0.0.1:${config.rmPort + 1}/`,
    // No ZERO_LITESTREAM_BACKUP_URL: `restoreReplica` takes it from the
    // replication-manager's snapshot response, and a view-syncer that
    // declared its own would be inferring backup configuration it does not
    // own. `undefined` clears whatever the parent process inherited; Node
    // drops undefined entries when it builds the child's environment.
    ZERO_LITESTREAM_BACKUP_URL: undefined,
    // REQUIRED. `apps/zbugs/.env` sets the change-log mode, dotenvx injects
    // any variable the child does not already have, and a view-syncer that
    // inherits a writing mode logs a warning and runs no log at all. Say
    // `off` rather than relying on omission.
    ...changeLogEnv({
      mode: 'off',
      readPercent: 0,
      coldReadPercent: 0,
      comparePercent: 0,
      retentionMs: config.changeLog.retentionMs,
    }),
  };
}

export function makeRunDir(root: string): {
  root: string;
  logsDir: string;
} {
  mkdirSync(root, {recursive: true});
  const logsDir = join(root, 'logs');
  mkdirSync(logsDir, {recursive: true});
  return {root, logsDir};
}
