import {spawn, type ChildProcess} from 'node:child_process';
import {rm} from 'node:fs/promises';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import type {BenchmarkConfig} from './config.ts';
import {appRoot, repoRoot} from './config.ts';
import {sleep} from './util.ts';

export type ProcessCommand = {
  readonly name: string;
  readonly command: readonly string[];
  readonly cwd: string;
};

export type ManagedProcess = ProcessCommand & {
  readonly child: ChildProcess;
  stop(): Promise<void>;
};

export async function deployPermissions(
  config: BenchmarkConfig,
): Promise<ProcessCommand> {
  const deployPermissionsMain = fileURLToPath(
    new URL(
      '../../../packages/zero-cache/src/scripts/deploy-permissions.ts',
      import.meta.url,
    ),
  );
  const schemaPath = join(appRoot, 'src/permissions.ts');
  const command = [
    process.execPath,
    deployPermissionsMain,
    '--schema-path',
    schemaPath,
    '--upstream-db',
    config.pg.url,
    '--app-id',
    config.zero.appID,
    '--force',
  ];
  await runCommand(command[0], command.slice(1), repoRoot);
  return {
    name: 'zero-deploy-permissions',
    command,
    cwd: repoRoot,
  };
}

export async function startPostgres(): Promise<ProcessCommand> {
  const cwd = join(appRoot, 'docker');
  await runCommand('docker', ['compose', 'up', '-d', 'postgres'], cwd);
  return {
    name: 'postgres',
    command: ['docker', 'compose', 'up', '-d', 'postgres'],
    cwd,
  };
}

export async function stopPostgres(): Promise<void> {
  await runCommand('docker', ['compose', 'down'], join(appRoot, 'docker'));
}

export async function removeReplicaFiles(replicaFile: string): Promise<void> {
  await Promise.all(
    [replicaFile, `${replicaFile}-shm`, `${replicaFile}-wal`].map(file =>
      rm(file, {force: true}),
    ),
  );
}

export function startZeroCache(config: BenchmarkConfig): ManagedProcess {
  const zeroCacheMain = fileURLToPath(
    new URL(
      '../../../packages/zero-cache/src/server/runner/main.ts',
      import.meta.url,
    ),
  );
  const command = [process.execPath, '--trace-warnings', zeroCacheMain];
  const env = zeroCacheEnv(config);
  const child = spawn(command[0], command.slice(1), {
    cwd: repoRoot,
    env,
    stdio: 'inherit',
  });

  return {
    name: 'zero-cache',
    command,
    cwd: repoRoot,
    child,
    stop: () => stopChild(child, 'SIGQUIT'),
  };
}

export async function waitForZeroCache(
  cacheURL: string,
  timeoutMs: number,
  process: ManagedProcess | undefined,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let exited = false;
  let lastError: unknown;
  const onExit = () => {
    exited = true;
  };
  process?.child.once('exit', onExit);
  try {
    while (Date.now() < deadline) {
      if (exited) {
        throw new Error('zero-cache exited before becoming ready');
      }
      try {
        const response = await fetch(new URL('/statz', cacheURL));
        if (response.ok || response.status === 401 || response.status === 403) {
          return;
        }
        lastError = new Error(`HTTP ${response.status}`);
      } catch (error) {
        lastError = error;
      }
      await sleep(500);
    }
  } finally {
    process?.child.off('exit', onExit);
  }
  throw new Error(
    `Timed out waiting for zero-cache after ${timeoutMs}ms: ${String(lastError)}`,
  );
}

function zeroCacheEnv(config: BenchmarkConfig): NodeJS.ProcessEnv {
  return {
    ...process.env,
    NODE_ENV: 'development',
    DO_NOT_TRACK: '1',
    ZERO_ENABLE_TELEMETRY: 'false',
    ZERO_UPSTREAM_DB: config.pg.url,
    ZERO_CVR_DB: config.pg.url,
    ZERO_CHANGE_DB: config.pg.url,
    ZERO_REPLICA_FILE: config.zero.replicaFile,
    ZERO_APP_ID: config.zero.appID,
    ZERO_TASK_ID: `zero-throughput-${config.runID}`,
    ZERO_PORT: String(config.zero.port),
    ZERO_NUM_SYNC_WORKERS: String(config.zero.numSyncWorkers),
    ZERO_UPSTREAM_MAX_CONNS: String(config.zero.upstreamMaxConns),
    ZERO_CVR_MAX_CONNS: String(config.zero.cvrMaxConns),
    ZERO_CHANGE_MAX_CONNS: String(config.zero.changeMaxConns),
    ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS: '0',
    ZERO_REPLICATION_LAG_REPORT_INTERVAL_MS: '1000',
    ZERO_LOG_LEVEL: config.zero.logLevel,
    ZERO_LOG_FORMAT: 'text',
  };
}

async function runCommand(
  command: string,
  args: readonly string[],
  cwd: string,
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(command, args, {cwd, stdio: 'inherit'});
    child.once('error', reject);
    child.once('exit', code => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`${command} ${args.join(' ')} exited with ${code}`));
      }
    });
  });
}

async function stopChild(
  child: ChildProcess,
  signal: NodeJS.Signals,
): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  const exited = new Promise<void>(resolve =>
    child.once('exit', () => resolve()),
  );
  child.kill(signal);
  const timeout = sleep(5_000).then(() => {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
    }
  });
  await Promise.race([exited, timeout]);
  await exited;
}
