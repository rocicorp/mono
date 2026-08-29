import {spawn, type ChildProcess} from 'node:child_process';
import {once} from 'node:events';
import {
  copyFileSync,
  createWriteStream,
  existsSync,
  mkdirSync,
  type WriteStream,
} from 'node:fs';
import {rm} from 'node:fs/promises';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import type {BenchmarkConfig} from './config.ts';
import {appPath, appRoot, repoRoot} from './config.ts';
import {
  profileQueryIndexesForRun,
  profileQueryName,
} from './profile-queries.ts';
import {sleep} from './util.ts';

const OTLP_METRICS_PATH_REGEX = /\/v1\/metrics$/;

export type ProcessCommand = {
  readonly name: string;
  readonly command: readonly string[];
  readonly cwd: string;
  readonly logPath?: string | undefined;
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

export async function analyzeProfileQueries(
  config: BenchmarkConfig,
): Promise<ProcessCommand> {
  const analyzeMain = fileURLToPath(new URL('analyze.ts', import.meta.url));
  const logPath = queryPlanAnalysisLogPath(config);
  const logStream = createWriteStream(logPath, {flags: 'w'});
  const indexes = profileQueryIndexesForRun(
    config.profile,
    config.queriesPerUser,
  );
  const command = [
    process.execPath,
    analyzeMain,
    '--zero-cache-url',
    config.cacheURL,
    '--profile',
    config.profile,
    '--model',
    config.model,
    '--rows-per-query',
    String(config.rowsPerQuery),
    '--join-plans',
  ];

  try {
    await writeLog(
      logStream,
      [
        `zero-throughput query plan analysis`,
        `runID: ${config.runID}`,
        `profile: ${config.profile}`,
        `model: ${config.model}`,
        `queriesPerUser: ${config.queriesPerUser}`,
        `rowsPerQuery: ${config.rowsPerQuery}`,
        `cacheURL: ${config.cacheURL}`,
        `distinctQueries: ${indexes.length}`,
        '',
      ].join('\n'),
    );

    for (const queryIndex of indexes) {
      const queryName = profileQueryName(config.profile, queryIndex);
      const args = [
        '--zero-cache-url',
        config.cacheURL,
        '--profile',
        config.profile,
        '--model',
        config.model,
        '--query-index',
        String(queryIndex),
        '--rows-per-query',
        String(config.rowsPerQuery),
        '--join-plans',
      ];
      await writeLog(
        logStream,
        [
          `\n================================================================================`,
          `query: ${queryName}`,
          `queryIndex: ${queryIndex}`,
          `command: ${process.execPath} ${[analyzeMain, ...args].join(' ')}`,
          `================================================================================\n`,
        ].join('\n'),
      );
      await runCommandToLog(
        process.execPath,
        [analyzeMain, ...args],
        repoRoot,
        logStream,
      );
    }
  } finally {
    await closeLog(logStream);
  }

  return {
    name: 'zero-analyze-profile-queries',
    command,
    cwd: repoRoot,
    logPath,
  };
}

export function queryPlanAnalysisLogPath(config: BenchmarkConfig): string {
  return join(processLogsDir(config), `${config.runID}-query-plans.log`);
}

export async function removeReplicaFiles(replicaFile: string): Promise<void> {
  const files: string[] = [
    replicaFile,
    `${replicaFile}-shm`,
    `${replicaFile}-wal`,
    `${replicaFile}-rm`,
    `${replicaFile}-rm-shm`,
    `${replicaFile}-rm-wal`,
  ];
  for (let i = 0; i < 20; i++) {
    files.push(
      `${replicaFile}-vs${i}`,
      `${replicaFile}-vs${i}-shm`,
      `${replicaFile}-vs${i}-wal`,
    );
  }
  await Promise.all(files.map(file => rm(file, {force: true})));
}

export type StartedTopology = {
  readonly processes: readonly ManagedProcess[];
  readonly readyURLs: readonly string[];
  stop(): Promise<void>;
};

export async function startZeroTopology(
  config: BenchmarkConfig,
  metricsEndpoint?: string | undefined,
): Promise<StartedTopology> {
  if (config.profileRM || config.profileVS) {
    mkdirSync(appPath(config.profileDir), {recursive: true});
  }

  if (config.topology === 'single') {
    const singleProc = spawnZeroProcess({
      config,
      name: 'zero-cache',
      port: config.zero.port,
      numSyncWorkers: config.zero.numSyncWorkers,
      replicaFile: config.zero.replicaFile,
      metricsEndpoint,
      profile: config.profileVS,
    });
    return {
      processes: [singleProc],
      readyURLs: [config.cacheURL],
      stop: () => singleProc.stop(),
    };
  }

  // Distributed topology: 1 RM + N View-Syncers
  const processes: ManagedProcess[] = [];
  const basePort = config.zero.port;
  const rmPort = basePort;
  const rmChangeStreamerPort = basePort + 1;

  // 1. Replication Manager
  const rmProcess = spawnZeroProcess({
    config,
    name: 'rm',
    port: rmPort,
    changeStreamerPort: rmChangeStreamerPort,
    changeStreamerMode: 'dedicated',
    numSyncWorkers: 0,
    replicaFile: `${config.zero.replicaFile}-rm`,
    metricsEndpoint,
    profile: config.profileRM,
  });
  processes.push(rmProcess);

  // The RM must finish initial sync before copying its replica to View-Syncers
  await waitForZeroCache(
    `http://127.0.0.1:${rmPort}`,
    config.zero.readyTimeoutMs,
    rmProcess,
  );

  // 2. View-Syncers (1 to N)
  const readyURLs: string[] = [];
  for (let i = 0; i < config.numViewSyncers; i++) {
    const vsPort = basePort + 10 + i;
    const vsURL = `http://127.0.0.1:${vsPort}`;
    readyURLs.push(vsURL);

    copyReplicaFile(
      `${config.zero.replicaFile}-rm`,
      `${config.zero.replicaFile}-vs${i}`,
    );

    const vs = spawnZeroProcess({
      config,
      name: `vs-${i}`,
      port: vsPort,
      changeStreamerURI: `http://127.0.0.1:${rmChangeStreamerPort}`,
      numSyncWorkers: config.zero.numSyncWorkers,
      replicaFile: `${config.zero.replicaFile}-vs${i}`,
      metricsEndpoint,
      profile: config.profileVS && i === 0,
    });
    processes.push(vs);
  }

  return {
    processes,
    readyURLs,
    stop: async () => {
      await Promise.all(processes.map(p => p.stop()));
    },
  };
}

function copyReplicaFile(src: string, dst: string): void {
  copyFileSync(src, dst);
  if (existsSync(`${src}-wal`)) {
    copyFileSync(`${src}-wal`, `${dst}-wal`);
  }
  if (existsSync(`${src}-shm`)) {
    copyFileSync(`${src}-shm`, `${dst}-shm`);
  }
}

function spawnZeroProcess(args: {
  readonly config: BenchmarkConfig;
  readonly name: string;
  readonly port: number;
  readonly numSyncWorkers: number;
  readonly replicaFile: string;
  readonly changeStreamerURI?: string | undefined;
  readonly changeStreamerPort?: number | undefined;
  readonly changeStreamerMode?: string | undefined;
  readonly metricsEndpoint?: string | undefined;
  readonly profile?: boolean | undefined;
}): ManagedProcess {
  const {config, name, port, numSyncWorkers, replicaFile} = args;
  const zeroCacheMain = fileURLToPath(
    new URL(
      '../../../packages/zero-cache/src/server/runner/main.ts',
      import.meta.url,
    ),
  );
  const command = [process.execPath, '--trace-warnings', zeroCacheMain];
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    NODE_ENV: 'development',
    DO_NOT_TRACK: '1',
    ZERO_ENABLE_TELEMETRY: 'false',
    ZERO_UPSTREAM_DB: config.pg.url,
    ZERO_CVR_DB: config.pg.url,
    ZERO_CHANGE_DB: config.pg.url,
    ZERO_REPLICA_FILE: replicaFile,
    ZERO_APP_ID: config.zero.appID,
    ZERO_TASK_ID: `zero-throughput-${name}-${config.runID}`,
    ZERO_PORT: String(port),
    ZERO_NUM_SYNC_WORKERS: String(numSyncWorkers),
    ZERO_UPSTREAM_MAX_CONNS: String(config.zero.upstreamMaxConns),
    ZERO_CVR_MAX_CONNS: String(config.zero.cvrMaxConns),
    ZERO_CHANGE_MAX_CONNS: String(config.zero.changeMaxConns),
    ZERO_LOG_LEVEL: config.zero.logLevel,
    ZERO_LOG_FORMAT: 'text',
    ZERO_ALLOW_LEGACY_QUERIES: 'true',
  };

  if (args.changeStreamerURI) {
    env.ZERO_CHANGE_STREAMER_URI = args.changeStreamerURI;
  }
  if (args.changeStreamerPort !== undefined) {
    env.ZERO_CHANGE_STREAMER_PORT = String(args.changeStreamerPort);
  }
  if (args.changeStreamerMode) {
    env.ZERO_CHANGE_STREAMER_MODE = args.changeStreamerMode;
  }

  if (args.metricsEndpoint) {
    const baseURL = args.metricsEndpoint.replace(OTLP_METRICS_PATH_REGEX, '');
    env.OTEL_EXPORTER_OTLP_ENDPOINT = baseURL;
    env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT = args.metricsEndpoint;
    env.OTEL_METRICS_EXPORTER = 'otlp';
    env.OTEL_EXPORTER_OTLP_PROTOCOL = 'http/json';
    env.OTEL_METRIC_EXPORT_INTERVAL = '500';
    env.OTEL_METRIC_EXPORT_TIMEOUT = '500';
  }

  if (args.profile) {
    const profDir = appPath(config.profileDir);
    mkdirSync(profDir, {recursive: true});
    env.NODE_OPTIONS = `--cpu-prof --cpu-prof-dir="${profDir}" ${process.env.NODE_OPTIONS ?? ''}`;
  }

  const logPath =
    config.processLogMode === 'file'
      ? join(processLogsDir(config), `${config.runID}-${name}.log`)
      : undefined;
  const logStream =
    logPath === undefined ? undefined : createWriteStream(logPath);
  const child = spawn(command[0], command.slice(1), {
    cwd: repoRoot,
    env,
    stdio:
      config.processLogMode === 'inherit'
        ? 'inherit'
        : [
            'ignore',
            config.processLogMode === 'file' ? 'pipe' : 'ignore',
            config.processLogMode === 'file' ? 'pipe' : 'ignore',
          ],
  });
  pipeProcessLogs(child, logStream);

  return {
    name,
    command,
    cwd: repoRoot,
    logPath,
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

async function runCommandToLog(
  command: string,
  args: readonly string[],
  cwd: string,
  logStream: WriteStream,
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout?.on('data', chunk => {
      logStream.write(chunk);
    });
    child.stderr?.on('data', chunk => {
      logStream.write(chunk);
    });
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

async function writeLog(stream: WriteStream, text: string): Promise<void> {
  if (!stream.write(text)) {
    await once(stream, 'drain');
  }
}

async function closeLog(stream: WriteStream): Promise<void> {
  stream.end();
  await once(stream, 'finish');
}

function processLogsDir(config: BenchmarkConfig): string {
  const logsDir = appPath(config.logsDir);
  mkdirSync(logsDir, {recursive: true});
  return logsDir;
}

function pipeProcessLogs(
  child: ChildProcess,
  logStream: WriteStream | undefined,
): void {
  if (logStream === undefined) {
    return;
  }
  child.stdout?.pipe(logStream, {end: false});
  child.stderr?.pipe(logStream, {end: false});
  child.once('close', () => logStream.end());
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
