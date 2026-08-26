import {copyFileSync} from 'node:fs';
import {resolver} from '@rocicorp/resolver';
import postgres from 'postgres';
import {assert} from '../../../shared/src/asserts.ts';
import {randInt} from '../../../shared/src/rand.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import {DbFile} from '../../src/test/lite.ts';
import {postgresTypeConfig, type PostgresDB} from '../../src/types/pg.ts';
import {childWorker, type Worker} from '../../src/types/processes.ts';
import {ClientGroupSimulator} from './client-simulator.ts';
import type {BenchmarkConfig} from './config.ts';
import {getZbugsClientSchema} from './default-queries.ts';
import {OTelMetricsCollector} from './metrics-collector.ts';
import {ensureOutputDir, getWorkerProfilingEnv} from './profiler-bridge.ts';
import type {BenchmarkResult} from './results.ts';

const RUNNER_MAIN_URL = new URL(
  '../../src/server/runner/main.ts',
  import.meta.url,
);

interface RunningProcess {
  readonly worker: Worker;
  readonly type: 'rm' | 'vs';
  readonly index: number;
  readonly port: number;
  readonly exitPromise: Promise<number>;
}

export async function runPipelineBenchmark(
  config: BenchmarkConfig,
): Promise<BenchmarkResult> {
  // Ensure childWorker does not inherit test / runner CLI args
  process.argv = process.argv.slice(0, 2);

  await ensureOutputDir(config.outputDir);

  const runID = randInt(100000, 999999);
  const appID = config.appID || 'zero';

  // 1. Provision Databases
  const {upstreamDB, cvrDB, changeDB, cleanupDBs} = await setupDatabases(
    runID,
    config,
  );

  const runningProcesses: RunningProcess[] = [];
  const replicaFiles: DbFile[] = [];

  // 2. Start OTel Metrics Collector
  const metricsCollector = new OTelMetricsCollector();
  await metricsCollector.start();

  try {
    // 3. Seed Database
    await config.seedDatabase(upstreamDB.sql);

    // 4. Start Replication Manager 1
    const rm1ReplicaFile = new DbFile(`bench-rm1-${runID}`);
    replicaFiles.push(rm1ReplicaFile);

    const rm1Port = randInt(10000, 20000);
    const rm1Env: NodeJS.ProcessEnv = {
      ...process.env,
      ...getWorkerProfilingEnv('rm', 0, config),
      ZERO_PORT: String(rm1Port),
      ZERO_ADMIN_PASSWORD: 'benchmark-admin-password',
      ZERO_APP_PUBLICATIONS: 'zero_all',
      ZERO_NUM_SYNC_WORKERS: '0',
      ZERO_UPSTREAM_DB: upstreamDB.uri,
      ZERO_CVR_DB: cvrDB.uri,
      ZERO_CHANGE_DB: changeDB.uri,
      ZERO_REPLICA_FILE: rm1ReplicaFile.path,
      ZERO_APP_ID: appID,
      ZERO_CHANGE_STREAMER_ADDRESS: `localhost:${rm1Port + 1}`,
      ZERO_CHANGE_STREAMER_SQLITE_CHANGE_LOG_MODE: config.sqliteChangeLogMode,
      ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS: '0',
      ZERO_LOG_LEVEL: config.logLevel,
      OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${metricsCollector.port}`,
      OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: metricsCollector.endpoint,
      OTEL_METRICS_EXPORTER: 'otlp',
      OTEL_EXPORTER_OTLP_PROTOCOL: 'http/json',
      OTEL_METRIC_EXPORT_INTERVAL: '500',
      OTEL_METRIC_EXPORT_TIMEOUT: '500',
      SINGLE_PROCESS: '0',
    };

    const rm1Proc = await spawnWorker(rm1Env, 'rm', 0, rm1Port);
    runningProcesses.push(rm1Proc);

    // 5. Optional: Start RM2 (RMv2 HA standby)
    if (config.numReplicationManagers === 2) {
      const rm2ReplicaFile = new DbFile(`bench-rm2-${runID}`);
      replicaFiles.push(rm2ReplicaFile);
      copyFileSync(rm1ReplicaFile.path, rm2ReplicaFile.path);

      const rm2Port = randInt(20001, 30000);
      const rm2Env: NodeJS.ProcessEnv = {
        ...process.env,
        ...getWorkerProfilingEnv('rm', 1, config),
        ZERO_PORT: String(rm2Port),
        ZERO_ADMIN_PASSWORD: 'benchmark-admin-password',
        ZERO_APP_PUBLICATIONS: 'zero_all',
        ZERO_TASK_ID: `bench-rm2-${runID}`,
        ZERO_NUM_SYNC_WORKERS: '0',
        ZERO_UPSTREAM_DB: upstreamDB.uri,
        ZERO_CVR_DB: cvrDB.uri,
        ZERO_CHANGE_DB: changeDB.uri,
        ZERO_REPLICA_FILE: rm2ReplicaFile.path,
        ZERO_APP_ID: appID,
        ZERO_CHANGE_STREAMER_ADDRESS: `localhost:${rm2Port + 1}`,
        ZERO_CHANGE_STREAMER_SQLITE_CHANGE_LOG_MODE: config.sqliteChangeLogMode,
        ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS: '5000',
        ZERO_CHANGE_STREAMER_MODE: 'dedicated',
        ZERO_LOG_LEVEL: config.logLevel,
        OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${metricsCollector.port}`,
        OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: metricsCollector.endpoint,
        OTEL_METRICS_EXPORTER: 'otlp',
        OTEL_EXPORTER_OTLP_PROTOCOL: 'http/json',
        OTEL_METRIC_EXPORT_INTERVAL: '500',
        OTEL_METRIC_EXPORT_TIMEOUT: '500',
        SINGLE_PROCESS: '0',
      };

      const rm2Proc = await spawnWorker(rm2Env, 'rm', 1, rm2Port);
      runningProcesses.push(rm2Proc);
    }

    // 6. Start View-Syncers (1 to N)
    const vsPorts: number[] = [];
    for (let i = 0; i < config.numViewSyncers; i++) {
      const vsReplicaFile = new DbFile(`bench-vs-${runID}-${i}`);
      replicaFiles.push(vsReplicaFile);
      copyFileSync(rm1ReplicaFile.path, vsReplicaFile.path);

      const vsPort = randInt(30001 + i * 1000, 31000 + i * 1000);
      vsPorts.push(vsPort);

      const vsEnv: NodeJS.ProcessEnv = {
        ...process.env,
        ...getWorkerProfilingEnv('vs', i, config),
        ZERO_PORT: String(vsPort),
        ZERO_ADMIN_PASSWORD: 'benchmark-admin-password',
        ZERO_APP_PUBLICATIONS: 'zero_all',
        ZERO_NUM_SYNC_WORKERS: '1',
        ZERO_UPSTREAM_DB: upstreamDB.uri,
        ZERO_CVR_DB: cvrDB.uri,
        ZERO_CHANGE_DB: changeDB.uri,
        ZERO_REPLICA_FILE: vsReplicaFile.path,
        ZERO_APP_ID: appID,
        ZERO_CHANGE_STREAMER_MODE: 'discover',
        ZERO_LOG_LEVEL: config.logLevel,
        ZERO_MIN_ADVANCE_INTERVAL_MS: String(config.minAdvanceIntervalMs ?? 0),
        ZERO_ADAPTIVE_FRAME_RATE: config.adaptiveFrameRate ? '1' : '0',
        OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${metricsCollector.port}`,
        OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: metricsCollector.endpoint,
        OTEL_METRICS_EXPORTER: 'otlp',
        OTEL_EXPORTER_OTLP_PROTOCOL: 'http/json',
        OTEL_METRIC_EXPORT_INTERVAL: '500',
        OTEL_METRIC_EXPORT_TIMEOUT: '500',
        SINGLE_PROCESS: '0',
      };

      const vsProc = await spawnWorker(vsEnv, 'vs', i, vsPort);
      runningProcesses.push(vsProc);
    }

    // 7. Connect Simulated Clients
    const {clientSchema} = getZbugsClientSchema();
    const clientSimulator = new ClientGroupSimulator({
      viewSyncerPorts: vsPorts,
      clientsPerViewSyncer: config.clientsPerViewSyncer,
      queries: config.clientQueries,
      clientSchema,
    });

    await clientSimulator.connectAll();
    await clientSimulator.waitForAllHydrated(60_000);

    // Warm-up settling: Allow cold-start initialization, schema checks,
    // background CVR setups, and startup replication probes to fully settle.
    await sleep(2500);

    // Reset metric samples collected during cold start so benchmark measurements
    // strictly capture steady-state load performance.
    metricsCollector.reset();

    // 8. Run Active Load Phase
    const abortController = new AbortController();
    const loadStats = await config.loadGenerator(
      upstreamDB.sql,
      config.writeRatePerSecond,
      config.loadDurationSeconds,
      abortController.signal,
    );

    // 9. Dynamic Drain / Settling Phase (wait up to 30s or until all pokes received)
    const drainTimeoutMs = Math.max(config.drainTimeoutSeconds, 30) * 1000;
    const expectedPokesPerClient = Math.max(
      1,
      Math.floor(loadStats.writesSucceeded * 0.8),
    );
    await clientSimulator.waitForDrain({
      expectedPokesPerClient,
      timeoutMs: drainTimeoutMs,
      quiescentMs: 2500,
    });

    // 10. Collect Stats & Metrics
    const clientStats = clientSimulator.getAllStats();
    clientSimulator.closeAll();

    // Small delay to allow final OTel export flush
    await sleep(2000);

    const metricsSummary = metricsCollector.getSummary();

    const result: BenchmarkResult = {
      timestamp: new Date().toISOString(),
      topology: {
        numReplicationManagers: config.numReplicationManagers,
        numViewSyncers: config.numViewSyncers,
        totalClients: clientSimulator.totalClients,
        clientsPerViewSyncer: config.clientsPerViewSyncer,
      },
      config: {
        writeRatePerSecond: config.writeRatePerSecond,
        loadDurationSeconds: config.loadDurationSeconds,
        sqliteChangeLogMode: config.sqliteChangeLogMode,
        dbMode: config.dbMode,
      },
      loadStats,
      clientStats,
      metrics: metricsSummary,
    };

    return result;
  } finally {
    // 11. Teardown
    await metricsCollector.stop();

    for (const proc of runningProcesses) {
      try {
        proc.worker.kill('SIGTERM');
      } catch {
        // ignore
      }
    }

    await Promise.allSettled(runningProcesses.map(p => p.exitPromise));

    for (const file of replicaFiles) {
      try {
        file.delete();
      } catch {
        // ignore
      }
    }

    await cleanupDBs();
  }
}

async function spawnWorker(
  env: NodeJS.ProcessEnv,
  type: 'rm' | 'vs',
  index: number,
  port: number,
): Promise<RunningProcess> {
  const {promise: readyPromise, resolve: resolveReady} = resolver<void>();
  const {promise: exitPromise, resolve: resolveExit} = resolver<number>();

  const worker = childWorker(RUNNER_MAIN_URL, env);

  worker.onMessageType('ready', () => resolveReady());
  worker.on('close', code => resolveExit(code ?? 0));
  worker.on('error', () => {
    resolveExit(-1);
  });

  await readyPromise;

  return {
    worker,
    type,
    index,
    port,
    exitPromise,
  };
}

interface DBHandle {
  readonly sql: PostgresDB;
  readonly uri: string;
}

async function setupDatabases(
  runID: number,
  config: BenchmarkConfig,
): Promise<{
  upstreamDB: DBHandle;
  cvrDB: DBHandle;
  changeDB: DBHandle;
  cleanupDBs: () => Promise<void>;
}> {
  if (config.dbMode === 'external') {
    assert(config.upstreamDB, 'upstreamDB required when dbMode is external');
    assert(config.cvrDB, 'cvrDB required when dbMode is external');

    const upSql: PostgresDB = postgres(config.upstreamDB, postgresTypeConfig());
    const cvrSql: PostgresDB = postgres(config.cvrDB, postgresTypeConfig());
    const changeSql: PostgresDB = config.changeDB
      ? postgres(config.changeDB, postgresTypeConfig())
      : upSql;

    return {
      upstreamDB: {sql: upSql, uri: config.upstreamDB},
      cvrDB: {sql: cvrSql, uri: config.cvrDB},
      changeDB: {
        sql: changeSql,
        uri: config.changeDB ?? config.upstreamDB,
      },
      cleanupDBs: async () => {
        await Promise.allSettled([
          upSql.end(),
          cvrSql.end(),
          changeSql !== upSql ? changeSql.end() : Promise.resolve(),
        ]);
      },
    };
  }

  const upBaseURI =
    process.env.ZERO_UPSTREAM_DB ||
    process.env.PG_CONNECTION_STRING ||
    'postgres://user:password@localhost:6434/postgres';
  const cvrBaseURI =
    process.env.ZERO_CVR_DB ||
    'postgres://user:password@localhost:6435/postgres';

  const upName = `bench_up_${runID}`;
  const cvrName = `bench_cvr_${runID}`;
  const changeName = `bench_chg_${runID}`;

  const adminUp = postgres(upBaseURI);
  const adminCvr = postgres(cvrBaseURI);

  await adminUp.unsafe(`CREATE DATABASE "${upName}"`);
  await adminCvr.unsafe(`CREATE DATABASE "${cvrName}"`);
  await adminCvr.unsafe(`CREATE DATABASE "${changeName}"`);

  const upURI = buildDbURI(upBaseURI, upName);
  const cvrURI = buildDbURI(cvrBaseURI, cvrName);
  const changeURI = buildDbURI(cvrBaseURI, changeName);

  const upSql: PostgresDB = postgres(upURI, postgresTypeConfig());
  const cvrSql: PostgresDB = postgres(cvrURI, postgresTypeConfig());
  const changeSql: PostgresDB = postgres(changeURI, postgresTypeConfig());

  return {
    upstreamDB: {sql: upSql, uri: upURI},
    cvrDB: {sql: cvrSql, uri: cvrURI},
    changeDB: {sql: changeSql, uri: changeURI},
    cleanupDBs: async () => {
      await Promise.allSettled([upSql.end(), cvrSql.end(), changeSql.end()]);

      try {
        await adminUp.unsafe(
          `DROP DATABASE IF EXISTS "${upName}" WITH (FORCE)`,
        );
      } catch {
        // ignore
      }
      try {
        await adminCvr.unsafe(
          `DROP DATABASE IF EXISTS "${cvrName}" WITH (FORCE)`,
        );
        await adminCvr.unsafe(
          `DROP DATABASE IF EXISTS "${changeName}" WITH (FORCE)`,
        );
      } catch {
        // ignore
      }

      await Promise.allSettled([adminUp.end(), adminCvr.end()]);
    },
  };
}

function buildDbURI(baseURI: string, dbName: string): string {
  const url = new URL(baseURI);
  url.pathname = `/${dbName}`;
  return url.toString();
}
