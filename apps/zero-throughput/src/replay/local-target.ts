import {spawn, type ChildProcess} from 'node:child_process';
import {createWriteStream, mkdirSync} from 'node:fs';
import {readFile, rm} from 'node:fs/promises';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import postgres from 'postgres';
import type {ClientSchema} from '../../../../packages/zero-protocol/src/client-schema.ts';
import {waitForPostgres} from '../db.ts';
import {sleep} from '../util.ts';
import {quoteIdentifier, quoteTable, sqlLiteral} from './backfill.ts';

/**
 * A local stand-in for a deployed stack: a Postgres database whose tables
 * are generated from the workload's client schema and filled by a seed
 * script, a query server, and a zero-cache replicating that database.
 */
export type LocalTargetOptions = {
  /** Connection URL of the server that hosts the database. */
  readonly pgServerURL: string;
  readonly database: string;
  /** Drop and recreate the database, replica and zero-cache state. */
  readonly reset: boolean;
  readonly clientSchema: ClientSchema;
  readonly userIDs: readonly string[];
  readonly seedSQLFiles: readonly string[];
  /** Shell command that starts the query server; gets PORT and QUERY_SECRET. */
  readonly queryServerCommand: string;
  readonly queryServerPort: number;
  readonly queryPath: string;
  readonly authSecret: string;
  readonly zeroPort: number;
  readonly numSyncWorkers: number;
  readonly replicaFile: string;
  readonly logsDir: string;
  readonly runID: string;
  readonly log: (message: string) => void;
};

export type LocalTarget = {
  readonly cacheURL: string;
  readonly pgURL: string;
  stop(): Promise<void>;
};

const APP_ID = 'replay';
const PUBLICATION = 'replay_tables';

export function databaseURL(serverURL: string, database: string): string {
  const url = new URL(serverURL);
  url.pathname = `/${database}`;
  return url.toString();
}

/** `CREATE` statements for the tables a client schema describes. */
export function schemaDDL(clientSchema: ClientSchema): string[] {
  const statements: string[] = [];
  const schemas = new Set<string>();
  for (const [name, table] of Object.entries(clientSchema.tables)) {
    const schema = name.includes('.') ? name.split('.')[0] : 'public';
    if (!schemas.has(schema)) {
      schemas.add(schema);
      statements.push(`CREATE SCHEMA IF NOT EXISTS ${quoteIdentifier(schema)}`);
    }
    const primaryKey = new Set(table.primaryKey ?? []);
    const columns = Object.entries(table.columns).map(
      ([column, {type}]) =>
        `${quoteIdentifier(column)} ${pgType(type)}${primaryKey.has(column) ? ' NOT NULL' : ''}`,
    );
    if (primaryKey.size > 0) {
      columns.push(
        `PRIMARY KEY (${Array.from(primaryKey, quoteIdentifier).join(', ')})`,
      );
    }
    statements.push(
      `CREATE TABLE ${quoteTable(name)} (\n  ${columns.join(',\n  ')}\n)`,
    );
    // Joins go through `*_id` columns; without indexes on them every
    // correlated lookup in the replica is a table scan.
    const leading = table.primaryKey?.[0];
    for (const column of Object.keys(table.columns)) {
      if (column.endsWith('_id') && column !== leading) {
        statements.push(
          `CREATE INDEX ON ${quoteTable(name)} (${quoteIdentifier(column)})`,
        );
      }
    }
  }
  statements.push(
    `CREATE PUBLICATION ${PUBLICATION} FOR TABLES IN SCHEMA ${Array.from(
      schemas,
      quoteIdentifier,
    ).join(', ')}`,
  );
  return statements;
}

function pgType(type: string): string {
  switch (type) {
    case 'number':
      return 'double precision';
    case 'boolean':
      return 'boolean';
    case 'json':
      return 'jsonb';
    default:
      return 'text';
  }
}

export async function startLocalTarget(
  o: LocalTargetOptions,
): Promise<LocalTarget> {
  await waitForPostgres(o.pgServerURL, 60_000);
  const pgURL = databaseURL(o.pgServerURL, o.database);
  const children: ChildProcess[] = [];
  const stop = async () => {
    for (const child of children.reverse()) {
      await stopChild(child);
    }
  };

  const admin = postgres(o.pgServerURL, {max: 1, onnotice: () => undefined});
  try {
    const [exists] = await admin<{n: number}[]>`
      SELECT count(*)::int AS n FROM pg_database WHERE datname = ${o.database}`;
    if (o.reset && exists.n > 0) {
      o.log(`dropping database ${o.database}`);
      // A logical slot pins its database, so the slots go first.
      for (const slot of await admin<
        {slotName: string; activePID: number | null}[]
      >`SELECT slot_name AS "slotName", active_pid AS "activePID"
          FROM pg_replication_slots WHERE database = ${o.database}`) {
        if (slot.activePID !== null) {
          await admin`SELECT pg_terminate_backend(${slot.activePID})`;
          await sleep(500);
        }
        await admin`SELECT pg_drop_replication_slot(${slot.slotName})`;
      }
      await admin.unsafe(
        `DROP DATABASE ${quoteIdentifier(o.database)} WITH (FORCE)`,
      );
    }
    if (o.reset || exists.n === 0) {
      await admin.unsafe(`CREATE DATABASE ${quoteIdentifier(o.database)}`);
      await seedDatabase(pgURL, o);
    } else {
      o.log(`reusing database ${o.database}; pass --local-reset to rebuild it`);
    }
  } finally {
    await admin.end();
  }
  if (o.reset) {
    await Promise.all(
      ['', '-wal', '-shm', '-wal2'].map(suffix =>
        rm(`${o.replicaFile}${suffix}`, {force: true}),
      ),
    );
  }

  mkdirSync(o.logsDir, {recursive: true});
  try {
    const queryLog = join(o.logsDir, `${o.runID}-query-server.log`);
    o.log(`starting query server (${queryLog})`);
    const queryServer = spawnLogged(
      'sh',
      ['-c', o.queryServerCommand],
      queryLog,
      {PORT: String(o.queryServerPort), QUERY_SECRET: o.authSecret},
    );
    children.push(queryServer);
    await waitForHTTP(
      `http://127.0.0.1:${o.queryServerPort}/health`,
      120_000,
      queryServer,
      queryLog,
    );

    const zeroLog = join(o.logsDir, `${o.runID}-zero-cache.log`);
    o.log(`starting zero-cache (${zeroLog})`);
    const zeroCacheMain = fileURLToPath(
      new URL(
        '../../../../packages/zero-cache/src/server/runner/main.ts',
        import.meta.url,
      ),
    );
    const zeroCache = spawnLogged(process.execPath, [zeroCacheMain], zeroLog, {
      NODE_ENV: 'development',
      DO_NOT_TRACK: '1',
      ZERO_ENABLE_TELEMETRY: 'false',
      ZERO_UPSTREAM_DB: pgURL,
      ZERO_CVR_DB: pgURL,
      ZERO_CHANGE_DB: pgURL,
      ZERO_REPLICA_FILE: o.replicaFile,
      ZERO_APP_ID: APP_ID,
      ZERO_APP_PUBLICATIONS: PUBLICATION,
      ZERO_PORT: String(o.zeroPort),
      ZERO_NUM_SYNC_WORKERS: String(o.numSyncWorkers),
      ZERO_QUERY_URL: `http://127.0.0.1:${o.queryServerPort}${o.queryPath}`,
      // Opaque tokens need both URLs set; nothing here calls mutate.
      ZERO_MUTATE_URL: `http://127.0.0.1:${o.queryServerPort}/api/mutate`,
      ZERO_LOG_FORMAT: 'text',
    });
    children.push(zeroCache);
    const cacheURL = `http://127.0.0.1:${o.zeroPort}`;
    // The first start copies the whole database into the replica.
    await waitForHTTP(
      `${cacheURL}/statz`,
      30 * 60_000,
      zeroCache,
      zeroLog,
      [401, 403],
    );
    return {cacheURL, pgURL, stop};
  } catch (e) {
    await stop();
    throw e;
  }
}

async function seedDatabase(
  pgURL: string,
  o: LocalTargetOptions,
): Promise<void> {
  const sql = postgres(pgURL, {max: 1, onnotice: () => undefined});
  try {
    o.log(
      `creating ${Object.keys(o.clientSchema.tables).length} tables from the client schema`,
    );
    for (const statement of schemaDDL(o.clientSchema)) {
      await sql.unsafe(statement);
    }
    // The replayed users, for seed scripts to build their data around. In a
    // schema of its own so the publication does not replicate it.
    await sql.unsafe(`CREATE SCHEMA replay_meta`);
    await sql.unsafe(
      `CREATE TABLE replay_meta.users (ordinal int PRIMARY KEY, user_id text NOT NULL)`,
    );
    const users = [...new Set(o.userIDs)];
    for (let i = 0; i < users.length; i += 1000) {
      const values = users
        .slice(i, i + 1000)
        .map((u, j) => `(${i + j}, ${sqlLiteral(u)})`)
        .join(',');
      await sql.unsafe(`INSERT INTO replay_meta.users VALUES ${values}`);
    }
    for (const file of o.seedSQLFiles) {
      o.log(`running seed ${file}`);
      const started = Date.now();
      await sql.unsafe(await readFile(file, 'utf8'));
      o.log(`seed ${file} took ${((Date.now() - started) / 1000).toFixed(1)}s`);
    }
    await sql.unsafe('ANALYZE');
  } finally {
    await sql.end();
  }
}

function spawnLogged(
  command: string,
  args: readonly string[],
  logPath: string,
  env: Readonly<Record<string, string>>,
): ChildProcess {
  const log = createWriteStream(logPath);
  // Its own process group, so stopping it also stops what a shell started.
  const child = spawn(command, args, {
    env: {...process.env, ...env},
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: true,
  });
  const killGroup = () => {
    try {
      process.kill(-(child.pid as number), 'SIGKILL');
    } catch {
      // Already gone.
    }
  };
  process.once('exit', killGroup);
  child.once('exit', () => process.off('exit', killGroup));
  child.stdout?.pipe(log, {end: false});
  child.stderr?.pipe(log, {end: false});
  child.once('close', () => log.end());
  return child;
}

async function waitForHTTP(
  url: string,
  timeoutMs: number,
  child: ChildProcess,
  logPath: string,
  alsoOK: readonly number[] = [],
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error(`${url}: the process exited; see ${logPath}`);
    }
    try {
      const response = await fetch(url);
      if (response.ok || alsoOK.includes(response.status)) {
        return;
      }
      lastError = new Error(`HTTP ${response.status}`);
    } catch (e) {
      lastError = e;
    }
    await sleep(500);
  }
  throw new Error(`Timed out waiting for ${url}: ${String(lastError)}`);
}

async function stopChild(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  const exited = new Promise<void>(resolve =>
    child.once('exit', () => resolve()),
  );
  const signalGroup = (signal: NodeJS.Signals) => {
    try {
      process.kill(-(child.pid as number), signal);
    } catch {
      child.kill(signal);
    }
  };
  signalGroup('SIGTERM');
  const timer = setTimeout(signalGroup, 10_000, 'SIGKILL');
  await exited;
  clearTimeout(timer);
}
