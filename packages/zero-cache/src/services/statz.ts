import type {LogContext} from '@rocicorp/logger';
import auth from 'basic-auth';
import type {FastifyReply, FastifyRequest} from 'fastify';
import fs from 'fs';
import os from 'os';
import type {Writable} from 'stream';
import {BigIntJSON} from '../../../shared/src/bigint-json.ts';
import {Database} from '../../../zqlite/src/db.ts';
import type {NormalizedZeroConfig as ZeroConfig} from '../config/normalize.ts';
import {isAdminPasswordValid} from '../config/zero-config.ts';
import {StatementRunner} from '../db/statements.ts';
import {pgClient} from '../types/pg.ts';
import {getShardID, upstreamSchema} from '../types/shards.ts';
import {fromLexiVersion} from './change-source/pg/lsn.ts';
import {getReplicationState} from './replicator/schema/replication-state.ts';

async function upstreamStats(lc: LogContext, config: ZeroConfig) {
  const schema = upstreamSchema(getShardID(config));
  const sql = pgClient(lc, config.upstream.db);
  try {
    return await getPgStats([
      [
        'numReplicas',
        sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."replicas"`,
      ],
      [
        'numClientsWithMutations',
        sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."clients"`,
      ],
      [
        'numMutationsProcessed',
        sql`SELECT SUM("lastMutationID") as "c" FROM ${sql(schema)}."clients"`,
      ],
    ]);
  } finally {
    await sql.end();
  }
}

async function cvrStats(lc: LogContext, config: ZeroConfig) {
  const schema = upstreamSchema(getShardID(config)) + '/cvr';
  const sql = pgClient(lc, config.cvr.db);

  function numQueriesPerClientGroup(
    active: boolean,
  ): ReturnType<ReturnType<typeof pgClient>> {
    const filter = active
      ? sql`WHERE "inactivatedAt" IS NULL AND deleted = false`
      : sql`WHERE "inactivatedAt" IS NOT NULL AND ("inactivatedAt" + "ttl") > NOW()`;
    return sql`WITH
    group_counts AS (
      SELECT
        "clientGroupID",
        COUNT(*) AS num_queries
      FROM ${sql(schema)}."desires"
      ${filter}
      GROUP BY "clientGroupID"
    ),
    -- Count distinct clientIDs per clientGroupID
    client_per_group_counts AS (
      SELECT
        "clientGroupID",
        COUNT(DISTINCT "clientID") AS num_clients
      FROM ${sql(schema)}."desires"
      ${filter}
      GROUP BY "clientGroupID"
    )
    -- Combine all the information
    SELECT
      g."clientGroupID",
      cpg.num_clients,
      g.num_queries
    FROM group_counts g
    JOIN client_per_group_counts cpg ON g."clientGroupID" = cpg."clientGroupID"
    ORDER BY g.num_queries DESC;`;
  }

  try {
    return await getPgStats([
      [
        'totalNumQueries',
        sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."desires"`,
      ],
      [
        'numUniqueQueryHashes',
        sql`SELECT COUNT(DISTINCT "queryHash") as "c" FROM ${sql(
          schema,
        )}."desires"`,
      ],
      [
        'numActiveQueries',
        sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."desires" WHERE "inactivatedAt" IS NULL AND "deleted" = false`,
      ],
      [
        'numInactiveQueries',
        sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."desires" WHERE "inactivatedAt" IS NOT NULL AND ("inactivatedAt" + "ttl") > NOW()`,
      ],
      [
        'numDeletedQueries',
        sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."desires" WHERE "deleted" = true`,
      ],
      [
        'freshQueriesPercentiles',
        sql`WITH client_group_counts AS (
        -- Count inactive desires per clientGroupID
        SELECT
          "clientGroupID",
          COUNT(*) AS fresh_count
        FROM ${sql(schema)}."desires"
        WHERE
          ("inactivatedAt" IS NOT NULL
          AND ("inactivatedAt" + "ttl") > NOW()) OR ("inactivatedAt" IS NULL
          AND deleted = false)
        GROUP BY "clientGroupID"
      )

      SELECT
        percentile_cont(0.50) WITHIN GROUP (ORDER BY fresh_count) AS "p50",
        percentile_cont(0.75) WITHIN GROUP (ORDER BY fresh_count) AS "p75",
        percentile_cont(0.90) WITHIN GROUP (ORDER BY fresh_count) AS "p90",
        percentile_cont(0.95) WITHIN GROUP (ORDER BY fresh_count) AS "p95",
        percentile_cont(0.99) WITHIN GROUP (ORDER BY fresh_count) AS "p99",
        MIN(fresh_count) AS "min",
        MAX(fresh_count) AS "max",
        AVG(fresh_count) AS "avg"
      FROM client_group_counts;`,
      ],
      [
        'rowsPerClientGroupPercentiles',
        sql`WITH client_group_counts AS (
        -- Count inactive desires per clientGroupID
        SELECT
          "clientGroupID",
          COUNT(*) AS row_count
        FROM ${sql(schema)}."rows"
        GROUP BY "clientGroupID"
      )
      SELECT
        percentile_cont(0.50) WITHIN GROUP (ORDER BY row_count) AS "p50",
        percentile_cont(0.75) WITHIN GROUP (ORDER BY row_count) AS "p75",
        percentile_cont(0.90) WITHIN GROUP (ORDER BY row_count) AS "p90",
        percentile_cont(0.95) WITHIN GROUP (ORDER BY row_count) AS "p95",
        percentile_cont(0.99) WITHIN GROUP (ORDER BY row_count) AS "p99",
        MIN(row_count) AS "min",
        MAX(row_count) AS "max",
        AVG(row_count) AS "avg"
      FROM client_group_counts;`,
      ],
      [
        // check for AST blowup due to DNF conversion.
        'astSizes',
        sql`SELECT
        percentile_cont(0.25) WITHIN GROUP (ORDER BY length("clientAST"::text)) AS "25th_percentile",
        percentile_cont(0.5) WITHIN GROUP (ORDER BY length("clientAST"::text)) AS "50th_percentile",
        percentile_cont(0.75) WITHIN GROUP (ORDER BY length("clientAST"::text)) AS "75th_percentile",
        percentile_cont(0.9) WITHIN GROUP (ORDER BY length("clientAST"::text)) AS "90th_percentile",
        percentile_cont(0.95) WITHIN GROUP (ORDER BY length("clientAST"::text)) AS "95th_percentile",
        percentile_cont(0.99) WITHIN GROUP (ORDER BY length("clientAST"::text)) AS "99th_percentile",
        MIN(length("clientAST"::text)) AS "minimum_length",
        MAX(length("clientAST"::text)) AS "maximum_length",
        AVG(length("clientAST"::text))::integer AS "average_length",
        COUNT(*) AS "total_records"
      FROM ${sql(schema)}."queries";`,
      ],
      [
        // output the hash of the largest AST
        'biggestAstHash',
        sql`SELECT "queryHash", length("clientAST"::text) AS "ast_length"
      FROM ${sql(schema)}."queries"
      ORDER BY length("clientAST"::text) DESC
      LIMIT 1;`,
      ],
      [
        'totalActiveQueriesPerClientAndClientGroup',
        numQueriesPerClientGroup(true),
      ],
      [
        'totalInactiveQueriesPerClientAndClientGroup',
        numQueriesPerClientGroup(false),
      ],
      [
        'totalRowsPerClientGroup',
        sql`SELECT "clientGroupID", COUNT(*) as "c" FROM ${sql(
          schema,
        )}."rows" GROUP BY "clientGroupID" ORDER BY "c" DESC`,
      ],
      [
        'numRowsPerQuery',
        sql`SELECT
        k.key AS "queryHash",
        COUNT(*) AS row_count
      FROM ${sql(schema)}."rows" r,
      LATERAL jsonb_each(r."refCounts") k
      GROUP BY k.key
      ORDER BY row_count DESC;`,
      ],
    ] satisfies [
      name: string,
      query: ReturnType<ReturnType<typeof pgClient>>,
    ][]);
  } finally {
    await sql.end();
  }
}

async function changeLogStats(lc: LogContext, config: ZeroConfig) {
  const schema = upstreamSchema(getShardID(config)) + '/cdc';
  const sql = pgClient(lc, config.change.db);

  try {
    return await getPgStats([
      [
        'changeLogSize',
        sql`SELECT COUNT(*) as "change_log_size" FROM ${sql(schema)}."changeLog"`,
      ],
    ]);
  } finally {
    await sql.end();
  }
}

function replicaStats(lc: LogContext, config: ZeroConfig) {
  const db = new Database(lc, config.replica.file);
  return Object.fromEntries([
    ['wal checkpoint', pick(first(db.pragma('WAL_CHECKPOINT')))],
    ['page count', pick(first(db.pragma('PAGE_COUNT')))],
    ['page size', pick(first(db.pragma('PAGE_SIZE')))],
    ['journal mode', pick(first(db.pragma('JOURNAL_MODE')))],
    ['synchronous', pick(first(db.pragma('SYNCHRONOUS')))],
    ['cache size', pick(first(db.pragma('CACHE_SIZE')))],
    ['auto vacuum', pick(first(db.pragma('AUTO_VACUUM')))],
    ['freelist count', pick(first(db.pragma('FREELIST_COUNT')))],
    ['wal autocheckpoint', pick(first(db.pragma('WAL_AUTOCHECKPOINT')))],
    ['db file stats', fs.statSync(config.replica.file)],
  ] as const);
}

function replicationStats(lc: LogContext, config: ZeroConfig) {
  const db = new Database(lc, config.replica.file);
  return getReplicationStats(db);
}

function getReplicationStats(db: Database) {
  const {stateVersion} = getReplicationState(new StatementRunner(db));
  const lsn = fromLexiVersion(stateVersion);
  return {lsn};
}

function osStats() {
  return Object.fromEntries([
    ['load avg', os.loadavg()],
    ['uptime', os.uptime()],
    ['total mem', os.totalmem()],
    ['free mem', os.freemem()],
    ['cpus', os.cpus().length],
    ['available parallelism', os.availableParallelism()],
    ['platform', os.platform()],
    ['arch', os.arch()],
    ['release', os.release()],
    ['uptime', os.uptime()],
  ] as const);
}

async function getPgStats(
  pendingQueries: [
    name: string,
    query: ReturnType<ReturnType<typeof pgClient>>,
  ][],
) {
  const results = await Promise.all(
    pendingQueries.map(async ([name, query]) => [name, await query] as const),
  );
  return Object.fromEntries(results);
}

type StatsObject = Record<string, unknown>;

function printStats(group: string, statsObject: StatsObject, out: Writable) {
  const queries = Object.entries(statsObject);
  out.write('\n' + header(group));
  for (const [name, result] of queries) {
    out.write('\n' + name + ': ' + BigIntJSON.stringify(result, null, 2));
  }
  out.write('\n');
}

/**
 * HTTP query parameters:
 * * `filter`: restricts the groups to compute stats
 * * `format=json`: returns the stats as a JSON object
 * * `pretty`: formats the JSON object with indentation
 */
export async function handleStatzRequest(
  lc: LogContext,
  config: ZeroConfig,
  req: FastifyRequest,
  res: FastifyReply,
) {
  const credentials = auth(req);
  if (!isAdminPasswordValid(lc, config, credentials?.pass)) {
    void res
      .code(401)
      .header('WWW-Authenticate', 'Basic realm="Statz Protected Area"')
      .send('Unauthorized');
    return;
  }

  const statsFns: Record<string, () => Promise<StatsObject> | StatsObject> = {
    upstream: () => upstreamStats(lc, config),
    cvr: () => cvrStats(lc, config),
    changeLog: () => changeLogStats(lc, config),
    replica: () => replicaStats(lc, config),
    replication: () => replicationStats(lc, config),
    os: () => osStats(),
  };

  async function computeStats(group: string): Promise<[string, StatsObject]> {
    try {
      return [group, await statsFns[group]()];
    } catch (e) {
      lc.error?.(`error computing ${group} stats`, e);
      return [group, {error: String(e)}];
    }
  }

  const query = req.query as Record<string, unknown>;
  const groups =
    typeof query.filter === 'string'
      ? query.filter.split(',')
      : Array.isArray(query.filter)
        ? query.filter
        : undefined;

  const stats = await Promise.all(
    groups
      ? groups.filter(g => g in statsFns).map(computeStats)
      : Object.keys(statsFns).map(computeStats),
  );

  if (query.format === 'json') {
    const indent = query.pretty !== undefined ? 2 : undefined;
    await res
      .header('Content-Type', 'application/json')
      .send(BigIntJSON.stringify(Object.fromEntries(stats), null, indent));
  } else {
    for (const [group, statsObject] of stats) {
      printStats(group, statsObject, res.raw);
    }
  }

  res.raw.end();
}

function first(x: object[]): object {
  return x[0];
}

function pick(x: object): unknown {
  return Object.values(x)[0];
}

function header(name: string): string {
  return `=== ${name} ===\n`;
}
