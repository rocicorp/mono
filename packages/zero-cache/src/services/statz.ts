import {astToZQL} from '../../../ast-to-zql/src/ast-to-zql.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {type ZeroConfig} from '../config/zero-config.ts';
import {getShardID, upstreamSchema} from '../types/shards.ts';
import {pgClient} from '../types/pg.ts';
import type {Writable} from 'stream';
import {BigIntJSON} from '../types/bigint-json.ts';
import os from 'os';
import fs from 'fs';
import type {FastifyReply, FastifyRequest} from 'fastify';
import {Database} from '../../../zqlite/src/db.ts';
import auth from 'basic-auth';

const lc = createSilentLogContext();

async function upstreamStats(config: ZeroConfig, out: Writable) {
  const schema = upstreamSchema(getShardID(config));
  const sql = pgClient(lc, config.upstream.db);

  out.write(header('Upstream'));

  await printPgStats(
    [
      [
        'num replicas',
        sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."replicas"`,
      ],
      [
        'num clients with mutations',
        sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."clients"`,
      ],
      [
        'num mutations processed',
        sql`SELECT SUM("lastMutationID") as "c" FROM ${sql(schema)}."clients"`,
      ],
    ],
    out,
  );

  await sql.end();
}

async function cvrStats(config: ZeroConfig, out: Writable) {
  out.write(header('CVR'));

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

  await printPgStats(
    [
      [
        'total num queries',
        sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."desires"`,
      ],
      [
        'num unique query hashes',
        sql`SELECT COUNT(DISTINCT "queryHash") as "c" FROM ${sql(
          schema,
        )}."desires"`,
      ],
      [
        'num active queries',
        sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."desires" WHERE "inactivatedAt" IS NULL AND "deleted" = false`,
      ],
      [
        'num inactive queries',
        sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."desires" WHERE "inactivatedAt" IS NOT NULL AND ("inactivatedAt" + "ttl") > NOW()`,
      ],
      [
        'num deleted queries',
        sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."desires" WHERE "deleted" = true`,
      ],
      [
        'fresh queries percentiles',
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
        'rows per client group percentiles',
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
        'ast sizes',
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
        'biggest ast hash',
        sql`SELECT "queryHash", length("clientAST"::text) AS "ast_length"
      FROM ${sql(schema)}."queries"
      ORDER BY length("clientAST"::text) DESC
      LIMIT 1;`,
      ],
      [
        'total active queries per client and client group',
        numQueriesPerClientGroup(true),
      ],
      [
        'total inactive queries per client and client group',
        numQueriesPerClientGroup(false),
      ],
      [
        'total rows per client group',
        sql`SELECT "clientGroupID", COUNT(*) as "c" FROM ${sql(
          schema,
        )}."rows" GROUP BY "clientGroupID" ORDER BY "c" DESC`,
      ],
      [
        'num rows per query',
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
    ][],
    out,
  );

  out.write('ZQL (without permissions) for each query:');
  const queryAsts =
    await sql`SELECT "queryHash", "clientAST" FROM ${sql(schema)}."queries"`;

  const seenQueries = new Set<string>();
  const parseFailures: string[] = [];
  for (const row of queryAsts) {
    const {queryHash, clientAST} = row;
    if (seenQueries.has(queryHash)) {
      continue;
    }
    seenQueries.add(queryHash);

    try {
      const zql = clientAST.table + astToZQL(clientAST);
      out.write('HASH:' + queryHash);
      out.write('ZQL:' + zql + '\n');
    } catch (e) {
      parseFailures.push(queryHash);
    }
  }
  if (parseFailures.length > 0) {
    out.write('Failed to parse the following hashes: ' + parseFailures);
  }

  await sql.end();
}

async function changelogStats(config: ZeroConfig, out: Writable) {
  out.write(header('Change DB'));
  const schema = upstreamSchema(getShardID(config)) + '/cdc';
  const sql = pgClient(lc, config.change.db);

  await printPgStats(
    [
      [
        'change log size',
        sql`SELECT COUNT(*) as "change_log_size" FROM ${sql(schema)}."changeLog"`,
      ],
    ],
    out,
  );
  await sql.end();
}

function replicaStats(config: ZeroConfig, out: Writable) {
  out.write(header('Replica'));
  const db = new Database(lc, config.replica.file);
  printStats(
    'replica',
    [
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
    ] as const,
    out,
  );
}

function osStats(out: Writable) {
  printStats(
    'os',
    [
      ['load avg', os.loadavg()],
      ['uptime', os.uptime()],
      ['total mem', os.totalmem()],
      ['free mem', os.freemem()],
      ['cpus', os.cpus().length],
      ['platform', os.platform()],
      ['arch', os.arch()],
      ['release', os.release()],
      ['uptime', os.uptime()],
    ] as const,
    out,
  );
}

async function printPgStats(
  pendingQueries: [
    name: string,
    query: ReturnType<ReturnType<typeof pgClient>>,
  ][],
  out: Writable,
) {
  const results = await Promise.all(
    pendingQueries.map(async ([name, query]) => [name, await query]),
  );
  for (const [name, data] of results) {
    out.write('\n');
    out.write(name);
    out.write('\n');
    out.write(BigIntJSON.stringify(data, null, 2));
  }
}

function printStats(
  group: string,
  queries: readonly [name: string, result: unknown][],
  out: Writable,
) {
  out.write('\n' + header(group));
  for (const [name, result] of queries) {
    out.write('\n' + name + BigIntJSON.stringify(result, null, 2));
  }
}

export async function handleStatzRequest(
  config: ZeroConfig,
  req: FastifyRequest,
  res: FastifyReply,
) {
  const credentials = auth(req);
  const expectedPassword = config.adminPassword;
  if (!expectedPassword || credentials?.pass !== expectedPassword) {
    void res
      .code(401)
      .header('WWW-Authenticate', 'Basic realm="Statz Protected Area"')
      .send('Unauthorized');
    return;
  }

  await upstreamStats(config, res.raw);
  res.raw.write('\n\n');
  await cvrStats(config, res.raw);
  res.raw.write('\n\n');
  await changelogStats(config, res.raw);
  res.raw.write('\n\n');
  replicaStats(config, res.raw);
  res.raw.write('\n\n');
  osStats(res.raw);
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
