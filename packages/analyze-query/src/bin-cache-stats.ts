/* eslint-disable no-console */
import '@dotenvx/dotenvx/config';
import {astToZQL} from '../../ast-to-zql/src/ast-to-zql.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {parseOptions} from '../../shared/src/options.ts';
import * as v from '../../shared/src/valita.ts';
import {
  appOptions,
  shardOptions,
  ZERO_ENV_VAR_PREFIX,
} from '../../zero-cache/src/config/zero-config.ts';
import {pgClient} from '../../zero-cache/src/types/pg.ts';
import {getShardID, upstreamSchema} from '../../zero-cache/src/types/shards.ts';
import {BigIntJSON} from '../../zero-cache/src/types/bigint-json.ts';
import chalk from 'chalk';

const options = {
  upstream: {
    db: v.string(),
  },
  cvr: {
    db: v.string(),
  },
  change: {
    db: v.string(),
  },
  app: appOptions,
  shard: shardOptions,
  dumpZql: v.boolean().optional(),
  detailed: v.boolean().optional(),
};

const config = parseOptions(
  options,
  process.argv.slice(2),
  ZERO_ENV_VAR_PREFIX,
);

const lc = createSilentLogContext();

async function upstreamStats() {
  const schema = upstreamSchema(getShardID(config));
  const sql = pgClient(lc, config.upstream.db);

  await printStats([
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
  ]);

  await sql.end();
}

async function cvrStats() {
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

  await printStats([
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
    ...((config.detailed
      ? [
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
        ]
      : []) satisfies [
      name: string,
      query: ReturnType<ReturnType<typeof pgClient>>,
    ][]),
  ]);

  if (config.dumpZql) {
    console.log(chalk.blue.bold('ZQL (without permissions) for each query:'));
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
        console.log(chalk.red.bold('HASH:'), queryHash);
        console.log(chalk.red.bold('ZQL:'), zql, '\n');
      } catch (e) {
        console.log(e);
        parseFailures.push(queryHash);
      }
    }
    if (parseFailures.length > 0) {
      console.log('Failed to parse the following hashes:', parseFailures);
    }
  }

  await sql.end();
}

async function changelogStats() {
  const schema = upstreamSchema(getShardID(config)) + '/cdc';
  const sql = pgClient(lc, config.change.db);

  await printStats([
    [
      'change log size',
      sql`SELECT COUNT(*) as "change_log_size" FROM ${sql(schema)}."changeLog"`,
    ],
  ]);
  await sql.end();
}

async function printStats(
  pendingQueries: [
    name: string,
    query: ReturnType<ReturnType<typeof pgClient>>,
  ][],
) {
  const results = await Promise.all(
    pendingQueries.map(async ([name, query]) => [name, await query]),
  );
  for (const result of results) {
    console.log('\n', chalk.blue.bold(result[0]), '\n');
    console.log(BigIntJSON.stringify(result[1], null, 2));
  }
}

await changelogStats();
await upstreamStats();
await cvrStats();
