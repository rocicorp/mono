/* eslint-disable no-console */
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
import type {AST} from '../../zero-protocol/src/ast.ts';

const options = {
  upstream: {
    db: v.string(),
  },
  cvr: {
    db: v.string(),
  },
  cdc: {
    db: v.string(),
  },
  app: appOptions,
  shard: shardOptions,
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
}

async function cvrStats() {
  const schema = upstreamSchema(getShardID(config)) + '/cvr';
  const sql = pgClient(lc, config.cvr.db);

  function numQueriesPerClientAndClientGroup(
    onlyFresh: boolean,
  ): ReturnType<ReturnType<typeof pgClient>> {
    const filter = onlyFresh
      ? sql`WHERE "expiresAt" IS NULL OR "expiresAt" < NOW()`
      : sql``;
    return sql`WITH 
    -- Count rows per clientID
    client_counts AS (
      SELECT 
        "clientGroupID",
        "clientID",
        COUNT(*) AS num_queries
      FROM ${sql(schema)}."desires"
      ${filter}
      GROUP BY "clientGroupID", "clientID"
    ),
    -- Count total rows per clientGroupID
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
      c."clientGroupID",
      cpg.num_clients,
      g.num_queries,
      json_agg(json_build_object(
        'clientID', c."clientID",
        'rows_count', c.num_queries
      )) AS client_details
    FROM client_counts c
    JOIN group_counts g ON c."clientGroupID" = g."clientGroupID"
    JOIN client_per_group_counts cpg ON c."clientGroupID" = cpg."clientGroupID"
    GROUP BY c."clientGroupID", cpg.num_clients, g.num_queries
    ORDER BY c."clientGroupID";`;
  }

  function rowsPerClientID(
    inactive: boolean,
  ): ReturnType<ReturnType<typeof pgClient>> {
    const filter = inactive ? sql`WHERE "inactivedAt" IS NOT NULL` : sql``;
    return sql`WITH desire_client_mapping AS (
      SELECT 
        "queryHash",
        "clientID"
      FROM ${sql(schema)}."desires"
      ${filter}
    )
    SELECT 
      d."clientID",
      COUNT(DISTINCT r."rowKey") AS total_rows
    FROM ${sql(schema)}."rows" r,
    LATERAL jsonb_each(r."refCounts") k
    JOIN desire_client_mapping d ON k.key = d."queryHash"
    GROUP BY d."clientID"
    ORDER BY total_rows DESC;`;
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
      sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."desires" WHERE "inactivedAt" IS NULL`,
    ],
    [
      'num fresh queries',
      sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."desires"
          WHERE "inactivatedAt" < NOW() AND "expiresAt" > NOW()`,
    ],
    [
      'num deleted queries',
      sql`SELECT COUNT(*) as "c" FROM ${sql(schema)}."desires" WHERE "deleted" = true`,
    ],
    [
      'total num queries per client and client group',
      numQueriesPerClientAndClientGroup(false),
    ],
    [
      'num fresh queries per client and client group',
      numQueriesPerClientAndClientGroup(true),
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
    ['total rows per client', rowsPerClientID(false)],
    ['total active rows per client', rowsPerClientID(true)],
    [
      'rows per client group',
      sql`SELECT 
      r."clientGroupID",
      COUNT(*) AS total_rows
    FROM ${sql(schema)}."rows" r
    GROUP BY r."clientGroupID"
    ORDER BY total_rows DESC;`,
    ],
  ]);

  const queryAsts =
    await sql`SELECT "queryHash", "clientAST" FROM ${sql(schema)}."queries"`;

  const seenQueries = new Set<string>();
  const parseFailures: string[] = [];
  for (const row of queryAsts) {
    const {queryHash, clientAST} = row.queryHash;
    if (seenQueries.has(queryHash)) {
      continue;
    }
    seenQueries.add(queryHash);

    try {
      const ast = JSON.parse(clientAST) as AST;
      const zql = ast.table + astToZQL(ast);
      console.log('HASH:', queryHash, 'ZQL:', zql);
    } catch (e) {
      parseFailures.push(queryHash);
    }
  }
  if (parseFailures.length > 0) {
    console.log('Failed to parse the following hashes:', parseFailures);
  }
}

async function changelogStats() {
  const schema = upstreamSchema(getShardID(config)) + '/cdc';
  const sql = pgClient(lc, config.cdc.db);

  await printStats([
    [
      'change log size',
      sql`SELECT COUNT(*) as "change_log_size" FROM ${sql(schema)}."change_log"`,
    ],
  ]);
}

async function printStats(
  pendingQueries: [
    name: string,
    query: ReturnType<ReturnType<typeof pgClient>>,
  ][],
) {
  const results = await Promise.all(pendingQueries);
  for (const result of results) {
    console.log(result[0]);
  }
}

await changelogStats();
await upstreamStats();
await cvrStats();
