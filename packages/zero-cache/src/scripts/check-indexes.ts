import '../../../shared/src/dotenv.ts';

import {colorConsole, createLogContext} from '../../../shared/src/logging.ts';
import {parseOptions} from '../../../shared/src/options.ts';
import * as v from '../../../shared/src/valita.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {ZERO_ENV_VAR_PREFIX} from '../config/zero-config.ts';
import {
  findMissingRelationshipIndexes,
  type CheckIndexesResult,
} from '../db/relationship-indexes.ts';
import {getPublicationInfo} from '../services/change-source/pg/schema/published.ts';
import {SHARD_CONFIG_TABLE} from '../services/change-source/pg/schema/shard.ts';
import {liteTableName} from '../types/names.ts';
import {pgClient, type PostgresDB} from '../types/pg.ts';
import {getShardID, upstreamSchema} from '../types/shards.ts';
import {
  deployPermissionsOptions,
  loadSchemaAndPermissions,
} from './permissions.ts';

// Reuse the relevant subset of the deploy-permissions flags so the command
// shares the same schema/upstream/app/shard configuration that developers
// already use, without the output-file/force flags that don't apply here.
const checkIndexesOptions = {
  schema: deployPermissionsOptions.schema,
  upstream: {
    db: {
      type: v.string().optional(),
      desc: [
        `The upstream Postgres database to check relationship indexes against.`,
      ],
    },
    type: deployPermissionsOptions.upstream.type,
  },
  app: deployPermissionsOptions.app,
  shard: deployPermissionsOptions.shard,
  log: deployPermissionsOptions.log,
};

const config = parseOptions(checkIndexesOptions, {
  argv: process.argv.slice(2),
  envNamePrefix: ZERO_ENV_VAR_PREFIX,
});

const shard = getShardID(config);
const lc = createLogContext(config);

async function getPublishedTablesAndIndexes(db: PostgresDB) {
  const schema = upstreamSchema(shard);

  // The published tables and indexes are only available once zero-cache has
  // initialized the upstream database (creating the shard config table and
  // the publications it references).
  const initialized = await db`
    SELECT relname FROM pg_class
      JOIN pg_namespace ON relnamespace = pg_namespace.oid
      WHERE nspname = ${schema} AND relname = ${SHARD_CONFIG_TABLE}`;
  if (initialized.length === 0) {
    return undefined;
  }
  const rows = await db<{publications: string[]}[]>`
    SELECT publications FROM ${db(schema + '.' + SHARD_CONFIG_TABLE)}`;
  if (rows.length === 0) {
    return undefined;
  }
  const [{publications}] = rows;
  return getPublicationInfo(db, publications);
}

function report(result: CheckIndexesResult) {
  const {missing, unsyncedTables} = result;
  if (unsyncedTables.length > 0) {
    colorConsole.warn(
      `Skipped index checks for tables that are referenced by relationships ` +
        `but are not published/synced: ${unsyncedTables.join(', ')}.`,
    );
  }
  if (missing.length === 0) {
    colorConsole.info(
      `✓ All relationship join fields used by related()/whereExists() are backed by an index.`,
    );
    return;
  }

  const lines = [
    `Found ${missing.length} relationship join field${
      missing.length === 1 ? '' : 's'
    } without a backing index.`,
    ``,
    `Zero maintains related()/whereExists() queries incrementally and reacts to`,
    `changes from either table, so an index is needed on BOTH sides of a`,
    `relationship. A missing index forces a full table scan on every change.`,
    ``,
  ];
  for (const m of missing) {
    const hopNote =
      m.hopCount > 1 ? ` (junction hop ${m.hop}/${m.hopCount})` : '';
    lines.push(
      `  • "${m.ownerTable}"."${m.relationship}"${hopNote}: ` +
        `${m.side} side has no index on ${m.serverTable}(${m.serverColumns.join(
          ', ',
        )})`,
    );
  }
  lines.push(``, `Add the following indexes (deduplicated):`);
  for (const sql of new Set(missing.map(m => m.createIndexSQL))) {
    lines.push(`  ${sql}`);
  }
  colorConsole.warn(lines.join('\n'));
}

async function checkIndexes(schema: Schema, upstreamURI: string) {
  const db = pgClient(lc, upstreamURI, 'check-indexes');
  try {
    const published = await getPublishedTablesAndIndexes(db);
    if (published === undefined) {
      colorConsole.warn(
        `zero-cache has not yet initialized the upstream database, so the ` +
          `published tables and indexes can't be read.\n` +
          `Start zero-cache once (it creates the replica and publications), ` +
          `then re-run this command.`,
      );
      return;
    }
    report(
      findMissingRelationshipIndexes(
        schema,
        published.tables.map(t => ({
          table: liteTableName({schema: t.schema, name: t.name}),
          primaryKey: t.primaryKey ?? [],
        })),
        published.indexes.map(i => ({
          table: liteTableName({schema: i.schema, name: i.tableName}),
          columns: Object.keys(i.columns),
        })),
      ),
    );
  } finally {
    await db.end();
  }
}

const ret = await loadSchemaAndPermissions(config.schema.path);

if (config.upstream.type !== 'pg') {
  colorConsole.warn(
    `Index checking is only supported for pg upstreams (got "${config.upstream.type}").`,
  );
} else if (config.upstream.db) {
  await checkIndexes(ret.schema, config.upstream.db);
} else {
  colorConsole.error(`No --upstream-db specified.`);
  // Shows the usage text.
  parseOptions(checkIndexesOptions, {
    argv: ['--help'],
    envNamePrefix: ZERO_ENV_VAR_PREFIX,
  });
}
