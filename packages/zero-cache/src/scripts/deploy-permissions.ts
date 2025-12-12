import '../../../shared/src/dotenv.ts';

import {writeFile} from 'node:fs/promises';
import {ident as id, literal} from 'pg-format';
import {colorConsole, createLogContext} from '../../../shared/src/logging.ts';
import {parseOptions} from '../../../shared/src/options.ts';
import {difference} from '../../../shared/src/set-utils.ts';
import {mapCondition} from '../../../zero-protocol/src/ast.ts';
import {
  type AssetPermissions,
  type PermissionsConfig,
  type Rule,
} from '../../../zero-schema/src/compiled-permissions.ts';
import {validator} from '../../../zero-schema/src/name-mapper.ts';
import {ZERO_ENV_VAR_PREFIX} from '../config/zero-config.ts';
import {getPublicationInfo} from '../services/change-source/pg/schema/published.ts';
import {
  ensureGlobalTables,
  SHARD_CONFIG_TABLE,
} from '../services/change-source/pg/schema/shard.ts';
import {liteTableName} from '../types/names.ts';
import {pgClient, type PostgresDB} from '../types/pg.ts';
import {appSchema, getShardID, upstreamSchema} from '../types/shards.ts';
import {
  deployPermissionsOptions,
  loadSchemaAndPermissions,
} from './permissions.ts';

const config = parseOptions(deployPermissionsOptions, {
  argv: process.argv.slice(2),
  envNamePrefix: ZERO_ENV_VAR_PREFIX,
});

const shard = getShardID(config);
const app = appSchema(shard);

const lc = createLogContext(config);

async function validatePermissions(
  db: PostgresDB,
  permissions: PermissionsConfig,
) {
  const schema = upstreamSchema(shard);

  // Check if the shardConfig table has been initialized.
  const result = await db`
    SELECT relname FROM pg_class
      JOIN pg_namespace ON relnamespace = pg_namespace.oid
      WHERE nspname = ${schema} AND relname = ${SHARD_CONFIG_TABLE}`;
  if (result.length === 0) {
    colorConsole.warn(
      `zero-cache has not yet initialized the upstream database.\n` +
        `Deploying ${app} permissions without validating against published tables/columns.`,
    );
    return;
  }

  // Get the publications for the shard
  const config = await db<{publications: string[]}[]>`
    SELECT publications FROM ${db(schema + '.' + SHARD_CONFIG_TABLE)}
  `;
  if (config.length === 0) {
    colorConsole.warn(
      `zero-cache has not yet initialized the upstream database.\n` +
        `Deploying ${app} permissions without validating against published tables/columns.`,
    );
    return;
  }
  colorConsole.info(
    `Validating permissions against tables and columns published for "${app}".`,
  );

  const [{publications: shardPublications}] = config;
  const {tables, publications} = await getPublicationInfo(
    db,
    shardPublications,
  );
  const pubnames = publications.map(p => p.pubname);
  const missing = difference(new Set(shardPublications), new Set(pubnames));
  if (missing.size) {
    colorConsole.warn(
      `Upstream is missing expected publications "${[...missing]}".\n` +
        `You may need to re-initialize your replica.\n` +
        `Deploying ${app} permissions without validating against published tables/columns.`,
    );
    return;
  }
  const tablesToColumns = new Map(
    tables.map(t => [liteTableName(t), Object.keys(t.columns)]),
  );
  const validate = validator(tablesToColumns);
  try {
    for (const [table, perms] of Object.entries(permissions?.tables ?? {})) {
      const validateRule = ([_, cond]: Rule) => {
        mapCondition(cond, table, validate);
      };
      const validateAsset = (asset: AssetPermissions | undefined) => {
        asset?.select?.forEach(validateRule);
        asset?.delete?.forEach(validateRule);
        asset?.insert?.forEach(validateRule);
        asset?.update?.preMutation?.forEach(validateRule);
        asset?.update?.postMutation?.forEach(validateRule);
      };
      validateAsset(perms.row);
      if (perms.cell) {
        Object.values(perms.cell).forEach(validateAsset);
      }
    }
  } catch (e) {
    failWithMessage(String(e));
  }
}

function failWithMessage(msg: string) {
  colorConsole.error(msg);
  colorConsole.info('\nUse --force to deploy at your own risk.\n');
  process.exit(-1);
}

async function deployPermissions(
  upstreamURI: string,
  permissions: PermissionsConfig,
  force: boolean,
) {
  const db = pgClient(lc, upstreamURI);
  const {host, port} = db.options;
  colorConsole.debug(`Connecting to upstream@${host}:${port}`);
  try {
    await ensureGlobalTables(db, shard);

    const {hash, changed} = await db.begin(async tx => {
      if (force) {
        colorConsole.warn(`--force specified. Skipping validation.`);
      } else {
        await validatePermissions(tx, permissions);
      }

      const {appID} = shard;
      colorConsole.info(
        `Deploying permissions for --app-id "${appID}" to upstream@${db.options.host}`,
      );
      const [{hash: beforeHash}] = await tx<{hash: string}[]>`
        SELECT hash from ${tx(app)}.permissions`;
      const [{hash}] = await tx<{hash: string}[]>`
        UPDATE ${tx(app)}.permissions SET ${db({permissions})} RETURNING hash`;

      return {hash: hash.substring(0, 7), changed: beforeHash !== hash};
    });
    if (changed) {
      colorConsole.info(`Deployed new permissions (hash=${hash})`);
    } else {
      colorConsole.info(`Permissions unchanged (hash=${hash})`);
    }
  } finally {
    await db.end();
  }
}

async function writePermissionsFile(
  perms: PermissionsConfig,
  file: string,
  format: 'sql' | 'json' | 'pretty',
) {
  const contents =
    format === 'sql'
      ? `UPDATE ${id(app)}.permissions SET permissions = ${literal(
          JSON.stringify(perms),
        )};`
      : JSON.stringify(perms, null, format === 'pretty' ? 2 : 0);
  await writeFile(file, contents);
  colorConsole.info(`Wrote ${format} permissions to ${config.output.file}`);
}

const ret = await loadSchemaAndPermissions(config.schema.path, true);
if (!ret || Object.keys(ret?.permissions ?? {}).length === 0) {
  colorConsole.warn(
    `No permissions found at ${config.schema.path}, so could not deploy ` +
      `permissions. Replicating data, but no tables will be syncable. ` +
      `Create a schema file with permissions to be able to sync data.`,
  );
} else {
  colorConsole.warn(
    `Permissions are deprecated and will be removed in an upcoming release. See: https://zero.rocicorp.dev/docs/auth.`,
  );

  const {permissions} = ret;
  if (config.output.file) {
    await writePermissionsFile(
      permissions,
      config.output.file,
      config.output.format,
    );
  } else if (config.upstream.type !== 'pg') {
    colorConsole.warn(
      `Permissions deployment is not supported for ${config.upstream.type} upstreams`,
    );
    process.exit(-1);
  } else if (config.upstream.db) {
    await deployPermissions(config.upstream.db, permissions, config.force);
  } else {
    colorConsole.error(`No --output-file or --upstream-db specified`);
    // Shows the usage text.
    parseOptions(deployPermissionsOptions, {
      argv: ['--help'],
      envNamePrefix: ZERO_ENV_VAR_PREFIX,
    });
  }
}
