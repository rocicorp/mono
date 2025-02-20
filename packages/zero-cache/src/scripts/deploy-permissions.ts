import {consoleLogSink, LogContext} from '@rocicorp/logger';
import 'dotenv/config';
import {writeFile} from 'node:fs/promises';
import {literal} from 'pg-format';
import {parseOptions} from '../../../shared/src/options.ts';
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
  APP_PUBLICATION_PREFIX,
  ensureGlobalTables,
  INTERNAL_PUBLICATION_PREFIX,
} from '../services/change-source/pg/schema/shard.ts';
import {liteTableName} from '../types/names.ts';
import {pgClient, type PostgresDB} from '../types/pg.ts';
import {deployPermissionsOptions, loadPermissions} from './permissions.ts';

export async function deploy() {
  const lc = new LogContext('debug', {}, consoleLogSink);

  async function validatePermissions(
    db: PostgresDB,
    permissions: PermissionsConfig,
  ) {
    const pubnames = await db.unsafe<{pubname: string}[]>(`
  SELECT pubname FROM pg_publication 
    WHERE pubname LIKE '${APP_PUBLICATION_PREFIX}%'
       OR pubname LIKE '${INTERNAL_PUBLICATION_PREFIX}%'`);
    if (pubnames.length === 0) {
      // If zero-cache has not yet initialized the upstream publications,
      // we can't validate the permissions against what's been published.
      // This can happen while bootstrapping / initializing a new stack.
      // In this case we deploy permissions without validate.
      lc.info?.(
        `zero-cache has not yet initialized the upstream database.\n` +
          `Deploying permissions without validating against published tables/columns.`,
      );
      return;
    }

    lc.info?.(
      'Validating permissions against upstream table and column names.',
    );

    const {tables} = await getPublicationInfo(
      db,
      pubnames.map(p => p.pubname),
    );
    const tablesToColumns = new Map(
      tables.map(t => [liteTableName(t), Object.keys(t.columns)]),
    );
    const validate = validator(tablesToColumns);
    try {
      for (const [table, perms] of Object.entries(permissions.tables)) {
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
    lc.error?.(msg);
    lc.info?.('\nUse --force to deploy at your own risk.\n');
    process.exit(-1);
  }

  async function deployPermissions(
    upstreamURI: string,
    permissions: PermissionsConfig,
    force: boolean,
  ) {
    const db = pgClient(lc, upstreamURI);
    try {
      await ensureGlobalTables(db);

      const {hash, changed} = await db.begin(async tx => {
        if (force) {
          lc.warn?.(`--force specified. Skipping validation.`);
        } else {
          await validatePermissions(tx, permissions);
        }

        lc.info?.(`Deploying permissions to upstream@${db.options.host}`);
        const [{hash: beforeHash}] = await tx<{hash: string}[]>`
        SELECT hash from zero.permissions`;
        const [{hash}] = await tx<{hash: string}[]>`
        UPDATE zero.permissions SET ${db({permissions})} RETURNING hash`;

        return {hash: hash.substring(0, 7), changed: beforeHash !== hash};
      });
      if (changed) {
        lc.info?.(`Deployed new permissions (hash=${hash})`);
      } else {
        lc.info?.(`Permissions unchanged (hash=${hash})`);
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
        ? `UPDATE zero.permissions SET permissions = ${literal(
            JSON.stringify(perms),
          )};`
        : JSON.stringify(perms, null, format === 'pretty' ? 2 : 0);
    await writeFile(file, contents);
    lc.info?.(`Wrote ${format} permissions to ${file}`);
  }

  const config = parseOptions(
    deployPermissionsOptions,
    process.argv.slice(2),
    ZERO_ENV_VAR_PREFIX,
  );

  const permissions = await loadPermissions(lc, config.schema.path);
  if (config.output.file) {
    await writePermissionsFile(
      permissions,
      config.output.file,
      config.output.format,
    );
  } else if (config.upstream.type !== 'pg') {
    lc.warn?.(
      `Permissions deployment is not supported for ${config.upstream.type} upstreams`,
    );
    process.exit(-1);
  } else if (config.upstream.db) {
    await deployPermissions(config.upstream.db, permissions, config.force);
  } else {
    lc.error?.(`No --output-file or --upstream-db specified`);
    // Shows the usage text.
    parseOptions(deployPermissionsOptions, ['--help'], ZERO_ENV_VAR_PREFIX);
  }
}
