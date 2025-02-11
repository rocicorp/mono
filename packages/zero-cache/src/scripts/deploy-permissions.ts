import {consoleLogSink, LogContext} from '@rocicorp/logger';
import 'dotenv/config';
import {writeFile} from 'node:fs/promises';
import {literal} from 'pg-format';
import {parseOptions} from '../../../shared/src/options.ts';
import {type PermissionsConfig} from '../../../zero-schema/src/compiled-permissions.ts';
import {ZERO_ENV_VAR_PREFIX} from '../config/zero-config.ts';
import {ensureGlobalTables} from '../services/change-source/pg/schema/shard.ts';
import {pgClient, type PostgresDB} from '../types/pg.ts';
import {deployPermissionsOptions, loadPermissions} from './permissions.ts';

const config = parseOptions(
  deployPermissionsOptions,
  process.argv.slice(2),
  ZERO_ENV_VAR_PREFIX,
);

async function validatePermissions(
  _lc: LogContext,
  _db: PostgresDB,
  _permissions: PermissionsConfig,
) {
  // TODO: Validate that the permissions rules match the upstream table / column names.
}

async function deployPermissions(
  lc: LogContext,
  upstreamURI: string,
  permissions: PermissionsConfig,
) {
  const db = pgClient(lc, upstreamURI);
  try {
    await validatePermissions(lc, db, permissions);
    await ensureGlobalTables(db);

    lc.info?.(`Deploying permissions to upstream@${db.options.host}`);

    const {hash, changed} = await db.begin(async tx => {
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
  lc: LogContext,
  perms: PermissionsConfig,
  file: string,
  format: 'sql' | 'json',
) {
  const contents =
    format === 'sql'
      ? `UPDATE zero.permissions SET permissions = ${literal(
          JSON.stringify(perms),
        )};`
      : JSON.stringify(perms, null, 2);
  await writeFile(file, contents);
  lc.info?.(`Wrote permissions ${format} to ${config.output.file}`);
}

const lc = new LogContext('debug', {}, consoleLogSink);

const permissions = await loadPermissions(lc, config.schema.path);
if (config.output.file) {
  await writePermissionsFile(
    lc,
    permissions,
    config.output.file,
    config.output.format,
  );
} else if (config.upstream.db) {
  await deployPermissions(lc, config.upstream.db, permissions);
} else {
  lc.error?.(`No --output-file or --upstream-db specified`);
  // Shows the usage text.
  parseOptions(deployPermissionsOptions, ['--help'], ZERO_ENV_VAR_PREFIX);
}
