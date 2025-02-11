import {consoleLogSink, LogContext} from '@rocicorp/logger';
import 'dotenv/config';
import {writeFile} from 'node:fs/promises';
import {basename, dirname, join, relative, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {literal} from 'pg-format';
import {tsImport} from 'tsx/esm/api';
import {parseOptions} from '../../../shared/src/options.ts';
import * as v from '../../../shared/src/valita.ts';
import {
  permissionsConfigSchema,
  type PermissionsConfig,
} from '../../../zero-schema/src/compiled-permissions.ts';
import {isSchemaConfig} from '../../../zero-schema/src/schema-config.ts';
import {ZERO_ENV_VAR_PREFIX, zeroOptions} from '../config/zero-config.ts';
import {ensureGlobalTables} from '../services/change-source/pg/schema/shard.ts';
import {pgClient, type PostgresDB} from '../types/pg.ts';

export const deployPermissionsOptions = {
  schema: {
    path: {
      type: v.string().default('schema.ts'),
      desc: [
        'Relative path to the file containing the schema definition.',
        'The file must have a default export of type SchemaConfig.',
      ],
      alias: 'p',
    },
  },

  upstream: {
    db: {
      ...zeroOptions.upstream.db,
      type: v.string().optional(),
      desc: [
        `The upstream Postgres database to deploy permissions to.`,
        `This is ignored if an {bold output-file} is specified.`,
      ],
    },
  },

  output: {
    file: {
      type: v.string().optional(),
      desc: [
        `Outputs the permissions to a file with the requested {bold output-format}.`,
      ],
    },

    format: {
      type: v.union(v.literal('sql'), v.literal('json')).default('sql'),
      desc: [
        `The desired format of the output file.`,
        ``,
        `A {bold sql} file can be executed via "psql -f <file.sql>", or "\\\\i <file.sql>"`,
        `from within the psql console, or copied and pasted into a migration script.`,
        ``,
        `The {bold json} format is available for general debugging.`,
      ],
    },
  },
};

const config = parseOptions(
  deployPermissionsOptions,
  process.argv.slice(2),
  ZERO_ENV_VAR_PREFIX,
);

export async function loadPermissions(
  lc: LogContext,
  schema: typeof config.schema,
): Promise<PermissionsConfig> {
  lc.info?.(`Loading permissions from ${schema.path}`);
  const dir = dirname(fileURLToPath(import.meta.url));
  const absoluteSchemaPath = resolve(config.schema.path);
  let relativePath = join(
    relative(dir, dirname(absoluteSchemaPath)),
    basename(absoluteSchemaPath),
  );

  // tsImport doesn't expect to receive slashes in the Windows format when running
  // on Windows. They need to be converted to *nix format.
  relativePath = relativePath.replace(/\\/g, '/');

  let module;
  try {
    module = await tsImport(relativePath, import.meta.url);
  } catch (e) {
    lc.error?.(`Failed to load zero schema from ${absoluteSchemaPath}:`, e);
    process.exit(1);
  }

  if (!isSchemaConfig(module)) {
    throw new Error(
      `Schema file ${schema.path} must export [schema] and [permissions].`,
    );
  }
  try {
    const schemaConfig = module;
    const perms =
      await (schemaConfig.permissions as unknown as Promise<unknown>);
    return v.parse(perms, permissionsConfigSchema);
  } catch (e) {
    lc.error?.(`Failed to parse Permissions object`, e);
    process.exit(1);
  }
}

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

const permissions = await loadPermissions(lc, config.schema);
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
