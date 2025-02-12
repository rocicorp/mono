import type {LogContext} from '@rocicorp/logger';
import {basename, dirname, join, relative, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {tsImport} from 'tsx/esm/api';
import * as v from '../../../shared/src/valita.ts';
import {
  permissionsConfigSchema,
  type PermissionsConfig,
} from '../../../zero-schema/src/compiled-permissions.ts';
import {isSchemaConfig} from '../../../zero-schema/src/schema-config.ts';
import {zeroOptions} from '../config/zero-config.ts';

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

  force: {
    type: v.boolean().default(false),
    desc: [
      `Deploy to upstream even if validation fails. Use at your own risk.`,
    ],
    alias: 'f',
  },
};

export async function loadPermissions(
  lc: LogContext,
  schemaPath: string,
): Promise<PermissionsConfig> {
  lc.info?.(`Loading permissions from ${schemaPath}`);
  const dir = dirname(fileURLToPath(import.meta.url));
  const absoluteSchemaPath = resolve(schemaPath);
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
      `Schema file ${schemaPath} must export [schema] and [permissions].`,
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
