import type {LogContext} from '@rocicorp/logger';
import {basename, dirname, join, relative, resolve, sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {tsImport} from 'tsx/esm/api';
import * as v from '../../../shared/src/valita.ts';
import {
  permissionsConfigSchema,
  type PermissionsConfig,
} from '../../../zero-schema/src/compiled-permissions.ts';
import {isSchemaConfig} from '../../../zero-schema/src/schema-config.ts';
import {appOptions, shardOptions, zeroOptions} from '../config/zero-config.ts';

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
      type: v.string().optional(),
      desc: [
        `The upstream Postgres database to deploy permissions to.`,
        `This is ignored if an {bold output-file} is specified.`,
      ],
    },

    type: zeroOptions.upstream.type,
  },

  app: {id: appOptions.id},

  shard: shardOptions,

  output: {
    file: {
      type: v.string().optional(),
      desc: [
        `Outputs the permissions to a file with the requested {bold output-format}.`,
      ],
    },

    format: {
      type: v
        .union(v.literal('sql'), v.literal('json'), v.literal('pretty'))
        .default('sql'),
      desc: [
        `The desired format of the output file.`,
        ``,
        `A {bold sql} file can be executed via "psql -f <file.sql>", or "\\\\i <file.sql>"`,
        `from within the psql console, or copied and pasted into a migration script.`,
        ``,
        `The {bold json} and {bold pretty} formats are available for non-pg backends`,
        `and general debugging.`,
      ],
    },
  },

  force: {
    type: v.boolean().default(false),
    desc: [`Deploy to upstream without validation. Use at your own risk.`],
    alias: 'f',
  },
};

export async function loadPermissions(
  lc: LogContext,
  schemaPath: string,
): Promise<PermissionsConfig> {
  const typeModuleErrorMessage = () =>
    `\n\nYou may need to add \` "type": "module" \` to the package.json file for ${schemaPath}.\n`;

  lc.info?.(`Loading permissions from ${schemaPath}`);
  const dir = dirname(fileURLToPath(import.meta.url));
  const absoluteSchemaPath = resolve(schemaPath);
  const relativeDir = relative(dir, dirname(absoluteSchemaPath));
  let relativePath = join(
    // tsImport expects the relativePath to be a path and not just a filename.
    relativeDir.length ? relativeDir : `.${sep}`,
    basename(absoluteSchemaPath),
  );

  // tsImport doesn't expect to receive slashes in the Windows format when running
  // on Windows. They need to be converted to *nix format.
  relativePath = relativePath.replace(/\\/g, '/');

  let module;
  try {
    module = await tsImport(relativePath, import.meta.url);
  } catch (e) {
    lc.error?.(
      `Failed to load zero schema from ${absoluteSchemaPath}` +
        typeModuleErrorMessage(),
    );
    throw e;
  }

  if (!isSchemaConfig(module)) {
    lc.error?.(
      `Schema file ${schemaPath} must export [schema] and [permissions].` +
        typeModuleErrorMessage(),
    );
    process.exit(1);
  }
  try {
    const schemaConfig = module;
    const perms =
      await (schemaConfig.permissions as unknown as Promise<unknown>);
    return v.parse(perms, permissionsConfigSchema);
  } catch (e) {
    lc.error?.(`Failed to parse Permissions object`);
    throw e;
  }
}
