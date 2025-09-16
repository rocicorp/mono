/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {basename, dirname, join, relative, resolve, sep} from 'node:path';
import {existsSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {tsImport} from 'tsx/esm/api';
import {logOptions} from '../../../otel/src/log-options.ts';
import * as v from '../../../shared/src/valita.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  permissionsConfigSchema,
  type PermissionsConfig,
} from '../../../zero-schema/src/compiled-permissions.ts';
import {isSchemaConfig} from '../../../zero-schema/src/schema-config.ts';
import {appOptions, shardOptions, zeroOptions} from '../config/zero-config.ts';
import {colorConsole} from '../../../shared/src/logging.ts';

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

  log: logOptions,

  output: {
    file: {
      type: v.string().optional(),
      desc: [
        `Outputs the permissions to a file with the requested {bold output-format}.`,
      ],
    },

    format: {
      type: v.literalUnion('sql', 'json', 'pretty').default('sql'),
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

export async function loadSchemaAndPermissions(
  schemaPath: string,
  allowMissing: true,
): Promise<{schema: Schema; permissions: PermissionsConfig} | undefined>;
export async function loadSchemaAndPermissions(
  schemaPath: string,
  allowMissing?: false,
): Promise<{schema: Schema; permissions: PermissionsConfig}>;
export async function loadSchemaAndPermissions(
  schemaPath: string,
  allowMissing: boolean | undefined,
): Promise<{schema: Schema; permissions: PermissionsConfig} | undefined> {
  const typeModuleErrorMessage = () =>
    `\n\nYou may need to add \` "type": "module" \` to the package.json file for ${schemaPath}.\n`;

  colorConsole.info(`Loading permissions from ${schemaPath}`);
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

  if (!existsSync(absoluteSchemaPath)) {
    if (allowMissing) {
      return undefined;
    }
    colorConsole.error(`Schema file ${schemaPath} does not exist.`);
    process.exit(1);
  }

  let module;
  try {
    module = await tsImport(relativePath, import.meta.url);
  } catch (e) {
    colorConsole.error(
      `Failed to load zero schema from ${absoluteSchemaPath}` +
        typeModuleErrorMessage(),
    );
    throw e;
  }

  if (!isSchemaConfig(module)) {
    colorConsole.error(
      `Schema file ${schemaPath} must export [schema] and [permissions].` +
        typeModuleErrorMessage(),
    );
    process.exit(1);
  }
  try {
    const schemaConfig = module;
    const perms =
      await (schemaConfig.permissions as unknown as Promise<unknown>);
    const {schema} = schemaConfig;
    return {
      schema,
      permissions: v.parse(perms, permissionsConfigSchema),
    };
  } catch (e) {
    colorConsole.error(`Failed to parse Permissions object`);
    throw e;
  }
}
