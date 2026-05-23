import {existsSync} from 'node:fs';
import {basename, dirname, join, relative, resolve, sep} from 'node:path';
import process from 'node:process';
import {fileURLToPath} from 'node:url';
import {colorConsole} from 'shared/src/logging.ts';
import * as v from 'shared/src/valita.ts';
import {
  permissionsConfigSchema,
  type PermissionsConfig,
} from 'zero-schema/src/compiled-permissions.ts';
import {isSchemaConfig} from 'zero-schema/src/schema-config.ts';
import type {Schema} from 'zero-types/src/schema.ts';

export type {PermissionsConfig};

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

  colorConsole.info(`Loading schema from ${schemaPath}`);

  const dir = dirname(fileURLToPath(import.meta.url));
  const absoluteSchemaPath = resolve(schemaPath);
  const relativeDir = relative(dir, dirname(absoluteSchemaPath));
  let relativePath =
    relativeDir.length && relativeDir !== '.'
      ? join(relativeDir, basename(absoluteSchemaPath))
      : `.${sep}${basename(absoluteSchemaPath)}`;

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
    module = await import(relativePath);
  } catch (e) {
    colorConsole.error(
      `Failed to load zero schema from ${absoluteSchemaPath}` +
        typeModuleErrorMessage(),
    );
    throw e;
  }

  if (!isSchemaConfig(module)) {
    colorConsole.error(
      `Schema file ${schemaPath} must export [schema].` +
        typeModuleErrorMessage(),
    );
    process.exit(1);
  }
  try {
    const schemaConfig = module;
    const perms =
      await (schemaConfig.permissions as unknown as Promise<unknown>);
    const {schema} = schemaConfig;

    if (perms) {
      colorConsole.warn?.(
        'Permissions are deprecated and will be removed in an upcoming release. See: https://zero.rocicorp.dev/docs/auth.',
      );
    }

    return {
      schema,
      permissions: v.parse(perms ?? {}, permissionsConfigSchema),
    };
  } catch (e) {
    colorConsole.error(`Failed to parse Permissions object`);
    throw e;
  }
}
