import path from 'node:path';
import {
  permissionsConfigSchema,
  type PermissionsConfig,
} from '../../../zero-schema/src/compiled-permissions.js';
import type {Schema} from '../../../zero-schema/src/schema.js';
import {readFile} from 'node:fs/promises';
import * as v from '../../../shared/src/valita.js';
import type {ZeroConfig} from '../config/zero-config.js';

let loadedSchema:
  | Promise<{
      schema: Schema;
      permissions: PermissionsConfig;
    }>
  | undefined;

function parseAuthConfig(
  input: string,
  source: string,
): {
  schema: Schema;
  permissions: PermissionsConfig;
} {
  try {
    const config = JSON.parse(input);
    return {
      permissions: v.parse(
        config.permissions,
        permissionsConfigSchema,
        'strict',
      ),
      schema: config.schema as Schema,
    };
  } catch (e) {
    throw new Error(`Failed to parse schema config from ${source}: ${e}`);
  }
}

export function getSchema(config: ZeroConfig): Promise<{
  schema: Schema;
  permissions: PermissionsConfig;
}> {
  if (loadedSchema) {
    return loadedSchema;
  }

  loadedSchema = (async () => {
    if (config.schema.json) {
      return parseAuthConfig(config.schema.json, 'config.schema.json');
    }
    console.log?.('Loading schema from file: ', config.schema.file);

    const fileContent = await readFile(
      path.resolve(config.schema.file),
      'utf-8',
    );
    return parseAuthConfig(fileContent, config.schema.file);
  })();

  return loadedSchema;
}
