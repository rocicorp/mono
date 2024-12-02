import path from 'node:path';
import {type PermissionsConfig} from '../../../zero-schema/src/compiled-permissions.js';
import type {Schema} from '../../../zero-schema/src/schema.js';
import {readFile} from 'node:fs/promises';
import type {ZeroConfig} from '../config/zero-config.js';
import {parseSchema} from '../../../zero-schema/src/schema-config.js';

let loadedSchema:
  | Promise<{
      schema: Schema;
      permissions: PermissionsConfig;
    }>
  | undefined;

export function getSchema(config: ZeroConfig): Promise<{
  schema: Schema;
  permissions: PermissionsConfig;
}> {
  if (loadedSchema) {
    return loadedSchema;
  }

  loadedSchema = (async () => {
    if (config.schema.json) {
      return parseSchema(config.schema.json, 'config.schema.json');
    }
    const fileContent = await readFile(
      path.resolve(config.schema.file),
      'utf-8',
    );
    return parseSchema(fileContent, config.schema.file);
  })();

  return loadedSchema;
}
