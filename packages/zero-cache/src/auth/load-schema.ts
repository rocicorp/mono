import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {
  mapSchemaToServer,
  type Schema,
} from '../../../zero-schema/src/builder/schema-builder.ts';
import {type PermissionsConfig} from '../../../zero-schema/src/compiled-permissions.ts';
import {parseSchema} from '../../../zero-schema/src/schema-config.ts';
import type {ZeroConfig} from '../config/zero-config.ts';

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
  })().then(({schema, permissions}) => ({
    // The schema includes serverName fields but is structured with client
    // names. Remap it into the server namespace.
    schema: mapSchemaToServer(schema),
    // Permissions are already compiled with server names
    permissions,
  }));

  return loadedSchema;
}
