import path from 'node:path';
import {
  authorizationConfigSchema,
  type AuthorizationConfig,
} from '../../../zero-schema/src/compiled-authorization.js';
import type {Schema} from '../../../zero-schema/src/schema.js';
import {readFile} from 'node:fs/promises';
import * as v from '../../../shared/src/valita.js';
import type {ZeroConfig} from '../config/zero-config.js';

let loadedSchema:
  | Promise<{
      schema: Schema;
      authorization: AuthorizationConfig;
    }>
  | undefined;

function parseAuthConfig(
  input: string,
  source: string,
): {
  schema: Schema;
  authorization: AuthorizationConfig;
} {
  try {
    const config = JSON.parse(input);
    return {
      authorization: v.parse(
        config.authorization,
        authorizationConfigSchema,
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
  authorization: AuthorizationConfig;
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
