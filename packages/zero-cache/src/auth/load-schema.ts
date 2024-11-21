import path from 'node:path';
import {
  authorizationConfigSchema,
  type AuthorizationConfig,
} from '../../../zero-schema/src/compiled-authorization.js';
import type {Schema} from '../../../zero-schema/src/schema.js';
import {readFile} from 'node:fs/promises';
import * as v from '../../../shared/src/valita.js';

const ENV_VAR_PREFIX = 'ZERO_SCHEMA_';

let loadedConfig:
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
    throw new Error(
      `Failed to parse authorization config from ${source}: ${e}`,
    );
  }
}

export async function getSchema(): Promise<{
  schema: Schema;
  authorization: AuthorizationConfig;
}> {
  if (loadedConfig) {
    return loadedConfig;
  }

  const jsonConfig = process.env[`${ENV_VAR_PREFIX}JSON`];
  const jsonConfigPath =
    process.env[`${ENV_VAR_PREFIX}JSON_PATH`] || './zero-schema.json';

  if (!jsonConfig && !path.isAbsolute(jsonConfigPath)) {
    const fileContent = await readFile(path.resolve(jsonConfigPath), 'utf-8');
    return parseAuthConfig(fileContent, jsonConfigPath);
  }

  loadedConfig = (async () => {
    if (jsonConfig) {
      return parseAuthConfig(jsonConfig, `${ENV_VAR_PREFIX}JSON`);
    }
    const fileContent = await readFile(path.resolve(jsonConfigPath), 'utf-8');
    return parseAuthConfig(fileContent, jsonConfigPath);
  })();

  return loadedConfig;
}
