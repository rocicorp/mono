import path from 'node:path';
import type {AuthorizationConfig} from '../../../zero-schema/src/compiled-authorization.js';
import {fileURLToPath} from 'node:url';
import {tsImport} from 'tsx/esm/api';
import type {ZeroConfig} from '../config/zero-config.js';
import type {Schema} from '../../../zero-schema/src/schema.js';
import {readFile} from 'node:fs/promises';

let loadedConfig:
  | Promise<{
      schema: Schema;
      authorization: AuthorizationConfig;
    }>
  | undefined;

export function getSchema(config: ZeroConfig): Promise<{
  schema: Schema;
  authorization: AuthorizationConfig;
}> {
  if (loadedConfig) {
    return loadedConfig;
  }

  const dirname = path.dirname(fileURLToPath(import.meta.url));
  const jsonConfigPath = process.env['ZERO_CONFIG_JSON'];
  const tsConfigPath = process.env['ZERO_CONFIG_PATH'] ?? './schema.ts';

  if (jsonConfigPath) {
    const absoluteJsonPath = path.resolve(jsonConfigPath);
    loadedConfig = readFile(absoluteJsonPath, 'utf-8')
      .then(data => JSON.parse(data) as AuthorizationConfig)
      .catch(e => {
        console.error(
          `Failed to load zero schema from ${absoluteJsonPath}: ${e}`,
        );
        throw e;
      });
    return loadedConfig;
  }

  const absoluteConfigPath = path.resolve(tsConfigPath);
  const relativePath = path.join(
    path.relative(dirname, path.dirname(absoluteConfigPath)),
    path.basename(absoluteConfigPath),
  );

  loadedConfig = tsImport(relativePath, import.meta.url)
    .then(async module => {
      const schema = module.default.schema as Schema;
      const authorization = (await module.default
        .authorization) as AuthorizationConfig;
      return {
        schema,
        authorization,
      } as const;
    })
    .catch(e => {
      console.error(
        `Failed to load zero schema from ${absoluteConfigPath}: ${e}`,
      );
      throw e;
    });
  return loadedConfig;
}
