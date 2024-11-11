import path from 'node:path';
import type {AuthorizationConfig} from '../../../zero-schema/src/compiled-authorization.js';
import {fileURLToPath} from 'node:url';
import {tsImport} from 'tsx/esm/api';

let loadedConfig: Promise<AuthorizationConfig> | undefined;

export function getAuthorizationConfig(): Promise<AuthorizationConfig> {
  if (loadedConfig) {
    return loadedConfig;
  }

  const dirname = path.dirname(fileURLToPath(import.meta.url));
  const configFile = process.env['ZERO_CONFIG_PATH'] ?? './schema.ts';
  const absoluteConfigPath = path.resolve(configFile);
  const relativePath = path.join(
    path.relative(dirname, path.dirname(absoluteConfigPath)),
    path.basename(absoluteConfigPath),
  );

  loadedConfig = tsImport(relativePath, import.meta.url)
    .then(async module => (await module.authorization) as AuthorizationConfig)
    .catch(e => {
      console.error(
        `Failed to load zero schema from ${absoluteConfigPath}: ${e}`,
      );
      throw e;
    });
  return loadedConfig;
}
