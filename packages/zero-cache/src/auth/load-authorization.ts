import path from 'node:path';
import type {AuthorizationConfig} from '../../../zero-schema/src/compiled-authorization.js';
import {fileURLToPath} from 'node:url';
import {tsImport} from 'tsx/esm/api';
import type {ZeroConfig} from '../config/zero-config.js';

let loadedAuthorization: Promise<AuthorizationConfig> | undefined;

export function getAuthorizationConfig(
  config: ZeroConfig,
): Promise<AuthorizationConfig> {
  if (loadedAuthorization) {
    return loadedAuthorization;
  }

  const dirname = path.dirname(fileURLToPath(import.meta.url));
  const absoluteConfigPath = path.resolve(config.schemaFile);
  const relativePath = path.join(
    path.relative(dirname, path.dirname(absoluteConfigPath)),
    path.basename(absoluteConfigPath),
  );

  loadedAuthorization = tsImport(relativePath, import.meta.url)
    .then(async module => (await module.authorization) as AuthorizationConfig)
    .catch(e => {
      console.error(
        `Failed to load zero schema from ${absoluteConfigPath}: ${e}`,
      );
      throw e;
    });
  return loadedAuthorization;
}
