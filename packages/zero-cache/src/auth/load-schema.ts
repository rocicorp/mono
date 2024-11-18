import path from 'node:path';
import type {AuthorizationConfig} from '../../../zero-schema/src/compiled-authorization.js';
import {fileURLToPath} from 'node:url';
import {tsImport} from 'tsx/esm/api';
import type {ZeroConfig} from '../config/zero-config.js';
import type {Schema} from '../../../zero-schema/src/schema.js';

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
  const absoluteConfigPath = path.resolve(config.schemaFile);
  const relativePath = path.join(
    path.relative(dirname, path.dirname(absoluteConfigPath)),
    path.basename(absoluteConfigPath),
  );

  loadedConfig = tsImport(relativePath, import.meta.url)
    .then(
      async module =>
        (await module.default) as {
          schema: Schema;
          authorization: AuthorizationConfig;
        },
    )
    .catch(e => {
      console.error(
        `Failed to load zero schema from ${absoluteConfigPath}: ${e}`,
      );
      throw e;
    });
  return loadedConfig;
}
