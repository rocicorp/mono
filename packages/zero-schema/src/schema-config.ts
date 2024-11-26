import type {PermissionsConfig} from './compiled-permissions.js';
import type {Schema} from './schema.js';

export type SchemaConfig = {
  schema: Schema;
  permissions: PermissionsConfig;
};

export function isSchemaConfig(value: object): value is SchemaConfig {
  return value !== null && 'schema' in value && 'permissions' in value;
}
