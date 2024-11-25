import type { AuthorizationConfig } from "./compiled-authorization.js";
import type { Schema } from "./schema.js";

export type SchemaConfig = {
  schema: Schema;
  authorization: AuthorizationConfig; 
}

export function isSchemaConfig(value: object): value is SchemaConfig {
  return (
    value !== null &&
    'schema' in value &&
    'authorization' in value
  );
}