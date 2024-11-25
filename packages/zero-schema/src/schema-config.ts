import type { AuthorizationConfig } from "./compiled-authorization.js";
import type { Schema } from "./schema.js";

export type SchemaConfig = {
  schema: Schema;
  authorization: AuthorizationConfig; 
}