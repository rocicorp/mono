/**
 * app-config is the configuration related to the application(s) powered by
 * Zero.
 *
 * Things in the app-config:
 * - Authorization rules
 * - Authentication secrets. E.g., JWT secret.
 */
import {AST} from 'zql/src/zql/ast/ast.js';
import * as v from 'shared/src/valita.js';
import {astSchema} from 'zero-protocol';
import fs from 'node:fs/promises';

type Action = 'read' | 'insert' | 'update' | 'delete';
type RuleType = 'allow';

const policySchema = v.array(v.tuple([v.literal('allow'), astSchema]));

const assetSchema = v.object({
  read: policySchema,
  insert: policySchema,
  update: policySchema,
  delete: policySchema,
});

const authorizationConfigSchema: v.Type<AuthorizationConfig> = v.record(
  v.object({
    table: assetSchema,
    column: v.record(assetSchema),
    row: assetSchema,
    cell: v.record(assetSchema),
  }),
);

export type AuthorizationConfig = {
  [tableName: string]: {
    table?: {
      [K in Action]: [RuleType, AST][];
    };
    column?: {
      [columnName: string]: {
        [K in Action]: [RuleType, AST][];
      };
    };
    row?: {
      [K in Action]: [RuleType, AST][];
    };
    cell?: {
      [columnName: string]: {
        [K in Action]: [RuleType, AST][];
      };
    };
  };
};

export const appConfigSchema = v.object({
  authorization: authorizationConfigSchema,
});

export type AppConfig = v.Infer<typeof appConfigSchema>;

let loadedConfig: Promise<AppConfig> | undefined;
export function getAppConfig(path: string) {
  if (loadedConfig) {
    return loadedConfig;
  }
  loadedConfig = fs
    .readFile(path, 'utf-8')
    .then(rawContent => v.parse(JSON.parse(rawContent), appConfigSchema));
  return loadedConfig;
}
