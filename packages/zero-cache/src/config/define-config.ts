/**
 * Developers can define their configuration via typescript.
 * These types represent the shape that their config must adhere to
 * so we can compile it to a JSON ZeroConfig.
 */
import {Query, SchemaToRow} from 'zql/src/zql/query/query.js';
import {Action, ZeroConfigSansAuthorization} from './zero-config.js';
import {Schema} from 'zql/src/zql/query/schema.js';
import type {ZeroConfig as CompiledZeroConfig} from './zero-config.js';
import path from 'node:path';
import fs from 'node:fs';
import {newQuery} from 'zql/src/zql/query/query-impl.js';
import {ConfigZqlContext} from './config-zql-context.js';

type SchemaDefs = {
  readonly [table: string]: Schema;
};

type Queries<TSchemas extends SchemaDefs> = {
  [K in keyof TSchemas]: Query<TSchemas[K]>;
};

type InstanceAuthzRule<TAuthDataShape, TSchema extends Schema> = (
  authData: TAuthDataShape,
  row: SchemaToRow<TSchema>,
) => Query<Schema>;

type StaticAuthzRule<TAuthDataShape> = (
  authData: TAuthDataShape,
) => Query<Schema>;

type StaticAssetAuthorization<TAuthDataShape> = {
  [K in Action]?: StaticAuthzRule<TAuthDataShape>[];
};

type InstanceAssetAuthorization<TAuthDataShape, TSchema extends Schema> = {
  [K in Action]?: InstanceAuthzRule<TAuthDataShape, TSchema>[];
};

export type AuthorizationConfig<TAuthDataShape, TSchemas extends SchemaDefs> = {
  [K in keyof TSchemas]?: {
    table?: StaticAssetAuthorization<TAuthDataShape>;
    column?: StaticAssetAuthorization<TAuthDataShape>;
    row?: InstanceAssetAuthorization<TAuthDataShape, TSchemas[K]>;
    cell?: InstanceAssetAuthorization<TAuthDataShape, TSchemas[K]>;
  };
};

export type ZeroConfig<
  TAuthDataShape,
  TSchemas extends SchemaDefs,
> = ZeroConfigSansAuthorization & {
  authorization?: AuthorizationConfig<TAuthDataShape, TSchemas>;
};

export function defineConfig<TAuthDataShape, TSchemas extends SchemaDefs>(
  schemas: TSchemas,
  definer: (queries: Queries<TSchemas>) => ZeroConfig<TAuthDataShape, TSchemas>,
) {
  const queries = {} as Record<string, Query<Schema>>;
  const context = new ConfigZqlContext();
  for (const [name, schema] of Object.entries(schemas)) {
    queries[name] = newQuery(context, schema);
  }

  const config = definer(queries as Queries<TSchemas>);
  const compiled = compileConfig(config);
  const serializedConfig = JSON.stringify(compiled, null, 2);
  const dest = path.join(process.cwd(), 'zero.config.json');
  return fs.writeFileSync(dest, serializedConfig);
}

function compileConfig<TAuthDataShape, TSchemas extends SchemaDefs>(
  config: ZeroConfig<TAuthDataShape, TSchemas>,
): CompiledZeroConfig {
  return {
    ...config,
    // To be completed in a follow up PR
    authorization: undefined,
  };
}
