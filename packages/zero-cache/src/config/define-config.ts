/**
 * Developers can define their configuration via typescript.
 * These types represent the shape that their config must adhere to
 * so we can compile it to a JSON ZeroConfig.
 */
import {Query, SchemaToRow} from 'zql/src/zql/query/query.js';
import {Action, ZeroConfigSansAuthorization} from './zero-config.js';
import {Schema} from 'zql/src/zql/query/schema.js';
import type {
  ZeroConfig as CompiledZeroConfig,
  AssetAuthorization as CompiledAssetAuthorization,
  AuthorizationConfig as CompiledAuthorizationConfig,
} from './zero-config.js';
import path from 'node:path';
import fs from 'node:fs';
import {newQuery} from 'zql/src/zql/query/query-impl.js';
import {ConfigZqlContext} from './config-zql-context.js';
import {AST} from 'zql/src/zql/ast/ast.js';

type SchemaDefs = {
  readonly [table: string]: Schema;
};

export type Queries<TSchemas extends SchemaDefs> = {
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
    authorization: compileAuthorization(config.authorization),
  };
}

function compileAuthorization<TAuthDataShape, TSchemas extends SchemaDefs>(
  authz: AuthorizationConfig<TAuthDataShape, TSchemas> | undefined,
): CompiledAuthorizationConfig | undefined {
  if (!authz) {
    return undefined;
  }
  const ret: CompiledAuthorizationConfig = {};
  for (const [tableName, tableConfig] of Object.entries(authz)) {
    ret[tableName] = {
      table: compileTableConfig(tableConfig.table),
      column: compileColumnConfig(tableConfig.column),
      row: compileRowConfig(tableConfig.row),
      cell: compileCellConfig(tableConfig.cell),
    };
  }

  return ret;
}

function compileTableConfig<TAuthDataShape>(
  tableRules: StaticAssetAuthorization<TAuthDataShape> | undefined,
): CompiledAssetAuthorization | undefined {
  if (!tableRules) {
    return undefined;
  }
  return {
    select: compileStaticRules(tableRules.select),
    insert: compileStaticRules(tableRules.insert),
    update: compileStaticRules(tableRules.update),
    delete: compileStaticRules(tableRules.delete),
  };
}

function compileStaticRules<TAuthDataShape>(
  rules: StaticAuthzRule<TAuthDataShape>[] | undefined,
): ['allow', AST][] | undefined {
  if (!rules) {
    return undefined;
  }
  // TODO: implement `AuthDataRef` to pass auth data to rules that will inject Parameter references
  return rules.map(
    rule =>
      [
        'allow',
        (
          rule({} as TAuthDataShape) as unknown as {
            ast: AST;
          }
        ).ast,
      ] as const,
  );
}

function compileColumnConfig<TAuthDataShape>(
  columnRules:
    | Record<string, StaticAssetAuthorization<TAuthDataShape>>
    | undefined,
): Record<string, CompiledAssetAuthorization> | undefined {
  if (!columnRules) {
    return undefined;
  }
  const ret: Record<string, CompiledAssetAuthorization> = {};
  for (const [columnName, rules] of Object.entries(columnRules)) {
    ret[columnName] = {
      select: compileStaticRules(rules.select),
      insert: compileStaticRules(rules.insert),
      update: compileStaticRules(rules.update),
      delete: compileStaticRules(rules.delete),
    };
  }
  return ret;
}

function compileRowConfig<TAuthDataShape, TSchema extends Schema>(
  rowRules: InstanceAssetAuthorization<TAuthDataShape, TSchema> | undefined,
): CompiledAssetAuthorization | undefined {
  if (!rowRules) {
    return undefined;
  }
  return {
    select: compileInstanceRules(rowRules.select),
    insert: compileInstanceRules(rowRules.insert),
    update: compileInstanceRules(rowRules.update),
    delete: compileInstanceRules(rowRules.delete),
  };
}

function compileInstanceRules<TAuthDataShape, TSchema extends Schema>(
  rules: InstanceAuthzRule<TAuthDataShape, TSchema>[] | undefined,
): ['allow', AST][] | undefined {
  if (!rules) {
    return undefined;
  }

  return rules.map(
    rule =>
      [
        'allow',
        // TODO: implement RowRef to inject Parameter references
        (
          rule({} as TAuthDataShape, {} as SchemaToRow<TSchema>) as unknown as {
            ast: AST;
          }
        ).ast,
      ] as const,
  );
}

function compileCellConfig<TAuthDataShape, TSchema extends Schema>(
  cellRules:
    | Record<string, InstanceAssetAuthorization<TAuthDataShape, TSchema>>
    | undefined,
): Record<string, CompiledAssetAuthorization> | undefined {
  if (!cellRules) {
    return undefined;
  }
  const ret: Record<string, CompiledAssetAuthorization> = {};
  for (const [columnName, rules] of Object.entries(cellRules)) {
    ret[columnName] = {
      select: compileInstanceRules(rules.select),
      insert: compileInstanceRules(rules.insert),
      update: compileInstanceRules(rules.update),
      delete: compileInstanceRules(rules.delete),
    };
  }
  return ret;
}
