/**
 * Developers can define their configuration via typescript.
 * These types represent the shape that their config must adhere to
 * so we can compile it to a JSON ZeroConfig.
 */
import {normalizeSchema} from '../../../zero-client/src/client/normalized-schema.js';
import type {AST} from '../../../zql/src/zql/ast/ast.js';
import type {Query, SchemaToRow} from '../../../zql/src/zql/query/query.js';
import type {TableSchema} from '../../../zql/src/zql/query/schema.js';
import {ConfigQuery} from './config-query.js';
import {authDataRef, preMutationRowRef} from './refs.js';
import type {
  Action,
  AssetAuthorization as CompiledAssetAuthorization,
  AuthorizationConfig as CompiledAuthorizationConfig,
  ZeroConfig as CompiledZeroConfig,
  EnvRef,
  ZeroConfigSansAuthorization,
} from './zero-config.js';

type Schema = {
  readonly version: number;
  readonly tables: {readonly [table: string]: TableSchema};
};

export type Queries<TSchema extends Schema> = {
  [K in keyof TSchema['tables']]: Query<TSchema['tables'][K]>;
};

type InstanceAuthzRule<TAuthDataShape, TSchema extends TableSchema> = (
  authData: TAuthDataShape,
  row: SchemaToRow<TSchema>,
) => Query<TableSchema>;

type StaticAuthzRule<TAuthDataShape> = (
  authData: TAuthDataShape,
) => Query<TableSchema>;

type StaticAssetAuthorization<TAuthDataShape> = {
  [K in Action]?: StaticAuthzRule<TAuthDataShape>[];
};

type InstanceAssetAuthorization<TAuthDataShape, TSchema extends TableSchema> = {
  [K in Action]?: InstanceAuthzRule<TAuthDataShape, TSchema>[];
};

export type AuthorizationConfig<TAuthDataShape, TSchema extends Schema> = {
  [K in keyof TSchema['tables']]?: {
    table?: StaticAssetAuthorization<TAuthDataShape>;
    column?: {
      [C in keyof TSchema['tables'][K]['columns']]?: StaticAssetAuthorization<TAuthDataShape>;
    };
    row?: InstanceAssetAuthorization<TAuthDataShape, TSchema['tables'][K]>;
    cell?: {
      [C in keyof TSchema['tables'][K]['columns']]?: InstanceAssetAuthorization<
        TAuthDataShape,
        TSchema['tables'][K]
      >;
    };
  };
};

export type ZeroConfig<
  TAuthDataShape,
  TSchema extends Schema,
> = ZeroConfigSansAuthorization & {
  authorization?: AuthorizationConfig<TAuthDataShape, TSchema>;
};

export function runtimeEnv(key: string): EnvRef {
  return {tag: 'env', name: key};
}

export function defineConfig<TAuthDataShape, TSchema extends Schema>(
  schema: TSchema,
  definer: (queries: Queries<TSchema>) => ZeroConfig<TAuthDataShape, TSchema>,
): CompiledZeroConfig {
  const normalizedSchema = normalizeSchema(schema);
  const queries = {} as Record<string, Query<TableSchema>>;
  for (const [name, tableSchema] of Object.entries(normalizedSchema.tables)) {
    queries[name] = new ConfigQuery(tableSchema);
  }

  const config = definer(queries as Queries<TSchema>);
  return compileConfig(config);
}

const DEFAULT_SHARD_ID = '0';
function compileConfig<TAuthDataShape, TSchema extends Schema>(
  config: ZeroConfig<TAuthDataShape, TSchema>,
): CompiledZeroConfig {
  return {
    ...config,
    authorization: compileAuthorization(config.authorization),
    shard: {
      id: config.shard?.id ?? DEFAULT_SHARD_ID,
      publications: config?.shard?.publications ?? [],
    },
  };
}

function compileAuthorization<TAuthDataShape, TSchema extends Schema>(
  authz: AuthorizationConfig<TAuthDataShape, TSchema> | undefined,
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
  return rules.map(
    rule =>
      [
        'allow',
        (
          rule(authDataRef as TAuthDataShape) as unknown as {
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

function compileRowConfig<TAuthDataShape, TSchema extends TableSchema>(
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

function compileInstanceRules<TAuthDataShape, TSchema extends TableSchema>(
  rules: InstanceAuthzRule<TAuthDataShape, TSchema>[] | undefined,
): ['allow', AST][] | undefined {
  if (!rules) {
    return undefined;
  }

  return rules.map(
    rule =>
      [
        'allow',
        (
          rule(
            authDataRef as TAuthDataShape,
            preMutationRowRef as SchemaToRow<TSchema>,
          ) as unknown as {
            ast: AST;
          }
        ).ast,
      ] as const,
  );
}

function compileCellConfig<TAuthDataShape, TSchema extends TableSchema>(
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
