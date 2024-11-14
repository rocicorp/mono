import type {Query} from '../../zql/src/query/query.js';
import type {Schema} from './schema.js';
import type {TableSchema, TableSchemaToRow} from './table-schema.js';
import type {
  AssetAuthorization as CompiledAssetAuthorization,
  AuthorizationConfig as CompiledAuthorizationConfig,
} from './compiled-authorization.js';
import {normalizeSchema} from './normalized-schema.js';
import {AuthQuery} from '../../zql/src/query/auth-query.js';
import type {AST} from '../../zero-protocol/src/ast.js';
import {staticParam} from '../../zql/src/query/query-impl.js';

type Action = 'select' | 'insert' | 'update' | 'delete';

export type Queries<TSchema extends Schema> = {
  [K in keyof TSchema['tables']]: Query<TSchema['tables'][K]>;
};

type InstanceAuthzRule<TAuthDataShape, TSchema extends TableSchema> = (
  authData: TAuthDataShape,
  row: TableSchemaToRow<TSchema>,
) => Query<TableSchema>;

type InstanceAssetAuthorization<TAuthDataShape, TSchema extends TableSchema> = {
  [K in Action]?: InstanceAuthzRule<TAuthDataShape, TSchema>[];
};

export type AuthorizationConfig<TAuthDataShape, TSchema extends Schema> = {
  [K in keyof TSchema['tables']]?: {
    row?: InstanceAssetAuthorization<TAuthDataShape, TSchema['tables'][K]>;
    cell?: {
      [C in keyof TSchema['tables'][K]['columns']]?: InstanceAssetAuthorization<
        TAuthDataShape,
        TSchema['tables'][K]
      >;
    };
  };
};

export async function defineAuthorization<
  TAuthDataShape,
  TSchema extends Schema,
>(
  schema: TSchema,
  definer: (
    query: Queries<TSchema>,
  ) =>
    | Promise<AuthorizationConfig<TAuthDataShape, TSchema>>
    | AuthorizationConfig<TAuthDataShape, TSchema>,
): Promise<CompiledAuthorizationConfig | undefined> {
  const normalizedSchema = normalizeSchema(schema);
  const queries = {} as Record<string, Query<TableSchema>>;
  for (const [name, tableSchema] of Object.entries(normalizedSchema.tables)) {
    queries[name] = new AuthQuery(tableSchema);
  }

  const config = await definer(queries as Queries<TSchema>);
  return compileAuthorization(config);
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
      row: compileRowConfig(tableConfig.row),
      cell: compileCellConfig(tableConfig.cell),
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
            preMutationRowRef as TableSchemaToRow<TSchema>,
          ) as AuthQuery<TableSchema>
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

export const authDataRef = new Proxy(
  {},
  {
    get(_target, prop, _receiver) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return staticParam<any, any>('authData', prop as string);
    },
  },
);

export const preMutationRowRef = new Proxy(
  {},
  {
    get(_target, prop, _receiver) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return staticParam<any, any>('preMutationRow', prop as string);
    },
  },
);
