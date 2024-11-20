import type {Query} from '../../zql/src/query/query.js';
import type {Schema} from './schema.js';
import type {TableSchema} from './table-schema.js';
import type {
  AssetAuthorization as CompiledAssetAuthorization,
  AuthorizationConfig as CompiledAuthorizationConfig,
} from './compiled-authorization.js';
import {normalizeSchema} from './normalized-schema.js';
import {AuthQuery} from '../../zql/src/query/auth-query.js';
import type {Condition} from '../../zero-protocol/src/ast.js';
import {staticParam} from '../../zql/src/query/query-impl.js';
import type {ExpressionBuilder} from '../../zql/src/query/expression.js';

export type Queries<TSchema extends Schema> = {
  [K in keyof TSchema['tables']]: Query<TSchema['tables'][K]>;
};

type AuthorizationRule<TAuthDataShape, TSchema extends TableSchema> = (
  authData: TAuthDataShape,
  eb: ExpressionBuilder<TSchema>,
) => Condition;

type AssetAuthorization<TAuthDataShape, TSchema extends TableSchema> = {
  select?: AuthorizationRule<TAuthDataShape, TSchema>[] | undefined;
  insert?: AuthorizationRule<TAuthDataShape, TSchema>[] | undefined;
  update?:
    | {
        preMutation?: AuthorizationRule<TAuthDataShape, TSchema>[];
        postProposedMutation?: AuthorizationRule<TAuthDataShape, TSchema>[];
      }
    | undefined;
  delete?: AuthorizationRule<TAuthDataShape, TSchema>[] | undefined;
};

export type AuthorizationConfig<TAuthDataShape, TSchema extends Schema> = {
  [K in keyof TSchema['tables']]?: {
    row?: AssetAuthorization<TAuthDataShape, TSchema['tables'][K]> | undefined;
    cell?:
      | {
          [C in keyof TSchema['tables'][K]['columns']]?: AssetAuthorization<
            TAuthDataShape,
            TSchema['tables'][K]
          >;
        }
      | undefined;
  };
};

export async function defineAuthorization<
  TAuthDataShape,
  TSchema extends Schema,
>(
  schema: TSchema,
  definer: () =>
    | Promise<AuthorizationConfig<TAuthDataShape, TSchema>>
    | AuthorizationConfig<TAuthDataShape, TSchema>,
): Promise<CompiledAuthorizationConfig | undefined> {
  const normalizedSchema = normalizeSchema(schema);
  const expressionBuilders = {} as Record<
    string,
    ExpressionBuilder<TableSchema>
  >;
  for (const [name, tableSchema] of Object.entries(normalizedSchema.tables)) {
    expressionBuilders[name] = new AuthQuery(tableSchema).expressionBuilder();
  }

  const config = await definer();
  return compileAuthorization(config, expressionBuilders);
}

function compileAuthorization<TAuthDataShape, TSchema extends Schema>(
  authz: AuthorizationConfig<TAuthDataShape, TSchema> | undefined,
  expressionBuilders: Record<string, ExpressionBuilder<TableSchema>>,
): CompiledAuthorizationConfig | undefined {
  if (!authz) {
    return undefined;
  }
  const ret: CompiledAuthorizationConfig = {};
  for (const [tableName, tableConfig] of Object.entries(authz)) {
    ret[tableName] = {
      row: compileRowConfig(tableConfig.row, expressionBuilders[tableName]),
      cell: compileCellConfig(tableConfig.cell, expressionBuilders[tableName]),
    };
  }

  return ret;
}

function compileRowConfig<TAuthDataShape, TSchema extends TableSchema>(
  rowRules: AssetAuthorization<TAuthDataShape, TSchema> | undefined,
  expressionBuilder: ExpressionBuilder<TSchema>,
): CompiledAssetAuthorization | undefined {
  if (!rowRules) {
    return undefined;
  }
  return {
    select: compileRules(rowRules.select, expressionBuilder),
    insert: compileRules(rowRules.insert, expressionBuilder),
    update: {
      preMutation: compileRules(
        rowRules.update?.preMutation,
        expressionBuilder,
      ),
      postProposedMutation: compileRules(
        rowRules.update?.postProposedMutation,
        expressionBuilder,
      ),
    },
    delete: compileRules(rowRules.delete, expressionBuilder),
  };
}

function compileRules<TAuthDataShape, TSchema extends TableSchema>(
  rules: AuthorizationRule<TAuthDataShape, TSchema>[] | undefined,
  expressionBuilder: ExpressionBuilder<TSchema>,
): ['allow', Condition][] | undefined {
  if (!rules) {
    return undefined;
  }

  return rules.map(
    rule =>
      [
        'allow',
        rule(authDataRef as TAuthDataShape, expressionBuilder),
      ] as const,
  );
}

function compileCellConfig<TAuthDataShape, TSchema extends TableSchema>(
  cellRules:
    | Record<string, AssetAuthorization<TAuthDataShape, TSchema>>
    | undefined,
  expressionBuilder: ExpressionBuilder<TSchema>,
): Record<string, CompiledAssetAuthorization> | undefined {
  if (!cellRules) {
    return undefined;
  }
  const ret: Record<string, CompiledAssetAuthorization> = {};
  for (const [columnName, rules] of Object.entries(cellRules)) {
    ret[columnName] = {
      select: compileRules(rules.select, expressionBuilder),
      insert: compileRules(rules.insert, expressionBuilder),
      update: {
        preMutation: compileRules(rules.update?.preMutation, expressionBuilder),
        postProposedMutation: compileRules(
          rules.update?.postProposedMutation,
          expressionBuilder,
        ),
      },
      delete: compileRules(rules.delete, expressionBuilder),
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
