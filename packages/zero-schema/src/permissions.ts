import type {Query} from '../../zql/src/query/query.js';
import type {Schema} from './schema.js';
import type {TableSchema} from './table-schema.js';
import type {
  AssetPermissions as CompiledAssetPermissions,
  PermissionsConfig as CompiledPermissionsConfig,
} from './compiled-permissions.js';
import {normalizeSchema} from './normalized-schema.js';
import {AuthQuery} from '../../zql/src/query/auth-query.js';
import {
  toStaticParam,
  type Condition,
  type Parameter,
} from '../../zero-protocol/src/ast.js';
import {staticParam} from '../../zql/src/query/query-impl.js';
import type {ExpressionBuilder} from '../../zql/src/query/expression.js';
import {assert} from '../../shared/src/asserts.js';

export const ANYONE_CAN = undefined;
export const NOBODY_CAN = [];
export type Anchor = 'authData' | 'preMutationRow';

export type Queries<TSchema extends Schema> = {
  [K in keyof TSchema['tables']]: Query<TSchema['tables'][K]>;
};

type PermissionRule<TAuthDataShape, TSchema extends TableSchema> = (
  authData: TAuthDataShape,
  eb: ExpressionBuilder<TSchema>,
) => Condition;

type AssetPermissions<TAuthDataShape, TSchema extends TableSchema> = {
  // Why an array of rules?: https://github.com/rocicorp/mono/pull/3184/files#r1869680716
  select?: PermissionRule<TAuthDataShape, TSchema>[] | undefined;
  insert?: PermissionRule<TAuthDataShape, TSchema>[] | undefined;
  update?:
    | {
        preMutation?: PermissionRule<TAuthDataShape, TSchema>[];
        postMutation?: PermissionRule<TAuthDataShape, TSchema>[];
      }
    | undefined;
  delete?: PermissionRule<TAuthDataShape, TSchema>[] | undefined;
};

export type PermissionsConfig<TAuthDataShape, TSchema extends Schema> = {
  [K in keyof TSchema['tables']]?: {
    row?: AssetPermissions<TAuthDataShape, TSchema['tables'][K]> | undefined;
    cell?:
      | {
          [C in keyof TSchema['tables'][K]['columns']]?: Omit<
            AssetPermissions<TAuthDataShape, TSchema['tables'][K]>,
            'cell'
          >;
        }
      | undefined;
  };
};

export async function definePermissions<TAuthDataShape, TSchema extends Schema>(
  schema: TSchema,
  definer: () =>
    | Promise<PermissionsConfig<TAuthDataShape, TSchema>>
    | PermissionsConfig<TAuthDataShape, TSchema>,
): Promise<CompiledPermissionsConfig | undefined> {
  const normalizedSchema = normalizeSchema(schema);
  const expressionBuilders = {} as Record<
    string,
    ExpressionBuilder<TableSchema>
  >;
  for (const [name, tableSchema] of Object.entries(normalizedSchema.tables)) {
    expressionBuilders[name] = new AuthQuery(tableSchema).expressionBuilder();
  }

  const config = await definer();
  return compilePermissions(config, expressionBuilders);
}

function compilePermissions<TAuthDataShape, TSchema extends Schema>(
  authz: PermissionsConfig<TAuthDataShape, TSchema> | undefined,
  expressionBuilders: Record<string, ExpressionBuilder<TableSchema>>,
): CompiledPermissionsConfig | undefined {
  if (!authz) {
    return undefined;
  }
  const ret: CompiledPermissionsConfig = {};
  for (const [tableName, tableConfig] of Object.entries(authz)) {
    ret[tableName] = {
      row: compileRowConfig(tableConfig.row, expressionBuilders[tableName]),
      cell: compileCellConfig(tableConfig.cell, expressionBuilders[tableName]),
    };
  }

  return ret;
}

function compileRowConfig<TAuthDataShape, TSchema extends TableSchema>(
  rowRules: AssetPermissions<TAuthDataShape, TSchema> | undefined,
  expressionBuilder: ExpressionBuilder<TSchema>,
): CompiledAssetPermissions | undefined {
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
      postMutation: compileRules(
        rowRules.update?.postMutation,
        expressionBuilder,
      ),
    },
    delete: compileRules(rowRules.delete, expressionBuilder),
  };
}

/**
 * What is this "allow" and why are permissions policies an array of rules?
 *
 * Please read: https://github.com/rocicorp/mono/pull/3184/files#r1869680716
 */
function compileRules<TAuthDataShape, TSchema extends TableSchema>(
  rules: PermissionRule<TAuthDataShape, TSchema>[] | undefined,
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
    | Record<string, AssetPermissions<TAuthDataShape, TSchema>>
    | undefined,
  expressionBuilder: ExpressionBuilder<TSchema>,
): Record<string, CompiledAssetPermissions> | undefined {
  if (!cellRules) {
    return undefined;
  }
  const ret: Record<string, CompiledAssetPermissions> = {};
  for (const [columnName, rules] of Object.entries(cellRules)) {
    ret[columnName] = {
      select: compileRules(rules.select, expressionBuilder),
      insert: compileRules(rules.insert, expressionBuilder),
      update: {
        preMutation: compileRules(rules.update?.preMutation, expressionBuilder),
        postMutation: compileRules(
          rules.update?.postMutation,
          expressionBuilder,
        ),
      },
      delete: compileRules(rules.delete, expressionBuilder),
    };
  }
  return ret;
}

class CallTracker {
  readonly #anchor: Anchor;
  readonly #path: string[];
  constructor(anchor: Anchor, path: string[]) {
    this.#anchor = anchor;
    this.#path = path;
  }

  get(target: {[toStaticParam]: () => Parameter}, prop: string | symbol) {
    if (prop === toStaticParam) {
      return target[toStaticParam];
    }
    assert(typeof prop === 'string');
    const path = [...this.#path, prop];
    return new Proxy(
      {
        [toStaticParam]: () => staticParam(this.#anchor, path),
      },
      new CallTracker(this.#anchor, path),
    );
  }
}

function baseTracker(anchor: Anchor) {
  return new Proxy(
    {
      [toStaticParam]: () => {
        throw new Error('no JWT field specified');
      },
    },
    new CallTracker(anchor, []),
  );
}

export const authDataRef = baseTracker('authData');
export const preMutationRowRef = baseTracker('preMutationRow');
