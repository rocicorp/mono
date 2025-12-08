import {assert} from '../../shared/src/asserts.ts';
import {
  mapCondition,
  toStaticParam,
  type Condition,
  type Parameter,
} from '../../zero-protocol/src/ast.ts';
import {defaultFormat} from '../../zero-types/src/format.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {ExpressionBuilder} from '../../zql/src/query/expression.ts';
import type {Query} from '../../zql/src/query/query-builder.ts';
import {StaticQuery} from '../../zql/src/query/static-query.ts';
import type {
  AssetPermissions as CompiledAssetPermissions,
  PermissionsConfig as CompiledPermissionsConfig,
} from './compiled-permissions.ts';
import type {NameMapper} from './name-mapper.ts';
import {clientToServer} from './name-mapper.ts';

export const ANYONE_CAN = [
  (_: unknown, eb: ExpressionBuilder<never, Schema>) => eb.and(),
];

/**
 * @deprecated Use {@link ANYONE_CAN} instead.
 */
export const ANYONE_CAN_DO_ANYTHING = {
  row: {
    select: ANYONE_CAN,
    insert: ANYONE_CAN,
    update: {
      preMutation: ANYONE_CAN,
      postMutation: ANYONE_CAN,
    },
    delete: ANYONE_CAN,
  },
};

export const NOBODY_CAN = [];

export type Anchor = 'authData' | 'preMutationRow';

export type Queries<TSchema extends Schema> = {
  [K in keyof TSchema['tables']]: Query<K & string, TSchema>;
};

export type PermissionRule<
  TAuthDataShape,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
> = (
  authData: TAuthDataShape,
  eb: ExpressionBuilder<TTable, TSchema>,
) => Condition;

export type AssetPermissions<
  TAuthDataShape,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
> = {
  // Why an array of rules?: https://github.com/rocicorp/mono/pull/3184/files#r1869680716
  select?: PermissionRule<TAuthDataShape, TSchema, TTable>[] | undefined;
  /**
   * @deprecated Use Mutators instead.
   * @see {@link https://zero.rocicorp.dev/docs/writing-data}
   */
  insert?: PermissionRule<TAuthDataShape, TSchema, TTable>[] | undefined;
  /**
   * @deprecated Use Mutators instead.
   * @see {@link https://zero.rocicorp.dev/docs/writing-data}
   */
  update?:
    | {
        preMutation?: PermissionRule<TAuthDataShape, TSchema, TTable>[];
        postMutation?: PermissionRule<TAuthDataShape, TSchema, TTable>[];
      }
    | undefined;
  /**
   * @deprecated Use Mutators instead.
   * @see {@link https://zero.rocicorp.dev/docs/writing-data}
   */
  delete?: PermissionRule<TAuthDataShape, TSchema, TTable>[] | undefined;
};

export type PermissionsConfig<TAuthDataShape, TSchema extends Schema> = {
  [K in keyof TSchema['tables']]?: {
    row?: AssetPermissions<TAuthDataShape, TSchema, K & string> | undefined;
    cell?:
      | {
          [C in keyof TSchema['tables'][K]['columns']]?: Omit<
            AssetPermissions<TAuthDataShape, TSchema, K & string>,
            'cell'
          >;
        }
      | undefined;
  };
};

/**
 * @deprecated Use {@link defineMutators} and {@link defineQueries} instead.
 */
export async function definePermissions<TAuthDataShape, TSchema extends Schema>(
  schema: TSchema,
  definer: () =>
    | Promise<PermissionsConfig<TAuthDataShape, TSchema>>
    | PermissionsConfig<TAuthDataShape, TSchema>,
): Promise<CompiledPermissionsConfig | undefined> {
  const expressionBuilders = {} as Record<
    string,
    ExpressionBuilder<string, Schema>
  >;
  for (const name of Object.keys(schema.tables)) {
    expressionBuilders[name] = new StaticQuery(
      schema,
      name,
      {table: name},
      defaultFormat,
    ).expressionBuilder();
  }

  const config = await definer();
  return compilePermissions(schema, config, expressionBuilders);
}

function compilePermissions<TAuthDataShape, TSchema extends Schema>(
  schema: TSchema,
  authz: PermissionsConfig<TAuthDataShape, TSchema> | undefined,
  expressionBuilders: Record<string, ExpressionBuilder<string, Schema>>,
): CompiledPermissionsConfig | undefined {
  if (!authz) {
    return undefined;
  }
  const nameMapper = clientToServer(schema.tables);
  const ret: CompiledPermissionsConfig = {tables: {}};
  for (const [tableName, tableConfig] of Object.entries(authz)) {
    const serverName = schema.tables[tableName].serverName ?? tableName;
    ret.tables[serverName] = {
      row: compileRowConfig(
        nameMapper,
        tableName,
        tableConfig.row,
        expressionBuilders[tableName],
      ),
      cell: compileCellConfig(
        nameMapper,
        tableName,
        tableConfig.cell,
        expressionBuilders[tableName],
      ),
    };
  }

  return ret;
}

function compileRowConfig<
  TAuthDataShape,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
>(
  clientToServer: NameMapper,
  tableName: TTable,
  rowRules: AssetPermissions<TAuthDataShape, TSchema, TTable> | undefined,
  expressionBuilder: ExpressionBuilder<TTable, TSchema>,
): CompiledAssetPermissions | undefined {
  if (!rowRules) {
    return undefined;
  }
  return {
    select: compileRules(
      clientToServer,
      tableName,
      rowRules.select,
      expressionBuilder,
    ),
    insert: compileRules(
      clientToServer,
      tableName,
      rowRules.insert,
      expressionBuilder,
    ),
    update: {
      preMutation: compileRules(
        clientToServer,
        tableName,
        rowRules.update?.preMutation,
        expressionBuilder,
      ),
      postMutation: compileRules(
        clientToServer,
        tableName,
        rowRules.update?.postMutation,
        expressionBuilder,
      ),
    },
    delete: compileRules(
      clientToServer,
      tableName,
      rowRules.delete,
      expressionBuilder,
    ),
  };
}

/**
 * What is this "allow" and why are permissions policies an array of rules?
 *
 * Please read: https://github.com/rocicorp/mono/pull/3184/files#r1869680716
 */
function compileRules<
  TAuthDataShape,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
>(
  clientToServer: NameMapper,
  tableName: TTable,
  rules: PermissionRule<TAuthDataShape, TSchema, TTable>[] | undefined,
  expressionBuilder: ExpressionBuilder<TTable, TSchema>,
): ['allow', Condition][] | undefined {
  if (!rules) {
    return undefined;
  }

  return rules.map(rule => {
    const cond = rule(authDataRef as TAuthDataShape, expressionBuilder);
    return ['allow', mapCondition(cond, tableName, clientToServer)] as const;
  });
}

function compileCellConfig<
  TAuthDataShape,
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
>(
  clientToServer: NameMapper,
  tableName: TTable,
  cellRules:
    | Record<string, AssetPermissions<TAuthDataShape, TSchema, TTable>>
    | undefined,
  expressionBuilder: ExpressionBuilder<TTable, TSchema>,
): Record<string, CompiledAssetPermissions> | undefined {
  if (!cellRules) {
    return undefined;
  }
  const ret: Record<string, CompiledAssetPermissions> = {};
  for (const [columnName, rules] of Object.entries(cellRules)) {
    ret[columnName] = {
      select: compileRules(
        clientToServer,
        tableName,
        rules.select,
        expressionBuilder,
      ),
      insert: compileRules(
        clientToServer,
        tableName,
        rules.insert,
        expressionBuilder,
      ),
      update: {
        preMutation: compileRules(
          clientToServer,
          tableName,
          rules.update?.preMutation,
          expressionBuilder,
        ),
        postMutation: compileRules(
          clientToServer,
          tableName,
          rules.update?.postMutation,
          expressionBuilder,
        ),
      },
      delete: compileRules(
        clientToServer,
        tableName,
        rules.delete,
        expressionBuilder,
      ),
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
export function staticParam(
  anchorClass: 'authData' | 'preMutationRow',
  field: string | string[],
): Parameter {
  return {
    type: 'static',
    anchor: anchorClass,
    // for backwards compatibility
    field: field.length === 1 ? field[0] : field,
  };
}
