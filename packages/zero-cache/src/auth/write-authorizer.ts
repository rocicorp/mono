import type {SQLQuery} from '@databases/sql';
import {LogContext} from '@rocicorp/logger';
import type {JWTPayload} from 'jose';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {pid} from 'node:process';
import {assert} from '../../../shared/src/asserts.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import {randInt} from '../../../shared/src/rand.ts';
import * as v from '../../../shared/src/valita.ts';
import type {Condition} from '../../../zero-protocol/src/ast.ts';
import {
  primaryKeyValueSchema,
  type PrimaryKeyValue,
} from '../../../zero-protocol/src/primary-key.ts';
import type {
  CRUDOp,
  DeleteOp,
  InsertOp,
  UpdateOp,
  UpsertOp,
} from '../../../zero-protocol/src/push.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {
  PermissionsConfig,
  Policy,
} from '../../../zero-schema/src/compiled-permissions.ts';
import type {BuilderDelegate} from '../../../zql/src/builder/builder.ts';
import {
  bindStaticParameters,
  buildPipeline,
} from '../../../zql/src/builder/builder.ts';
import {AuthQuery, authQuery} from '../../../zql/src/query/auth-query.ts';
import {dnf} from '../../../zql/src/query/dnf.ts';
import type {Query} from '../../../zql/src/query/query.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {compile, sql} from '../../../zqlite/src/internal/sql.ts';
import {
  fromSQLiteTypes,
  TableSource,
} from '../../../zqlite/src/table-source.ts';
import type {LogConfig, ZeroConfig} from '../config/zero-config.ts';
import {computeZqlSpecs} from '../db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../db/specs.ts';
import {StatementRunner} from '../db/statements.ts';
import {DatabaseStorage} from '../services/view-syncer/database-storage.ts';
import {mapLiteDataTypeToZqlSchemaValue} from '../types/lite.ts';

type Phase = 'preMutation' | 'postMutation';

export interface WriteAuthorizer {
  canPreMutation(
    authData: JWTPayload | undefined,
    ops: Exclude<CRUDOp, UpsertOp>[],
  ): boolean;
  canPostMutation(
    authData: JWTPayload | undefined,
    ops: Exclude<CRUDOp, UpsertOp>[],
  ): boolean;
  normalizeOps(ops: CRUDOp[]): Exclude<CRUDOp, UpsertOp>[];
}

export class WriteAuthorizerImpl implements WriteAuthorizer {
  readonly #schema: Schema;
  readonly #permissionsConfig: PermissionsConfig;
  readonly #replica: Database;
  readonly #builderDelegate: BuilderDelegate;
  readonly #tableSpecs: Map<string, LiteAndZqlSpec>;
  readonly #tables = new Map<string, TableSource>();
  readonly #statementRunner: StatementRunner;
  readonly #lc: LogContext;
  readonly #clientGroupID: string;
  readonly #logConfig: LogConfig;

  constructor(
    lc: LogContext,
    config: ZeroConfig,
    schema: Schema,
    permissions: PermissionsConfig | undefined,
    replica: Database,
    cgID: string,
  ) {
    this.#clientGroupID = cgID;
    this.#lc = lc.withContext('class', 'WriteAuthorizerImpl');
    this.#logConfig = config.log;
    this.#schema = schema;
    this.#permissionsConfig = permissions ?? {};
    this.#replica = replica;
    const tmpDir = config.storageDBTmpDir ?? tmpdir();
    const writeAuthzStorage = DatabaseStorage.create(
      lc,
      path.join(tmpDir, `mutagen-${pid}-${randInt(1000000, 9999999)}`),
    );
    const cgStorage = writeAuthzStorage.createClientGroupStorage(cgID);
    this.#builderDelegate = {
      getSource: name => this.#getSource(name),
      createStorage: () => cgStorage.createStorage(),
    };
    this.#tableSpecs = computeZqlSpecs(this.#lc, replica);
    this.#statementRunner = new StatementRunner(replica);
  }

  canPreMutation(
    authData: JWTPayload | undefined,
    ops: Exclude<CRUDOp, UpsertOp>[],
  ) {
    for (const op of ops) {
      switch (op.op) {
        case 'insert':
          // insert does not run pre-mutation checks
          break;
        case 'update':
          if (!this.#canUpdate('preMutation', authData, op)) {
            return false;
          }
          break;
        case 'delete':
          if (!this.#canDelete('preMutation', authData, op)) {
            return false;
          }
          break;
      }
    }
    return true;
  }

  canPostMutation(
    authData: JWTPayload | undefined,
    ops: Exclude<CRUDOp, UpsertOp>[],
  ) {
    this.#statementRunner.beginConcurrent();
    try {
      for (const op of ops) {
        const source = this.#getSource(op.tableName);
        switch (op.op) {
          case 'insert': {
            source.push({
              type: 'add',
              row: op.value,
            });
            break;
          }
          // TODO (mlaw): what if someone updates the same thing twice?
          case 'update': {
            source.push({
              type: 'edit',
              oldRow: this.#requirePreMutationRow(op),
              row: op.value,
            });
            break;
          }
          case 'delete': {
            source.push({
              type: 'remove',
              row: this.#requirePreMutationRow(op),
            });
            break;
          }
        }
      }

      for (const op of ops) {
        switch (op.op) {
          case 'insert':
            if (!this.#canInsert('postMutation', authData, op)) {
              return false;
            }
            break;
          case 'update':
            if (!this.#canUpdate('postMutation', authData, op)) {
              return false;
            }
            break;
          case 'delete':
            // delete does not run post-mutation checks.
            break;
        }
      }
    } finally {
      this.#statementRunner.rollback();
    }

    return true;
  }

  normalizeOps(ops: CRUDOp[]): Exclude<CRUDOp, UpsertOp>[] {
    return ops.map(op => {
      if (op.op === 'upsert') {
        const preMutationRow = this.#getPreMutationRow(op);
        if (preMutationRow) {
          return {
            op: 'update',
            tableName: op.tableName,
            primaryKey: op.primaryKey,
            value: op.value,
          };
        }
        return {
          op: 'insert',
          tableName: op.tableName,
          primaryKey: op.primaryKey,
          value: op.value,
        };
      }
      return op;
    });
  }

  #canInsert(phase: Phase, authData: JWTPayload | undefined, op: InsertOp) {
    return this.#timedCanDo(phase, 'insert', authData, op);
  }

  #canUpdate(phase: Phase, authData: JWTPayload | undefined, op: UpdateOp) {
    return this.#timedCanDo(phase, 'update', authData, op);
  }

  #canDelete(phase: Phase, authData: JWTPayload | undefined, op: DeleteOp) {
    return this.#timedCanDo(phase, 'delete', authData, op);
  }

  #getSource(tableName: string) {
    let source = this.#tables.get(tableName);
    if (source) {
      return source;
    }
    const tableSpec = this.#tableSpecs.get(tableName);
    if (!tableSpec) {
      throw new Error(`Table ${tableName} not found`);
    }
    const {columns, primaryKey} = tableSpec.tableSpec;
    assert(primaryKey.length);
    source = new TableSource(
      this.#lc,
      this.#logConfig,
      this.#clientGroupID,
      this.#replica,
      tableName,
      Object.fromEntries(
        Object.entries(columns).map(([name, {dataType}]) => [
          name,
          mapLiteDataTypeToZqlSchemaValue(dataType),
        ]),
      ),
      [primaryKey[0], ...primaryKey.slice(1)],
    );
    this.#tables.set(tableName, source);

    return source;
  }

  #timedCanDo<A extends keyof ActionOpMap>(
    phase: Phase,
    action: A,
    authData: JWTPayload | undefined,
    op: ActionOpMap[A],
  ) {
    const start = performance.now();
    try {
      const ret = this.#canDo(phase, action, authData, op);
      return ret;
    } finally {
      this.#lc.info?.(
        'action:',
        action,
        'duration:',
        performance.now() - start,
        'tableName:',
        op.tableName,
        'primaryKey:',
        op.primaryKey,
      );
    }
  }

  /**
   * Evaluation order is from static to dynamic, broad to specific.
   * table -> column -> row -> cell.
   *
   * If any step fails, the entire operation is denied.
   *
   * That is, table rules supersede column rules, which supersede row rules,
   *
   * All steps must allow for the operation to be allowed.
   */
  #canDo<A extends keyof ActionOpMap>(
    phase: Phase,
    action: A,
    authData: JWTPayload | undefined,
    op: ActionOpMap[A],
  ) {
    const rules = this.#permissionsConfig[op.tableName];
    if (rules?.row === undefined && rules?.cell === undefined) {
      return true;
    }

    const rowPolicies = rules.row;
    let rowQuery = authQuery(this.#schema, op.tableName);
    op.primaryKey.forEach(pk => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      rowQuery = rowQuery.where(pk, '=', op.value[pk] as any);
    });

    let applicableRowPolicy: Policy | undefined;
    switch (action) {
      case 'insert':
        if (rowPolicies && rowPolicies.insert && phase === 'postMutation') {
          applicableRowPolicy = rowPolicies.insert;
        }
        break;
      case 'update':
        if (rowPolicies && rowPolicies.update) {
          if (phase === 'preMutation') {
            applicableRowPolicy = rowPolicies.update.preMutation;
          } else if (phase === 'postMutation') {
            applicableRowPolicy = rowPolicies.update.postMutation;
          }
        }
        break;
      case 'delete':
        if (rowPolicies && rowPolicies.delete && phase === 'preMutation') {
          applicableRowPolicy = rowPolicies.delete;
        }
        break;
    }

    const cellPolicies = rules.cell;
    const applicableCellPolicies: Policy[] = [];
    if (cellPolicies) {
      for (const [column, policy] of Object.entries(cellPolicies)) {
        if (action === 'update' && op.value[column] === undefined) {
          // If the cell is not being updated, we do not need to check
          // the cell rules.
          continue;
        }
        switch (action) {
          case 'insert':
            if (policy.insert && phase === 'postMutation') {
              applicableCellPolicies.push(policy.insert);
            }
            break;
          case 'update':
            if (phase === 'preMutation' && policy.update?.preMutation) {
              applicableCellPolicies.push(policy.update.preMutation);
            }
            if (phase === 'postMutation' && policy.update?.postMutation) {
              applicableCellPolicies.push(policy.update.postMutation);
            }
            break;
          case 'delete':
            if (policy.delete && phase === 'preMutation') {
              applicableCellPolicies.push(policy.delete);
            }
            break;
        }
      }
    }

    if (
      !this.#passesPolicyGroup(
        applicableRowPolicy,
        applicableCellPolicies,
        authData,
        rowQuery,
      )
    ) {
      return false;
    }

    return true;
  }

  #getPreMutationRow(op: UpsertOp | UpdateOp | DeleteOp) {
    const {value} = op;
    const conditions: SQLQuery[] = [];
    const values: PrimaryKeyValue[] = [];
    for (const pk of op.primaryKey) {
      conditions.push(sql`${sql.ident(pk)}=?`);
      values.push(v.parse(value[pk], primaryKeyValueSchema));
    }

    const spec = this.#tableSpecs.get(op.tableName);
    if (spec === undefined) {
      throw new Error(`Table ${op.tableName} not found`);
    }

    const ret = this.#statementRunner.get(
      compile(
        sql`SELECT ${sql.join(
          Object.keys(spec.zqlSpec).map(c => sql.ident(c)),
          sql`,`,
        )} FROM ${sql.ident(op.tableName)} WHERE ${sql.join(
          conditions,
          sql` AND `,
        )}`,
      ),
      ...values,
    );
    if (ret === undefined) {
      return ret;
    }
    return fromSQLiteTypes(spec.zqlSpec, ret);
  }

  #requirePreMutationRow(op: UpdateOp | DeleteOp) {
    const ret = this.#getPreMutationRow(op);
    assert(
      ret !== undefined,
      () => `Pre-mutation row not found for ${JSON.stringify(op.value)}`,
    );
    return ret;
  }

  #passesPolicyGroup(
    applicableRowPolicy: Policy | undefined,
    applicableCellPolicies: Policy[],
    authData: JWTPayload | undefined,
    rowQuery: Query<Schema, string>,
  ) {
    if (
      applicableRowPolicy === undefined &&
      applicableCellPolicies.length === 0
    ) {
      return true;
    }

    if (!this.#passesPolicy(applicableRowPolicy, authData, rowQuery)) {
      return false;
    }

    for (const policy of applicableCellPolicies) {
      if (!this.#passesPolicy(policy, authData, rowQuery)) {
        return false;
      }
    }

    return true;
  }

  #passesPolicy(
    policy: Policy | undefined,
    authData: JWTPayload | undefined,
    rowQuery: Query<Schema, string>,
  ) {
    if (policy === undefined) {
      return true;
    }
    if (policy.length === 0) {
      return false;
    }
    let rowQueryAst = (rowQuery as AuthQuery<Schema, string>).ast;
    rowQueryAst = bindStaticParameters(
      {
        ...rowQueryAst,
        where: updateWhere(rowQueryAst.where, policy),
      },
      {
        authData: authData as Record<string, JSONValue>,
        preMutationRow: undefined,
      },
    );

    const input = buildPipeline(rowQueryAst, this.#builderDelegate);
    try {
      const res = input.fetch({});
      for (const _ of res) {
        // if any row is returned at all, the
        // rule passes.
        return true;
      }
    } finally {
      input.destroy();
    }

    // no rows returned by any rules? The policy fails.
    return false;
  }
}

function updateWhere(where: Condition | undefined, policy: Policy) {
  assert(where, 'A where condition must exist for RowQuery');

  return dnf({
    type: 'and',
    conditions: [
      where,
      {type: 'or', conditions: policy.map(([, rule]) => rule)},
    ],
  });
}

type ActionOpMap = {
  insert: InsertOp;
  update: UpdateOp;
  delete: DeleteOp;
};
