import type {SQLQuery} from '@databases/sql';
import {LogContext} from '@rocicorp/logger';
import type {JWTPayload} from 'jose';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {pid} from 'node:process';
import {assert} from '../../../../shared/src/asserts.js';
import type {JSONValue} from '../../../../shared/src/json.js';
import {randInt} from '../../../../shared/src/rand.js';
import * as v from '../../../../shared/src/valita.js';
import type {
  InsertOp,
  DeleteOp,
  UpsertOp,
  UpdateOp,
  CRUDOp,
} from '../../../../zero-protocol/src/mod.js';
import {
  primaryKeyValueSchema,
  type PrimaryKeyValue,
} from '../../../../zero-protocol/src/primary-key.js';
import type {BuilderDelegate} from '../../../../zql/src/builder/builder.js';
import {buildPipeline} from '../../../../zql/src/builder/builder.js';
import {Database} from '../../../../zqlite/src/db.js';
import {compile, sql} from '../../../../zqlite/src/internal/sql.js';
import {TableSource} from '../../../../zqlite/src/table-source.js';
import type {ZeroConfig} from '../../config/zero-config.js';
import {listTables} from '../../db/lite-tables.js';
import {mapLiteDataTypeToZqlSchemaValue} from '../../types/lite.js';
import {DatabaseStorage} from '../view-syncer/database-storage.js';
import type {NormalizedTableSpec} from '../view-syncer/pipeline-driver.js';
import {normalize} from '../view-syncer/pipeline-driver.js';
import type {
  AuthorizationConfig,
  Policy,
} from '../../../../zero-schema/src/compiled-authorization.js';
import {StatementRunner} from '../../db/statements.js';
import type {Schema} from '../../../../zero-schema/src/schema.js';
import {AuthQuery, authQuery} from '../../../../zql/src/query/auth-query.js';
import {must} from '../../../../shared/src/must.js';
import type {Query} from '../../../../zql/src/query/query.js';
import type {TableSchema} from '../../../../zero-schema/src/table-schema.js';
import type {Condition} from '../../../../zero-protocol/src/ast.js';
import {dnf} from '../../../../zql/src/query/dnf.js';

type Phase = 'preMutation' | 'postMutation';

export interface WriteAuthorizer {
  canPreMutation(
    authData: JWTPayload,
    ops: Exclude<CRUDOp, UpsertOp>[],
  ): boolean;
  canPostMutation(
    authData: JWTPayload,
    ops: Exclude<CRUDOp, UpsertOp>[],
  ): boolean;
  normalizeOps(ops: CRUDOp[]): Exclude<CRUDOp, UpsertOp>[];
}

export class WriteAuthorizerImpl implements WriteAuthorizer {
  readonly #schema: Schema;
  readonly #authorizationConfig: AuthorizationConfig;
  readonly #replica: Database;
  readonly #builderDelegate: BuilderDelegate;
  readonly #tableSpecs: Map<string, NormalizedTableSpec>;
  readonly #tables = new Map<string, TableSource>();
  readonly #statementRunner: StatementRunner;
  readonly #lc: LogContext;

  constructor(
    lc: LogContext,
    config: Pick<ZeroConfig, 'storageDBTmpDir'>,
    schema: Schema,
    authorization: AuthorizationConfig | undefined,
    replica: Database,
    cgID: string,
  ) {
    this.#lc = lc.withContext('class', 'WriteAuthorizerImpl');
    this.#schema = schema;
    this.#authorizationConfig = authorization ?? {};
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
    this.#tableSpecs = new Map(
      listTables(replica).map(spec => [spec.name, normalize(spec)]),
    );
    this.#statementRunner = new StatementRunner(replica);
  }

  canPreMutation(authData: JWTPayload, ops: Exclude<CRUDOp, UpsertOp>[]) {
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

  canPostMutation(authData: JWTPayload, ops: Exclude<CRUDOp, UpsertOp>[]) {
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
              oldRow: this.#getPreMutationRow(op),
              row: op.value,
            });
            break;
          }
          case 'delete': {
            source.push({
              type: 'remove',
              row: this.#getPreMutationRow(op),
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

  #canInsert(phase: Phase, authData: JWTPayload, op: InsertOp) {
    return this.#timedCanDo(phase, 'insert', authData, op);
  }

  #canUpdate(phase: Phase, authData: JWTPayload, op: UpdateOp) {
    return this.#timedCanDo(phase, 'update', authData, op);
  }

  #canDelete(phase: Phase, authData: JWTPayload, op: DeleteOp) {
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
    const {columns, primaryKey} = tableSpec;
    assert(primaryKey.length);
    source = new TableSource(
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
    authData: JWTPayload,
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
    authData: JWTPayload,
    op: ActionOpMap[A],
  ) {
    const rules = this.#authorizationConfig[op.tableName];
    if (rules?.row === undefined && rules?.cell === undefined) {
      return true;
    }

    const rowPolicies = rules.row;
    let rowQuery = authQuery(
      must(
        this.#schema.tables[op.tableName],
        'No schema found for table ' + op.tableName,
      ),
    );
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
            applicableRowPolicy = rowPolicies.update.postProposedMutation;
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
            if (
              phase === 'postMutation' &&
              policy.update?.postProposedMutation
            ) {
              applicableCellPolicies.push(policy.update.postProposedMutation);
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

    return this.#statementRunner.get(
      compile(
        sql`SELECT * FROM ${sql.ident(op.tableName)} WHERE ${sql.join(
          conditions,
          sql` AND `,
        )}`,
      ),
      ...values,
    );
  }

  #passesPolicyGroup(
    applicableRowPolicy: Policy | undefined,
    applicableCellPolicies: Policy[],
    authData: JWTPayload,
    rowQuery: Query<TableSchema>,
  ) {
    if (
      applicableRowPolicy === undefined &&
      applicableCellPolicies.length === 0
    ) {
      return true;
    }

    if (
      applicableRowPolicy &&
      !this.#passesPolicy(applicableRowPolicy, authData, rowQuery)
    ) {
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
    policy: Policy,
    authData: JWTPayload,
    rowQuery: Query<TableSchema>,
  ) {
    let rowQueryAst = (rowQuery as AuthQuery<TableSchema>).ast;
    rowQueryAst = {
      ...rowQueryAst,
      where: updateWhere(rowQueryAst.where, policy),
    };

    const input = buildPipeline(rowQueryAst, this.#builderDelegate, {
      authData: authData as Record<string, JSONValue>,
      // TODO: We can remove this arg now that the pre-mutation row is got by the
      // rowQueryAst.
      preMutationRow: undefined,
    });
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
