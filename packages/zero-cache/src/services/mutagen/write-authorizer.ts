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
import type {Row} from '../../../../zero-protocol/src/data.js';
import type {
  InsertOp,
  DeleteOp,
  UpsertOp,
  UpdateOp,
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

export interface WriteAuthorizer {
  canInsert(authData: JWTPayload, op: InsertOp): boolean;
  canUpdate(authData: JWTPayload, op: UpdateOp): boolean;
  canDelete(authData: JWTPayload, op: DeleteOp): boolean;
  canUpsert(authData: JWTPayload, op: UpsertOp): boolean;
}

export class WriteAuthorizerImpl {
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
    authorization: AuthorizationConfig | undefined,
    replica: Database,
    cgID: string,
  ) {
    this.#lc = lc.withContext('class', 'WriteAuthorizerImpl');
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

  canInsert(authData: JWTPayload, op: InsertOp) {
    return this.#timedCanDo('insert', authData, op);
  }

  canUpdate(authData: JWTPayload, op: UpdateOp) {
    return this.#timedCanDo('update', authData, op);
  }

  canDelete(authData: JWTPayload, op: DeleteOp) {
    return this.#timedCanDo('delete', authData, op);
  }

  canUpsert(authData: JWTPayload, op: UpsertOp) {
    const preMutationRow = this.#getPreMutationRow(op);
    if (preMutationRow) {
      return this.canUpdate(authData, {
        op: 'update',
        tableName: op.tableName,
        primaryKey: op.primaryKey,
        value: op.value,
      });
    }

    return this.canInsert(authData, {
      op: 'insert',
      tableName: op.tableName,
      primaryKey: op.primaryKey,
      value: op.value,
    });
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
    action: A,
    authData: JWTPayload,
    op: ActionOpMap[A],
  ) {
    const start = performance.now();
    try {
      const ret = this.#canDo(action, authData, op);
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
    action: A,
    authData: JWTPayload,
    op: ActionOpMap[A],
  ) {
    const rules = this.#authorizationConfig[op.tableName];
    if (!rules) {
      return true;
    }

    let preMutationRow: Row | undefined;
    if (op.op !== 'insert') {
      preMutationRow = this.#getPreMutationRow(op);
    }

    const rowPolicies = rules.row;
    if (
      rowPolicies &&
      !this.#passesPolicy(rowPolicies[action], authData, preMutationRow)
    ) {
      return false;
    }

    const cellPolicies = rules.cell;
    if (cellPolicies) {
      for (const [column, policy] of Object.entries(cellPolicies)) {
        if (action === 'update' && op.value[column] === undefined) {
          // If the column is not being updated, we do not need to check
          // the column rules.
          continue;
        }
        if (!this.#passesPolicy(policy[action], authData, preMutationRow)) {
          return false;
        }
      }
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

  #passesPolicy(
    policy: Policy | undefined,
    authData: JWTPayload,
    preMutationRow: Row | undefined,
  ) {
    if (!policy) {
      return true;
    }

    for (const [_, rule] of policy) {
      const input = buildPipeline(rule, this.#builderDelegate, {
        authData: authData as Record<string, JSONValue>,
        preMutationRow,
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
    }

    // no rows returned by any rules? The policy fails.
    return false;
  }
}

type ActionOpMap = {
  insert: InsertOp;
  update: UpdateOp;
  delete: DeleteOp;
};
