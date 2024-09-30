import {Database} from 'zqlite/src/db.js';
import type {
  AuthorizationConfig,
  Policy,
  ZeroConfig,
} from '../../config/zero-config.js';
import type {CreateOp, DeleteOp, SetOp, UpdateOp} from 'zero-protocol';
import type {BuilderDelegate} from 'zql/src/zql/builder/builder.js';
import {buildPipeline} from 'zql/src/zql/builder/builder.js';
import type {NormalizedTableSpec} from '../view-syncer/pipeline-driver.js';
import {normalize} from '../view-syncer/pipeline-driver.js';
import {listTables} from '../../db/lite-tables.js';
import {TableSource} from 'zqlite/src/table-source.js';
import {assert} from 'shared/src/asserts.js';
import {mapLiteDataTypeToZqlSchemaValue} from '../../types/lite.js';
import {DatabaseStorage} from '../view-syncer/database-storage.js';
import {LogContext} from '@rocicorp/logger';
import path from 'path';
import {tmpdir} from 'node:os';
import {pid} from 'node:process';
import {randInt} from 'shared/src/rand.js';
import {StatementCache} from 'zqlite/src/internal/statement-cache.js';
import {sql, compile} from 'zqlite/src/internal/sql.js';
import type {Row} from 'zql/src/zql/ivm/data.js';
import type {JWTPayload} from 'jose';
import type {JSONValue} from 'shared/src/json.js';

export interface WriteAuthorizer {
  canInsert(authData: JWTPayload, op: CreateOp): boolean;
  canUpdate(authData: JWTPayload, op: UpdateOp): boolean;
  canDelete(authData: JWTPayload, op: DeleteOp): boolean;
  canUpsert(authData: JWTPayload, op: SetOp): boolean;
}

export class WriteAuthorizerImpl {
  readonly #authorizationConfig: AuthorizationConfig;
  readonly #replica: Database;
  readonly #builderDelegate: BuilderDelegate;
  readonly #tableSpecs: Map<string, NormalizedTableSpec>;
  readonly #tables = new Map<string, TableSource>();
  readonly #statementCache: StatementCache;

  constructor(
    lc: LogContext,
    config: ZeroConfig,
    replica: Database,
    cgID: string,
  ) {
    this.#authorizationConfig = config.authorization ?? {};
    this.#replica = replica;
    const tmpDir = config.storageDbTmpDir ?? tmpdir();
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
    this.#statementCache = new StatementCache(replica);
  }

  canInsert(authData: JWTPayload, op: CreateOp) {
    return this.#canDo('insert', authData, op);
  }

  canUpdate(authData: JWTPayload, op: UpdateOp) {
    return this.#canDo('update', authData, op);
  }

  canDelete(authData: JWTPayload, op: DeleteOp) {
    return this.#canDo('delete', authData, op);
  }

  canUpsert(authData: JWTPayload, op: SetOp) {
    const preMutationRow = this.#getPreMutationRow(op);
    if (preMutationRow) {
      return this.canUpdate(authData, {
        op: 'update',
        entityType: op.entityType,
        id: op.id,
        partialValue: op.value,
      });
    }

    return this.canInsert(authData, {
      op: 'create',
      entityType: op.entityType,
      id: op.id,
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
    const rules = this.#authorizationConfig[op.entityType];
    if (!rules) {
      return true;
    }

    const tableRules = rules.table;
    if (
      tableRules &&
      !this.#passesPolicy(tableRules[action], authData, undefined)
    ) {
      return false;
    }

    const columnRules = rules.column;
    if (columnRules) {
      for (const rule of Object.values(columnRules)) {
        if (!this.#passesPolicy(rule[action], authData, undefined)) {
          return false;
        }
      }
    }

    let preMutationRow: Row | undefined;
    if (op.op !== 'create') {
      preMutationRow = this.#getPreMutationRow(op);
    }

    const rowRules = rules.row;
    if (
      rowRules &&
      !this.#passesPolicy(rowRules[action], authData, preMutationRow)
    ) {
      return false;
    }

    const cellRules = rules.cell;
    if (cellRules) {
      for (const rule of Object.values(cellRules)) {
        if (!this.#passesPolicy(rule[action], authData, preMutationRow)) {
          return false;
        }
      }
    }

    return true;
  }

  #getPreMutationRow(op: SetOp | UpdateOp | DeleteOp) {
    return this.#statementCache.use(
      compile(sql`SELECT * FROM ${sql.ident(op.entityType)} WHERE id = ?`),
      stmt => stmt.statement.get<Row | undefined>(op.id.id),
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
  insert: CreateOp;
  update: UpdateOp;
  delete: DeleteOp;
};

export class WriteAuthorizationFailed extends Error {
  constructor(message: string) {
    super(message);
  }
}
