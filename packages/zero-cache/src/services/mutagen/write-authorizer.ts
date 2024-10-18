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
  CreateOp,
  DeleteOp,
  SetOp,
  UpdateOp,
} from '../../../../zero-protocol/src/mod.js';
import {
  primaryKeyValueSchema,
  type PrimaryKeyValue,
} from '../../../../zero-protocol/src/primary-key.js';
import type {BuilderDelegate} from '../../../../zql/src/zql/builder/builder.js';
import {buildPipeline} from '../../../../zql/src/zql/builder/builder.js';
import {MissingParameterError} from '../../../../zql/src/zql/builder/error.js';
import {Database} from '../../../../zqlite/src/db.js';
import {compile, sql} from '../../../../zqlite/src/internal/sql.js';
import {StatementCache} from '../../../../zqlite/src/internal/statement-cache.js';
import {TableSource} from '../../../../zqlite/src/table-source.js';
import type {
  AuthorizationConfig,
  Policy,
  ZeroConfig,
} from '../../config/zero-config.js';
import {listTables} from '../../db/lite-tables.js';
import {mapLiteDataTypeToZqlSchemaValue} from '../../types/lite.js';
import {DatabaseStorage} from '../view-syncer/database-storage.js';
import type {NormalizedTableSpec} from '../view-syncer/pipeline-driver.js';
import {normalize} from '../view-syncer/pipeline-driver.js';

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
  readonly #lc: LogContext;

  constructor(
    lc: LogContext,
    config: ZeroConfig,
    replica: Database,
    cgID: string,
  ) {
    this.#lc = lc.withContext('class', 'WriteAuthorizerImpl');
    this.#authorizationConfig = config.authorization ?? {};
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
    this.#statementCache = new StatementCache(replica);
  }

  canInsert(authData: JWTPayload, op: CreateOp) {
    return this.#timedCanDo('insert', authData, op);
  }

  canUpdate(authData: JWTPayload, op: UpdateOp) {
    return this.#timedCanDo('update', authData, op);
  }

  canDelete(authData: JWTPayload, op: DeleteOp) {
    return this.#timedCanDo('delete', authData, op);
  }

  canUpsert(authData: JWTPayload, op: SetOp) {
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
      op: 'create',
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
    const {value} = op;
    const conditions: SQLQuery[] = [];
    const values: PrimaryKeyValue[] = [];
    for (const pk of op.primaryKey) {
      conditions.push(sql`${sql.ident(pk)}=?`);
      values.push(v.parse(value[pk], primaryKeyValueSchema));
    }

    return this.#statementCache.use(
      compile(
        sql`SELECT * FROM ${sql.ident(op.tableName)} WHERE ${sql.join(
          conditions,
          sql` AND `,
        )}`,
      ),
      stmt => stmt.statement.get<Row | undefined>(...values),
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
      try {
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
      } catch (e) {
        if (!(e instanceof MissingParameterError)) {
          throw e;
        }

        // Authorization rules may refer to parameters
        // that are missing due to the current login context not having
        // those values. E.g., referring to a claim that user does not have.
        // In that case, the rule is not applicable. This is ok since
        // all rules can only `allow` or `skip`. If we supported
        // `deny` rules this would not work.
        //
        // The way to support `deny` would be to allow `null` and `undefined`
        // into ZQL pipelines. Right now `where` clauses cannot take `null`
        // or `undefined` values for two reasons:
        // 1. We do not have `IS` and `IS NOT` operators in ZQL yet.
        // 2. Our predicates do not compare `NULL` and `UNDEFINED` the same way SQL would.
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
