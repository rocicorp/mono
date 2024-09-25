import {Database} from 'zqlite/src/db.js';
import {
  AuthorizationConfig,
  Policy,
  ZeroConfig,
} from '../../config/zero-config.js';
import {CreateOp, DeleteOp, SetOp, UpdateOp} from 'zero-protocol';
import {BuilderDelegate, buildPipeline} from 'zql/src/zql/builder/builder.js';
import {
  NormalizedTableSpec,
  normalize,
} from '../view-syncer/pipeline-driver.js';
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
import {Row} from 'zql/src/zql/ivm/data.js';

export class WriteAuthorizer {
  readonly #authorizationConfig: AuthorizationConfig;
  readonly #replica: Database;
  readonly #builderDelegate: BuilderDelegate;
  readonly #tableSpecs: Map<string, NormalizedTableSpec>;
  readonly #tables = new Map<string, TableSource>();
  readonly #statementCache: StatementCache;
  // readonly #compiledRules = new Map<unknown, unknown>();

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

  canInsert(op: CreateOp) {
    return this.#canDo('insert', op);
  }

  canUpdate(op: UpdateOp) {
    return this.#canDo('update', op);
  }

  canDelete(op: DeleteOp) {
    return this.#canDo('delete', op);
  }

  canUpsert(_op: SetOp) {
    // if exists, canUpdate
    // else canInsert
    return true;
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

  #canDo<A extends keyof ActionOpMap>(action: A, op: ActionOpMap[A]) {
    const rules = this.#authorizationConfig[op.entityType];
    if (!rules) {
      return true;
    }

    const tableRules = rules.table;
    if (tableRules && !this.#passesPolicy(tableRules[action], undefined)) {
      return false;
    }

    const columnRules = rules.column;
    if (columnRules) {
      for (const rule of Object.values(columnRules)) {
        if (!this.#passesPolicy(rule[action], undefined)) {
          return false;
        }
      }
    }

    let preMutationRow: Row | undefined;
    if (op.op !== 'create') {
      preMutationRow = this.#statementCache.use(
        compile(sql`SELECT * FROM ${sql.ident(op.entityType)} WHERE id = ?`),
        stmt => stmt.statement.get<Row>(op.id),
      );
    }

    const rowRules = rules.row;
    if (rowRules && !this.#passesPolicy(rowRules[action], preMutationRow)) {
      return false;
    }

    const cellRules = rules.cell;
    if (cellRules) {
      for (const rule of Object.values(cellRules)) {
        if (!this.#passesPolicy(rule[action], preMutationRow)) {
          return false;
        }
      }
    }

    return true;
  }

  #passesPolicy(policy: Policy | undefined, preMutationRow: Row | undefined) {
    if (!policy) {
      return true;
    }

    for (const [_, rule] of policy) {
      const input = buildPipeline(rule, this.#builderDelegate, {
        authData: {},
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
