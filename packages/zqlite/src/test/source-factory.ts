import type {LogContext} from '@rocicorp/logger';
import type {LogConfig} from '../../../otel/src/log-options.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {
  mapAST,
  type AST,
  type CompoundKey,
} from '../../../zero-protocol/src/ast.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import {
  clientToServer,
  serverToClient,
} from '../../../zero-schema/src/name-mapper.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {Source} from '../../../zql/src/ivm/source.ts';
import type {SourceFactory} from '../../../zql/src/ivm/test/source-factory.ts';
import {QueryDelegateBase} from '../../../zql/src/query/query-delegate-base.ts';
import type {QueryDelegate} from '../../../zql/src/query/query-delegate.ts';
import {Database} from '../db.ts';
import {compile, sql} from '../internal/sql.ts';
import {TableSource, toSQLiteTypeName} from '../table-source.ts';
import {CREATE_STORAGE_TABLE, DatabaseStorage} from '../database-storage.ts';
import type {Storage} from '../../../zql/src/ivm/operator.ts';

export const createSource: SourceFactory = (
  lc: LogContext,
  logConfig: LogConfig,
  tableName: string,
  columns: Record<string, SchemaValue>,
  primaryKey: PrimaryKey,
): Source => {
  const db = new Database(createSilentLogContext(), ':memory:');
  // create a table with desired columns and primary keys
  const query = compile(
    sql`CREATE TABLE ${sql.ident(tableName)} (${sql.join(
      Object.keys(columns).map(c => sql.ident(c)),
      sql`, `,
    )}, PRIMARY KEY (${sql.join(
      primaryKey.map(p => sql.ident(p)),
      sql`, `,
    )}));`,
  );
  db.exec(query);
  return new TableSource(lc, logConfig, db, tableName, columns, primaryKey);
};

export function mapResultToClientNames<T, S extends Schema>(
  result: unknown,
  schema: S,
  rootTable: keyof S['tables'] & string,
): T {
  const serverToClientMapper = serverToClient(schema.tables);
  const clientToServerMapper = clientToServer(schema.tables);

  function mapResult(result: unknown, schema: Schema, rootTable: string) {
    // oxlint-disable-next-line eqeqeq
    if (result == null) {
      return result;
    }

    if (Array.isArray(result)) {
      return result.map(r => mapResultToClientNames(r, schema, rootTable)) as T;
    }

    const mappedResult: Record<string, unknown> = {};
    const serverTableName = clientToServerMapper.tableName(rootTable);
    for (const [serverCol, v] of Object.entries(result)) {
      if (serverCol === '_0_version') {
        continue;
      }

      try {
        const clientCol = serverToClientMapper.columnName(
          serverTableName,
          serverCol,
        );
        mappedResult[clientCol] = v;
      } catch (_e) {
        const relationship = schema.relationships[rootTable][serverCol];
        mappedResult[serverCol] = mapResult(
          v,
          schema,
          (relationship[1] ?? relationship[0]).destSchema,
        );
      }
    }

    return mappedResult as T;
  }

  return mapResult(result, schema, rootTable) as T;
}

class SourceFactoryQueryDelegate extends QueryDelegateBase {
  readonly defaultQueryComplete = true;
  readonly enableNotExists = true;

  readonly #sources = new Map<string, Source>();
  readonly #clientToServerMapper: ReturnType<typeof clientToServer>;
  readonly #serverToClientMapper: ReturnType<typeof serverToClient>;
  readonly #lc: LogContext;
  readonly #logConfig: LogConfig;
  readonly #db: Database;
  readonly #schema: Schema;
  readonly #cgs;
  readonly #shouldYield: () => boolean;

  constructor(
    lc: LogContext,
    logConfig: LogConfig,
    db: Database,
    schema: Schema,
    shouldYield: () => boolean = () => false,
  ) {
    super();
    this.#lc = lc;
    const dbs = new Database(lc, ':memory:');
    dbs.prepare(CREATE_STORAGE_TABLE).run();
    const s = new DatabaseStorage(dbs);
    this.#cgs = s.createClientGroupStorage('');
    this.#logConfig = logConfig;
    this.#db = db;
    this.#schema = schema;
    this.#clientToServerMapper = clientToServer(schema.tables);
    this.#serverToClientMapper = serverToClient(schema.tables);
    this.#shouldYield = shouldYield;
  }

  override createStorage(): Storage {
    return this.#cgs.createStorage();
  }

  override getSource(serverTableName: string): Source {
    const clientTableName =
      this.#serverToClientMapper.tableName(serverTableName);
    let source = this.#sources.get(serverTableName);
    if (source) {
      return source;
    }

    const tables = this.#schema.tables;
    const tableSchema = tables[clientTableName as keyof typeof tables];

    // create the SQLite table
    this.#db.exec(`
      CREATE TABLE IF NOT EXISTS "${serverTableName}" (
        ${Object.entries(tableSchema.columns)
          .map(
            ([name, c]) =>
              `"${this.#clientToServerMapper.columnName(
                clientTableName,
                name,
              )}" ${toSQLiteTypeName(c.type)}`,
          )
          .join(', ')},
        PRIMARY KEY (${tableSchema.primaryKey
          .map(
            k =>
              `"${this.#clientToServerMapper.columnName(clientTableName, k)}"`,
          )
          .join(', ')})
      )`);

    source = new TableSource(
      this.#lc,
      this.#logConfig,
      this.#db,
      serverTableName,
      Object.fromEntries(
        Object.entries(tableSchema.columns).map(([k, v]) => [
          this.#clientToServerMapper.columnName(clientTableName, k),
          v,
        ]),
      ),
      tableSchema.primaryKey.map(k =>
        this.#clientToServerMapper.columnName(clientTableName, k),
      ) as unknown as CompoundKey,
      this.#shouldYield,
    );

    this.#sources.set(serverTableName, source);
    return source;
  }

  mapAst(ast: AST): AST {
    return mapAST(ast, this.#clientToServerMapper);
  }
}

export function newQueryDelegate(
  lc: LogContext,
  logConfig: LogConfig,
  db: Database,
  schema: Schema,
  shouldYield: () => boolean = () => false,
): QueryDelegate {
  return new SourceFactoryQueryDelegate(lc, logConfig, db, schema, shouldYield);
}
