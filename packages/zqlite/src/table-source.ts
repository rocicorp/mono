import type {SQLQuery} from '@databases/sql';
import type {LogContext} from '@rocicorp/logger';
import SQLite3Database from '@rocicorp/zero-sqlite3';
import type {LogConfig} from '../../otel/src/log-options.ts';
import {timeSampled} from '../../otel/src/maybe-time.ts';
import {assert, unreachable} from '../../shared/src/asserts.ts';
import {must} from '../../shared/src/must.ts';
import type {Writable} from '../../shared/src/writable.ts';
import type {Condition, Ordering} from '../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';
import type {
  SchemaValue,
  ValueType,
} from '../../zero-schema/src/table-schema.ts';
import type {DebugDelegate} from '../../zql/src/builder/debug-delegate.ts';
import {
  createPredicate,
  transformFilters,
} from '../../zql/src/builder/filter.ts';
import {makeComparator, type Node} from '../../zql/src/ivm/data.ts';
import {
  generateWithOverlay,
  generateWithStart,
  genPushAndWriteWithSplitEdit,
  type Connection,
  type Overlay,
} from '../../zql/src/ivm/memory-source.ts';
import {type FetchRequest} from '../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../zql/src/ivm/schema.ts';
import {
  type Source,
  type SourceChange,
  type SourceInput,
} from '../../zql/src/ivm/source.ts';
import type {Stream} from '../../zql/src/ivm/stream.ts';
import type {Database, Statement} from './db.ts';
import {compile, format, sql} from './internal/sql.ts';
import {StatementCache} from './internal/statement-cache.ts';
import {
  buildSelectQuery,
  toSQLiteType,
  type NoSubqueryCondition,
} from './query-builder.ts';
import {assertOrderingIncludesPK} from '../../zql/src/query/complete-ordering.ts';

type Statements = {
  readonly cache: StatementCache;
  readonly insert: Statement;
  readonly delete: Statement;
  readonly update: Statement | undefined;
  readonly checkExists: Statement;
  readonly getExisting: Statement;
};

let eventCount = 0;

/**
 * A source that is backed by a SQLite table.
 *
 * Values are written to the backing table _after_ being vended by the source.
 *
 * This ordering of events is to ensure self joins function properly. That is,
 * we can't reveal a value to an output before it has been pushed to that output.
 *
 * The code is fairly straightforward except for:
 * 1. Dealing with a `fetch` that has a basis of `before`.
 * 2. Dealing with compound orders that have differing directions (a ASC, b DESC, c ASC)
 *
 * See comments in relevant functions for more details.
 */
export class TableSource implements Source {
  readonly #dbCache = new WeakMap<Database, Statements>();
  readonly #connections: Connection[] = [];
  readonly #table: string;
  readonly #columns: Record<string, SchemaValue>;
  // Maps sorted columns JSON string (e.g. '["a","b"]) to Set of columns.
  readonly #uniqueIndexes: Map<string, Set<string>>;
  readonly #primaryKey: PrimaryKey;
  readonly #logConfig: LogConfig;
  readonly #lc: LogContext;
  readonly #shouldYield: () => boolean;
  #stmts: Statements;
  #overlay?: Overlay | undefined;
  #pushEpoch = 0;

  /**
   * @param shouldYield a function called after each row is read from the database,
   * which should return true if the source should yield the special 'yield' value
   * to yield control back to the caller at the end of the pipeline.  Can
   * also throw an error to abort the pipeline processing.
   */
  constructor(
    logContext: LogContext,
    logConfig: LogConfig,
    db: Database,
    tableName: string,
    columns: Record<string, SchemaValue>,
    primaryKey: PrimaryKey,
    shouldYield = () => false,
  ) {
    this.#lc = logContext;
    this.#logConfig = logConfig;
    this.#table = tableName;
    this.#columns = columns;
    this.#uniqueIndexes = getUniqueIndexes(db, tableName);
    this.#primaryKey = primaryKey;
    this.#stmts = this.#getStatementsFor(db);
    this.#shouldYield = shouldYield;

    assert(
      this.#uniqueIndexes.has(JSON.stringify([...primaryKey].sort())),
      `primary key ${primaryKey} does not have a UNIQUE index`,
    );
  }

  get tableSchema() {
    return {
      name: this.#table,
      columns: this.#columns,
      primaryKey: this.#primaryKey,
    };
  }

  /**
   * Sets the db (snapshot) to use, to facilitate the Snapshotter leapfrog
   * algorithm for concurrent traversal of historic timelines.
   */
  setDB(db: Database) {
    this.#stmts = this.#getStatementsFor(db);
  }

  #getStatementsFor(db: Database) {
    const cached = this.#dbCache.get(db);
    if (cached) {
      return cached;
    }

    const stmts = {
      cache: new StatementCache(db),
      insert: db.prepare(
        compile(
          sql`INSERT INTO ${sql.ident(this.#table)} (${sql.join(
            Object.keys(this.#columns).map(c => sql.ident(c)),
            ', ',
          )}) VALUES (${sql.__dangerous__rawValue(
            Array.from({length: Object.keys(this.#columns).length})
              .fill('?')
              .join(','),
          )})`,
        ),
      ),
      delete: db.prepare(
        compile(
          sql`DELETE FROM ${sql.ident(this.#table)} WHERE ${sql.join(
            this.#primaryKey.map(k => sql`${sql.ident(k)}=?`),
            ' AND ',
          )}`,
        ),
      ),
      // If all the columns are part of the primary key, we cannot use UPDATE.
      update:
        Object.keys(this.#columns).length > this.#primaryKey.length
          ? db.prepare(
              compile(
                sql`UPDATE ${sql.ident(this.#table)} SET ${sql.join(
                  nonPrimaryKeys(this.#columns, this.#primaryKey).map(
                    c => sql`${sql.ident(c)}=?`,
                  ),
                  ',',
                )} WHERE ${sql.join(
                  this.#primaryKey.map(k => sql`${sql.ident(k)}=?`),
                  ' AND ',
                )}`,
              ),
            )
          : undefined,
      checkExists: db.prepare(
        compile(
          sql`SELECT 1 AS "exists" FROM ${sql.ident(
            this.#table,
          )} WHERE ${sql.join(
            this.#primaryKey.map(k => sql`${sql.ident(k)}=?`),
            ' AND ',
          )} LIMIT 1`,
        ),
      ),
      getExisting: db.prepare(
        compile(
          sql`SELECT * FROM ${sql.ident(this.#table)} WHERE ${sql.join(
            this.#primaryKey.map(k => sql`${sql.ident(k)}=?`),
            ' AND ',
          )}`,
        ),
      ),
    };
    this.#dbCache.set(db, stmts);
    return stmts;
  }

  get #allColumns() {
    return sql.join(
      Object.keys(this.#columns).map(c => sql.ident(c)),
      sql`,`,
    );
  }

  #getSchema(connection: Connection): SourceSchema {
    return {
      tableName: this.#table,
      columns: this.#columns,
      primaryKey: this.#primaryKey,
      sort: connection.sort,
      relationships: {},
      isHidden: false,
      system: 'client',
      compareRows: connection.compareRows,
    };
  }

  connect(
    sort: Ordering,
    filters?: Condition,
    splitEditKeys?: Set<string>,
    debug?: DebugDelegate,
  ) {
    const transformedFilters = transformFilters(filters);
    const input: SourceInput = {
      getSchema: () => schema,
      fetch: req => this.#fetch(req, connection),
      setOutput: output => {
        connection.output = output;
      },
      destroy: () => {
        const idx = this.#connections.indexOf(connection);
        assert(idx !== -1, 'Connection not found');
        this.#connections.splice(idx, 1);
      },
      fullyAppliedFilters: !transformedFilters.conditionsRemoved,
    };

    const connection: Connection = {
      input,
      debug,
      output: undefined,
      sort,
      splitEditKeys,
      filters: transformedFilters.filters
        ? {
            condition: transformedFilters.filters,
            predicate: createPredicate(transformedFilters.filters),
          }
        : undefined,
      compareRows: makeComparator(sort),
      lastPushedEpoch: 0,
    };
    const schema = this.#getSchema(connection);
    assertOrderingIncludesPK(sort, this.#primaryKey);

    this.#connections.push(connection);
    return input;
  }

  toSQLiteRow(row: Row): Row {
    return Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        toSQLiteType(value, this.#columns[key].type),
      ]),
    ) as Row;
  }

  *#fetch(req: FetchRequest, connection: Connection): Stream<Node | 'yield'> {
    const {sort, debug} = connection;

    const query = this.#requestToSQL(req, connection.filters?.condition, sort);
    const sqlAndBindings = format(query);

    const cachedStatement = this.#stmts.cache.get(sqlAndBindings.text);
    try {
      cachedStatement.statement.safeIntegers(true);
      const rowIterator = cachedStatement.statement.iterate<Row>(
        ...sqlAndBindings.values,
      );

      const comparator = makeComparator(sort, req.reverse);

      debug?.initQuery(this.#table, sqlAndBindings.text);

      yield* generateWithStart(
        generateWithYields(
          generateWithOverlay(
            req.start?.row,
            this.#mapFromSQLiteTypes(
              this.#columns,
              rowIterator,
              sqlAndBindings.text,
              debug,
            ),
            req.constraint,
            this.#overlay,
            connection.lastPushedEpoch,
            comparator,
            connection.filters?.predicate,
          ),
          this.#shouldYield,
        ),
        req.start,
        comparator,
      );
    } finally {
      if (debug) {
        let totalNvisit = 0;
        let i = 0;
        while (true) {
          const nvisit = cachedStatement.statement.scanStatus(
            i++,
            SQLite3Database.SQLITE_SCANSTAT_NVISIT,
            1,
          );
          if (nvisit === undefined) {
            break;
          }
          totalNvisit += Number(nvisit);
        }
        if (totalNvisit !== 0) {
          debug.recordNVisit(this.#table, sqlAndBindings.text, totalNvisit);
        }
        cachedStatement.statement.scanStatusReset();
      }
      this.#stmts.cache.return(cachedStatement);
    }
  }

  *#mapFromSQLiteTypes(
    valueTypes: Record<string, SchemaValue>,
    rowIterator: IterableIterator<Row>,
    query: string,
    debug: DebugDelegate | undefined,
  ): IterableIterator<Row> {
    let result;
    try {
      do {
        result = timeSampled(
          this.#lc,
          ++eventCount,
          this.#logConfig.ivmSampling,
          () => rowIterator.next(),
          this.#logConfig.slowRowThreshold,
          () =>
            `table-source.next took too long for ${query}. Are you missing an index?`,
        );
        if (result.done) {
          break;
        }
        const row = fromSQLiteTypes(valueTypes, result.value, this.#table);
        debug?.rowVended(this.#table, query, row);
        yield row;
      } while (!result.done);
    } finally {
      rowIterator.return?.();
    }
  }

  *push(change: SourceChange): Stream<'yield'> {
    for (const result of this.genPush(change)) {
      if (result === 'yield') {
        yield result;
      }
    }
  }

  *genPush(change: SourceChange) {
    const exists = (row: Row) =>
      this.#stmts.checkExists.get<{exists: number} | undefined>(
        ...toSQLiteTypes(this.#primaryKey, row, this.#columns),
      )?.exists === 1;
    const setOverlay = (o: Overlay | undefined) => (this.#overlay = o);
    const writeChange = (c: SourceChange) => this.#writeChange(c);

    yield* genPushAndWriteWithSplitEdit(
      this.#connections,
      change,
      exists,
      setOverlay,
      writeChange,
      () => ++this.#pushEpoch,
    );
  }

  #writeChange(change: SourceChange) {
    switch (change.type) {
      case 'add':
        this.#stmts.insert.run(
          ...toSQLiteTypes(
            Object.keys(this.#columns),
            change.row,
            this.#columns,
          ),
        );
        break;
      case 'remove':
        this.#stmts.delete.run(
          ...toSQLiteTypes(this.#primaryKey, change.row, this.#columns),
        );
        break;
      case 'edit': {
        // If the PK is the same, use UPDATE.
        if (
          canUseUpdate(
            change.oldRow,
            change.row,
            this.#columns,
            this.#primaryKey,
          )
        ) {
          const mergedRow = {
            ...change.oldRow,
            ...change.row,
          };
          const params = [
            ...nonPrimaryValues(this.#columns, this.#primaryKey, mergedRow),
            ...toSQLiteTypes(this.#primaryKey, mergedRow, this.#columns),
          ];
          must(this.#stmts.update).run(params);
        } else {
          this.#stmts.delete.run(
            ...toSQLiteTypes(this.#primaryKey, change.oldRow, this.#columns),
          );
          this.#stmts.insert.run(
            ...toSQLiteTypes(
              Object.keys(this.#columns),
              change.row,
              this.#columns,
            ),
          );
        }

        break;
      }
      default:
        unreachable(change);
    }
  }

  #getRowStmtCache = new Map<string, string>();

  #getRowStmt(keyCols: string[]): string {
    const keyString = JSON.stringify(keyCols);
    let stmt = this.#getRowStmtCache.get(keyString);
    if (!stmt) {
      stmt = compile(
        sql`SELECT ${this.#allColumns} FROM ${sql.ident(
          this.#table,
        )} WHERE ${sql.join(
          keyCols.map(k => sql`${sql.ident(k)}=?`),
          sql` AND`,
        )}`,
      );
      this.#getRowStmtCache.set(keyString, stmt);
    }
    return stmt;
  }

  /**
   * Retrieves a row from the backing DB by a unique key, or `undefined` if such a
   * row does not exist. This is not used in the IVM pipeline but is useful
   * for retrieving data that is consistent with the state (and type
   * semantics) of the pipeline. Note that this key may not necessarily correspond
   * to the `primaryKey` with which this TableSource.
   */
  getRow(rowKey: Row): Row | undefined {
    const keyCols = Object.keys(rowKey);

    const stmt = this.#getRowStmt(keyCols);
    const row = this.#stmts.cache.use(stmt, cached =>
      cached.statement
        .safeIntegers(true)
        .get<Row>(...toSQLiteTypes(keyCols, rowKey, this.#columns)),
    );
    if (row) {
      return fromSQLiteTypes(this.#columns, row, this.#table);
    }
    return row;
  }

  #requestToSQL(
    request: FetchRequest,
    filters: NoSubqueryCondition | undefined,
    order: Ordering,
  ): SQLQuery {
    return buildSelectQuery(
      this.#table,
      this.#columns,
      request.constraint,
      filters,
      order,
      request.reverse,
      request.start,
    );
  }
}

function getUniqueIndexes(
  db: Database,
  tableName: string,
): Map<string, Set<string>> {
  const sqlAndBindings = format(
    sql`
    SELECT idx.name, json_group_array(col.name) as columnsJSON
      FROM sqlite_master as idx
      JOIN pragma_index_list(idx.tbl_name) AS info ON info.name = idx.name
      JOIN pragma_index_info(idx.name) as col
      WHERE idx.tbl_name = ${tableName} AND
            idx.type = 'index' AND 
            info."unique" != 0
      GROUP BY idx.name
      ORDER BY idx.name`,
  );
  const stmt = db.prepare(sqlAndBindings.text);
  const indexes = stmt.all<{columnsJSON: string}>(...sqlAndBindings.values);
  return new Map(
    indexes.map(({columnsJSON}) => {
      const columns = JSON.parse(columnsJSON);
      const set = new Set<string>(columns);
      return [JSON.stringify(columns.sort()), set];
    }),
  );
}

export function toSQLiteTypes(
  columns: readonly string[],
  row: Row,
  columnTypes: Record<string, SchemaValue>,
): readonly unknown[] {
  return columns.map(col => toSQLiteType(row[col], columnTypes[col].type));
}

export function toSQLiteTypeName(type: ValueType) {
  switch (type) {
    case 'boolean':
      return 'INTEGER';
    case 'number':
      return 'REAL';
    case 'string':
      return 'TEXT';
    case 'null':
      return 'NULL';
    case 'json':
      return 'TEXT';
  }
}

export function fromSQLiteTypes(
  valueTypes: Record<string, SchemaValue>,
  row: Row,
  tableName: string,
): Row {
  const newRow: Writable<Row> = {};
  for (const key of Object.keys(row)) {
    const valueType = valueTypes[key];
    if (valueType === undefined) {
      const columnList = Object.keys(valueTypes).sort().join(', ');
      throw new Error(
        `Invalid column "${key}" for table "${tableName}". Synced columns include ${columnList}`,
      );
    }
    newRow[key] = fromSQLiteType(valueType.type, row[key], key, tableName);
  }
  return newRow;
}

function fromSQLiteType(
  valueType: ValueType,
  v: Value,
  column: string,
  tableName: string,
): Value {
  if (v === null) {
    return null;
  }
  switch (valueType) {
    case 'boolean':
      return !!v;
    case 'number':
    case 'string':
    case 'null':
      if (typeof v === 'bigint') {
        if (v > Number.MAX_SAFE_INTEGER || v < Number.MIN_SAFE_INTEGER) {
          throw new UnsupportedValueError(
            `value ${v} (in ${tableName}.${column}) is outside of supported bounds`,
          );
        }
        return Number(v);
      }
      return v;
    case 'json':
      try {
        return JSON.parse(v as string);
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        throw new UnsupportedValueError(
          `Failed to parse JSON for ${tableName}.${column}: ${errorMessage}`,
          {cause: error},
        );
      }
  }
}

export class UnsupportedValueError extends Error {}

function canUseUpdate(
  oldRow: Row,
  row: Row,
  columns: Record<string, SchemaValue>,
  primaryKey: PrimaryKey,
): boolean {
  for (const pk of primaryKey) {
    if (oldRow[pk] !== row[pk]) {
      return false;
    }
  }
  return Object.keys(columns).length > primaryKey.length;
}

function nonPrimaryValues(
  columns: Record<string, SchemaValue>,
  primaryKey: PrimaryKey,
  row: Row,
): Iterable<unknown> {
  return nonPrimaryKeys(columns, primaryKey).map(c =>
    toSQLiteType(row[c], columns[c].type),
  );
}

function nonPrimaryKeys(
  columns: Record<string, SchemaValue>,
  primaryKey: PrimaryKey,
) {
  return Object.keys(columns).filter(c => !primaryKey.includes(c));
}

function* generateWithYields(stream: Stream<Node>, shouldYield: () => boolean) {
  for (const n of stream) {
    if (shouldYield()) {
      yield 'yield';
    }
    yield n;
  }
}
