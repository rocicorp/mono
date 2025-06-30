import type {SQLQuery} from '@databases/sql';
import type {LogContext} from '@rocicorp/logger';
import type {LogConfig} from '../../otel/src/log-options.ts';
import {timeSampled} from '../../otel/src/maybe-time.ts';
import {assert, unreachable} from '../../shared/src/asserts.ts';
import {must} from '../../shared/src/must.ts';
import {difference} from '../../shared/src/set-utils.ts';
import type {Writable} from '../../shared/src/writable.ts';
import type {
  Condition,
  Ordering,
  SimpleCondition,
  ValuePosition,
} from '../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';
import type {
  SchemaValue,
  ValueType,
} from '../../zero-schema/src/table-schema.ts';
import {
  createPredicate,
  transformFilters,
  type NoSubqueryCondition,
} from '../../zql/src/builder/filter.ts';
import {makeComparator, type Node} from '../../zql/src/ivm/data.ts';
import {
  generateWithOverlay,
  generateWithStart,
  genPush,
  type Connection,
  type Overlay,
} from '../../zql/src/ivm/memory-source.ts';
import type {FetchRequest, Start} from '../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../zql/src/ivm/schema.ts';
import type {
  Source,
  SourceChange,
  SourceChangeSet,
  SourceInput,
} from '../../zql/src/ivm/source.ts';
import type {Stream} from '../../zql/src/ivm/stream.ts';
import {Database, Statement} from './db.ts';
import {compile, format, sql} from './internal/sql.ts';
import {StatementCache} from './internal/statement-cache.ts';
import {runtimeDebugStats} from './runtime-debug.ts';

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
  readonly #clientGroupID: string;
  readonly #logConfig: LogConfig;
  readonly #lc: LogContext;
  #stmts: Statements;
  #overlay?: Overlay | undefined;
  #splitEditOverlay?: Overlay | undefined;

  constructor(
    logContext: LogContext,
    logConfig: LogConfig,
    clientGroupID: string,
    db: Database,
    tableName: string,
    columns: Record<string, SchemaValue>,
    primaryKey: readonly [string, ...string[]],
  ) {
    this.#lc = logContext;
    this.#logConfig = logConfig;
    this.#clientGroupID = clientGroupID;
    this.#table = tableName;
    this.#columns = columns;
    this.#uniqueIndexes = getUniqueIndexes(db, tableName);
    this.#primaryKey = primaryKey;
    this.#stmts = this.#getStatementsFor(db);

    assert(
      this.#uniqueIndexes.has(JSON.stringify([...primaryKey].sort())),
      `primary key ${primaryKey} does not have a UNIQUE index`,
    );
  }

  get table() {
    return this.#table;
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
            new Array(Object.keys(this.#columns).length).fill('?').join(','),
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
      primaryKey: this.#selectPrimaryKeyFor(connection.sort),
      sort: connection.sort,
      relationships: {},
      isHidden: false,
      system: 'client',
      compareRows: connection.compareRows,
    };
  }

  connect(
    sort: Ordering,
    filters?: Condition | undefined,
    splitEditKeys?: Set<string> | undefined,
  ) {
    const transformedFilters = transformFilters(filters);
    const input: SourceInput = {
      getSchema: () => schema,
      fetch: req => this.#fetch(req, connection),
      cleanup: req => this.#cleanup(req, connection),
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
    };
    const schema = this.#getSchema(connection);

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

  #cleanup(req: FetchRequest, connection: Connection): Stream<Node> {
    return this.#fetch(req, connection);
  }

  *#fetch(req: FetchRequest, connection: Connection): Stream<Node> {
    const {sort} = connection;

    const query = this.#requestToSQL(req, connection.filters?.condition, sort);
    const sqlAndBindings = format(query);

    const cachedStatement = this.#stmts.cache.get(sqlAndBindings.text);
    try {
      cachedStatement.statement.safeIntegers(true);
      const rowIterator = cachedStatement.statement.iterate<Row>(
        ...sqlAndBindings.values,
      );

      const callingConnectionIndex = this.#connections.indexOf(connection);
      assert(callingConnectionIndex !== -1, 'Connection not found');

      const comparator = makeComparator(sort, req.reverse);

      runtimeDebugStats.initQuery(
        this.#clientGroupID,
        this.#table,
        sqlAndBindings.text,
      );

      yield* generateWithStart(
        generateWithOverlay(
          req.start?.row,
          this.#mapFromSQLiteTypes(
            this.#columns,
            rowIterator,
            sqlAndBindings.text,
          ),
          req.constraint,
          this.#overlay,
          this.#splitEditOverlay,
          callingConnectionIndex,
          comparator,
          connection.filters?.predicate,
        ),
        req.start,
        comparator,
      );
    } finally {
      this.#stmts.cache.return(cachedStatement);
    }
  }

  *#mapFromSQLiteTypes(
    valueTypes: Record<string, SchemaValue>,
    rowIterator: IterableIterator<Row>,
    query: string,
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
        const row = fromSQLiteTypes(valueTypes, result.value);
        runtimeDebugStats.rowVended(
          this.#clientGroupID,
          this.#table,
          query,
          row,
        );
        yield row;
      } while (!result.done);
    } finally {
      rowIterator.return?.();
    }
  }

  push(change: SourceChange | SourceChangeSet): void {
    for (const _ of this.genPush(change)) {
      // Nothing to do.
    }
  }

  *genPush(change: SourceChange | SourceChangeSet) {
    const exists = (row: Row) =>
      this.#stmts.checkExists.get<{exists: number} | undefined>(
        ...toSQLiteTypes(this.#primaryKey, row, this.#columns),
      )?.exists === 1;
    const setOverlay = (o: Overlay | undefined) => (this.#overlay = o);
    const setSplitEditOverlay = (o: Overlay | undefined) =>
      (this.#splitEditOverlay = o);

    if (change.type === 'set') {
      const existing = this.#stmts.getExisting.get<Row | undefined>(
        ...toSQLiteTypes(this.#primaryKey, change.row, this.#columns),
      );
      if (existing !== undefined) {
        change = {
          type: 'edit',
          oldRow: existing,
          row: change.row,
        };
      } else {
        change = {
          type: 'add',
          row: change.row,
        };
      }
    }

    for (const x of genPush(
      change,
      exists,
      this.#connections.entries(),
      setOverlay,
      setSplitEditOverlay,
    )) {
      yield x;
    }

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
    const keyVals = Object.values(rowKey);

    const stmt = this.#getRowStmt(keyCols);
    const row = this.#stmts.cache.use(stmt, cached =>
      cached.statement.safeIntegers(true).get<Row>(keyVals),
    );
    if (row) {
      return fromSQLiteTypes(this.#columns, row);
    }
    return row;
  }

  #requestToSQL(
    request: FetchRequest,
    filters: NoSubqueryCondition | undefined,
    order: Ordering,
  ): SQLQuery {
    const {constraint, start, reverse} = request;
    let query = sql`SELECT ${this.#allColumns} FROM ${sql.ident(this.#table)}`;
    const constraints: SQLQuery[] = [];

    if (constraint) {
      for (const [key, value] of Object.entries(constraint)) {
        constraints.push(
          sql`${sql.ident(key)} = ${toSQLiteType(
            value,
            this.#columns[key].type,
          )}`,
        );
      }
    }

    if (start) {
      constraints.push(
        gatherStartConstraints(start, reverse, order, this.#columns),
      );
    }

    if (filters) {
      constraints.push(filtersToSQL(filters));
    }

    if (constraints.length > 0) {
      query = sql`${query} WHERE ${sql.join(constraints, sql` AND `)}`;
    }

    if (reverse) {
      query = sql`${query} ORDER BY ${sql.join(
        order.map(
          s =>
            sql`${sql.ident(s[0])} ${sql.__dangerous__rawValue(
              s[1] === 'asc' ? 'desc' : 'asc',
            )}`,
        ),
        sql`, `,
      )}`;
    } else {
      query = sql`${query} ORDER BY ${sql.join(
        order.map(
          s => sql`${sql.ident(s[0])} ${sql.__dangerous__rawValue(s[1])}`,
        ),
        sql`, `,
      )}`;
    }

    return query;
  }

  #selectPrimaryKeyFor(sort: Ordering) {
    const columns = new Set(sort.map(([col]) => col));
    for (const uniqueColumns of this.#uniqueIndexes.values()) {
      if (difference(uniqueColumns, columns).size === 0) {
        assert(uniqueColumns.size > 0);
        return [...uniqueColumns] as unknown as PrimaryKey;
      }
    }
    throw new Error(
      `Cannot orderBy(${JSON.stringify(sort.map(([c]) => c))}). ` +
        (sort.length === 1
          ? `The column must be unique. `
          : `One or more columns must form a unique index. `) +
        `Did you forget to include a primary key or ` +
        `(non-null) unique index on the "${this.#table}" table?`,
    );
  }
}

/**
 * This applies all filters present in the AST for a query to the source.
 * This will work until subquery filters are added
 * at which point either:
 * a. we move filters to connect
 * b. we do the transform of removing subquery filters from filters while
 *    preserving the meaning of the filters.
 *
 * https://www.notion.so/replicache/Optional-Filters-OR-1303bed895458013a26ee5aafd5725d2
 */
export function filtersToSQL(filters: NoSubqueryCondition): SQLQuery {
  switch (filters.type) {
    case 'simple':
      return simpleConditionToSQL(filters);
    case 'and':
      return filters.conditions.length > 0
        ? sql`(${sql.join(
            filters.conditions.map(condition => filtersToSQL(condition)),
            sql` AND `,
          )})`
        : sql`TRUE`;
    case 'or':
      return filters.conditions.length > 0
        ? sql`(${sql.join(
            filters.conditions.map(condition => filtersToSQL(condition)),
            sql` OR `,
          )})`
        : sql`FALSE`;
  }
}

function simpleConditionToSQL(filter: SimpleCondition): SQLQuery {
  const {op} = filter;
  if (op === 'IN' || op === 'NOT IN') {
    switch (filter.right.type) {
      case 'literal':
        return sql`${valuePositionToSQL(
          filter.left,
        )} ${sql.__dangerous__rawValue(
          filter.op,
        )} (SELECT value FROM json_each(${JSON.stringify(
          filter.right.value,
        )}))`;
      case 'static':
        throw new Error(
          'Static parameters must be replaced before conversion to SQL',
        );
    }
  }
  return sql`${valuePositionToSQL(filter.left)} ${sql.__dangerous__rawValue(
    filter.op === 'ILIKE'
      ? 'LIKE'
      : filter.op === 'NOT ILIKE'
        ? 'NOT LIKE'
        : filter.op,
  )} ${valuePositionToSQL(filter.right)}`;
}

function valuePositionToSQL(value: ValuePosition): SQLQuery {
  switch (value.type) {
    case 'column':
      return sql.ident(value.name);
    case 'literal':
      return sql`${toSQLiteType(value.value, getJsType(value.value))}`;
    case 'static':
      throw new Error(
        'Static parameters must be replaced before conversion to SQL',
      );
  }
}

function getJsType(value: unknown): ValueType {
  if (value === null) {
    return 'null';
  }
  return typeof value === 'string'
    ? 'string'
    : typeof value === 'number'
      ? 'number'
      : typeof value === 'boolean'
        ? 'boolean'
        : 'json';
}

/**
 * The ordering could be complex such as:
 * `ORDER BY a ASC, b DESC, c ASC`
 *
 * In those cases, we need to encode the constraints as various
 * `OR` clauses.
 *
 * E.g.,
 *
 * to get the row after (a = 1, b = 2, c = 3) would be:
 *
 * `WHERE a > 1 OR (a = 1 AND b < 2) OR (a = 1 AND b = 2 AND c > 3)`
 *
 * - after vs before flips the comparison operators.
 * - inclusive adds a final `OR` clause for the exact match.
 */
function gatherStartConstraints(
  start: Start,
  reverse: boolean | undefined,
  order: Ordering,
  columnTypes: Record<string, SchemaValue>,
): SQLQuery {
  const constraints: SQLQuery[] = [];
  const {row: from, basis} = start;

  for (let i = 0; i < order.length; i++) {
    const group: SQLQuery[] = [];
    const [iField, iDirection] = order[i];
    for (let j = 0; j <= i; j++) {
      if (j === i) {
        if (iDirection === 'asc') {
          if (!reverse) {
            group.push(
              sql`${sql.ident(iField)} > ${toSQLiteType(
                from[iField],
                columnTypes[iField].type,
              )}`,
            );
          } else {
            reverse satisfies true;
            group.push(
              sql`${sql.ident(iField)} < ${toSQLiteType(
                from[iField],
                columnTypes[iField].type,
              )}`,
            );
          }
        } else {
          iDirection satisfies 'desc';
          if (!reverse) {
            group.push(
              sql`${sql.ident(iField)} < ${toSQLiteType(
                from[iField],
                columnTypes[iField].type,
              )}`,
            );
          } else {
            reverse satisfies true;
            group.push(
              sql`${sql.ident(iField)} > ${toSQLiteType(
                from[iField],
                columnTypes[iField].type,
              )}`,
            );
          }
        }
      } else {
        const [jField] = order[j];
        group.push(
          sql`${sql.ident(jField)} = ${toSQLiteType(
            from[jField],
            columnTypes[jField].type,
          )}`,
        );
      }
    }
    constraints.push(sql`(${sql.join(group, sql` AND `)})`);
  }

  if (basis === 'at') {
    constraints.push(
      sql`(${sql.join(
        order.map(
          s =>
            sql`${sql.ident(s[0])} = ${toSQLiteType(
              from[s[0]],
              columnTypes[s[0]].type,
            )}`,
        ),
        sql` AND `,
      )})`,
    );
  }

  return sql`(${sql.join(constraints, sql` OR `)})`;
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

function toSQLiteType(v: unknown, type: ValueType): unknown {
  switch (type) {
    case 'boolean':
      return v === null ? null : v ? 1 : 0;
    case 'number':
    case 'string':
    case 'null':
      return v;
    case 'json':
      return JSON.stringify(v);
  }
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
): Row {
  const newRow: Writable<Row> = {};
  for (const key of Object.keys(row)) {
    const valueType = valueTypes[key];
    if (valueType === undefined) {
      throw new Error(
        `Invalid column "${key}". Synced columns include ${Object.keys(
          valueTypes,
        ).sort()}`,
      );
    }
    newRow[key] = fromSQLiteType(valueType.type, row[key], key);
  }
  return newRow;
}

function fromSQLiteType(valueType: ValueType, v: Value, column: string): Value {
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
            `value ${v} (in column ${column}) is outside of supported bounds`,
          );
        }
        return Number(v);
      }
      return v;
    case 'json':
      return JSON.parse(v as string);
  }
}

export class UnsupportedValueError extends Error {
  constructor(msg: string) {
    super(msg);
  }
}

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
