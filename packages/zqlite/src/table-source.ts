import type {SQLQuery} from '@databases/sql';
import {Database, Statement} from 'better-sqlite3';
import {assert} from 'shared/src/asserts.js';
import type {Ordering, SimpleCondition} from 'zql/src/zql/ast/ast.js';
import {assertOrderingIncludesPK} from 'zql/src/zql/builder/builder.js';
import {
  Comparator,
  Node,
  Row,
  Value,
  makeComparator,
} from 'zql/src/zql/ivm/data.js';
import {
  generateWithOverlay,
  generateWithStart,
  type Overlay,
} from 'zql/src/zql/ivm/memory-source.js';
import type {
  Constraint,
  FetchRequest,
  Input,
  Output,
} from 'zql/src/zql/ivm/operator.js';
import {
  PrimaryKey,
  Schema,
  SchemaValue,
  ValueType,
} from 'zql/src/zql/ivm/schema.js';
import type {
  Source,
  SourceChange,
  SourceInput,
} from 'zql/src/zql/ivm/source.js';
import {Stream} from 'zql/src/zql/ivm/stream.js';
import {compile, format, sql} from './internal/sql.js';
import {StatementCache} from './internal/statement-cache.js';

type Connection = {
  input: Input;
  output: Output | undefined;
  sort: Ordering;
  filters: SimpleCondition[];
  compareRows: Comparator;
};

type Statements = {
  readonly cache: StatementCache;
  readonly insert: Statement;
  readonly delete: Statement;
  readonly checkExists: Statement;
};

/**
 * A source that is backed by a SQLite table.
 *
 * Values are written to the backing table _after_ being vended by the source.
 * An overlay index (not yet implemented) is used such that
 * `fetches` made after a `push` will see the new values.
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
  readonly #primaryKey: PrimaryKey;
  #stmts: Statements;
  #overlay?: Overlay | undefined;

  constructor(
    db: Database,
    tableName: string,
    columns: Record<string, SchemaValue>,
    primaryKey: readonly [string, ...string[]],
  ) {
    this.#table = tableName;
    this.#columns = columns;
    this.#primaryKey = primaryKey;
    this.#stmts = this.#getStatementsFor(db);
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
    assertPrimaryKeyMatch(db, this.#table, this.#primaryKey);

    const stmts = {
      cache: new StatementCache(db),
      insert: db.prepare(
        compile(
          sql`INSERT INTO ${sql.ident(this.#table)} (${sql.join(
            Object.keys(this.#columns).map(c => sql.ident(c)),
            sql`, `,
          )}) VALUES (${sql.__dangerous__rawValue(
            new Array(Object.keys(this.#columns).length).fill('?').join(', '),
          )})`,
        ),
      ),
      delete: db.prepare(
        compile(
          sql`DELETE FROM ${sql.ident(this.#table)} WHERE ${sql.join(
            this.#primaryKey.map(k => sql`${sql.ident(k)} = ?`),
            sql` AND `,
          )}`,
        ),
      ),
      checkExists: db.prepare(
        compile(
          sql`SELECT 1 AS "exists" FROM ${sql.ident(
            this.#table,
          )} WHERE ${sql.join(
            this.#primaryKey.map(k => sql`${sql.ident(k)} = ?`),
            sql` AND `,
          )} LIMIT 1`,
        ),
      ),
    };
    this.#dbCache.set(db, stmts);
    return stmts;
  }

  #getSchema(connection: Connection): Schema {
    return {
      tableName: this.#table,
      columns: this.#columns,
      primaryKey: this.#primaryKey,
      sort: connection.sort,
      relationships: {},
      isHidden: false,
      compareRows: connection.compareRows,
    };
  }

  connect(sort: Ordering, optionalFilters?: SimpleCondition[] | undefined) {
    const input: SourceInput = {
      getSchema: () => this.#getSchema(connection),
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
      appliedFilters: true,
    };

    const connection: Connection = {
      input,
      output: undefined,
      sort,
      filters: optionalFilters ?? [],
      compareRows: makeComparator(sort),
    };
    assertOrderingIncludesPK(sort, this.#primaryKey);

    this.#connections.push(connection);
    return input;
  }

  #cleanup(req: FetchRequest, connection: Connection): Stream<Node> {
    return this.#fetch(req, connection);
  }

  *#fetch(
    req: FetchRequest,
    connection: Connection,
    beforeRequest?: FetchRequest | undefined,
  ): Stream<Node> {
    const {start} = req;
    let newReq = req;
    const {sort} = connection;

    /**
     * Before isn't quite "before".
     * It means to fetch all values in the current order but starting at the row
     * _just before_ a given row.
     *
     * If we have values [1,2,3,4] and we say `fetch starting before 3` we should get back
     * `[2,3,4]` not `[1,2]`.
     *
     * To handle this, we convert `before` to `at` and re-invoke the fetch.
     */
    if (start?.basis === 'before') {
      assert(
        beforeRequest === undefined,
        'Before should only be converted once.',
      );
      const preSql = requestToSQL(
        this.#table,
        req.constraint,
        req.start !== undefined
          ? {
              from: req.start.row,
              direction: req.start.basis === 'before' ? 'before' : 'after',
              inclusive: req.start.basis === 'at',
            }
          : undefined,
        connection.filters,
        sort,
      );
      const sqlAndBindings = format(preSql);

      newReq = {...req, start: undefined};
      this.#stmts.cache.use(sqlAndBindings.text, cachedStatement => {
        for (const beforeRow of cachedStatement.statement.iterate(
          ...sqlAndBindings.values.map(v => toSQLiteType(v)),
        )) {
          newReq.start = {row: beforeRow, basis: 'at'};
          break;
        }
      });

      yield* this.#fetch(newReq, connection, req);
    } else {
      const query = requestToSQL(
        this.#table,
        req.constraint,
        req.start !== undefined
          ? {
              from: req.start.row,
              direction: req.start.basis === 'before' ? 'before' : 'after',
              inclusive: req.start.basis === 'at',
            }
          : undefined,
        connection.filters,
        sort,
      );
      const sqlAndBindings = format(query);

      const cachedStatement = this.#stmts.cache.get(sqlAndBindings.text);
      try {
        cachedStatement.statement.safeIntegers(true);
        const rowIterator = cachedStatement.statement.iterate(
          ...sqlAndBindings.values.map(v => toSQLiteType(v)),
        );

        const callingConnectionIndex = this.#connections.indexOf(connection);
        assert(callingConnectionIndex !== -1, 'Connection not found');

        const comparator = makeComparator(sort);

        let overlay: Overlay | undefined;
        if (this.#overlay) {
          if (callingConnectionIndex <= this.#overlay.outputIndex) {
            overlay = this.#overlay;
          }
        }

        yield* generateWithStart(
          generateWithOverlay(
            req.start?.row,
            mapFromSQLiteTypes(this.#columns, rowIterator),
            req.constraint,
            overlay,
            comparator,
          ),
          beforeRequest ?? req,
          comparator,
        );
      } finally {
        this.#stmts.cache.return(cachedStatement);
      }
    }
  }

  push(change: SourceChange) {
    // need to check for the existence of the row before modifying
    // the db so we don't push it to outputs if it does/doest not exist.
    const exists =
      this.#stmts.checkExists.get(...pickColumns(this.#primaryKey, change.row))
        ?.exists === 1;
    if (change.type === 'add') {
      assert(!exists, 'Row already exists');
    } else {
      assert(exists, 'Row not found');
    }

    // Outputs should see converted types (e.g. boolean).
    fromSQLiteTypes(this.#columns, change.row);
    for (const [outputIndex, {output}] of this.#connections.entries()) {
      this.#overlay = {outputIndex, change};
      if (output) {
        output.push({
          type: change.type,
          node: {
            row: change.row,
            relationships: {},
          },
        });
      }
    }
    this.#overlay = undefined;
    if (change.type === 'add') {
      this.#stmts.insert.run(
        ...toSQLiteTypes(Object.keys(this.#columns), change.row),
      );
    } else {
      change.type satisfies 'remove';
      this.#stmts.delete.run(...toSQLiteTypes(this.#primaryKey, change.row));
    }
  }
}

type Cursor = {
  from: Row;
  direction: 'before' | 'after';
  inclusive: boolean;
};

function requestToSQL(
  table: string,
  constraint: Constraint | undefined,
  cursor: Cursor | undefined,
  filters: SimpleCondition[],
  order: Ordering,
): SQLQuery {
  let query = sql`SELECT * FROM ${sql.ident(table)}`;
  const constraints: SQLQuery[] = [];

  if (constraint) {
    constraints.push(sql`${sql.ident(constraint.key)} = ${constraint.value}`);
  }

  if (cursor) {
    constraints.push(gatherStartConstraints(cursor, order));
  }

  for (const filter of filters) {
    const {op} = filter;
    if (op === 'IN' || op === 'NOT IN') {
      constraints.push(
        sql`${sql.ident(filter.field)} ${sql.__dangerous__rawValue(
          filter.op,
        )} (SELECT value FROM json_each(${JSON.stringify(filter.value)}))`,
      );
    } else {
      constraints.push(
        sql`${sql.ident(filter.field)} ${sql.__dangerous__rawValue(
          filter.op === 'ILIKE'
            ? 'LIKE'
            : filter.op === 'NOT ILIKE'
            ? 'NOT LIKE'
            : filter.op,
        )} ${filter.value}`,
      );
    }
  }

  if (constraints.length > 0) {
    query = sql`${query} WHERE ${sql.join(constraints, sql` AND `)}`;
  }

  if (cursor?.direction === 'before') {
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
function gatherStartConstraints(cursor: Cursor, order: Ordering): SQLQuery {
  const constraints: SQLQuery[] = [];
  const {from, direction, inclusive} = cursor;

  for (let i = 0; i < order.length; i++) {
    const group: SQLQuery[] = [];
    const [iField, iDirection] = order[i];
    for (let j = 0; j <= i; j++) {
      if (j === i) {
        if (iDirection === 'asc') {
          if (direction === 'after') {
            group.push(sql`${sql.ident(iField)} > ${from[iField]}`);
          } else {
            direction satisfies 'before';
            group.push(sql`${sql.ident(iField)} < ${from[iField]}`);
          }
        } else {
          iDirection satisfies 'desc';
          if (direction === 'after') {
            group.push(sql`${sql.ident(iField)} < ${from[iField]}`);
          } else {
            direction satisfies 'before';
            group.push(sql`${sql.ident(iField)} > ${from[iField]}`);
          }
        }
      } else {
        const [jField] = order[j];
        group.push(sql`${sql.ident(jField)} = ${from[jField]}`);
      }
    }
    constraints.push(sql`(${sql.join(group, sql` AND `)})`);
  }

  if (inclusive) {
    constraints.push(
      sql`(${sql.join(
        order.map(s => sql`${sql.ident(s[0])} = ${from[s[0]]}`),
        sql` AND `,
      )})`,
    );
  }

  return sql`(${sql.join(constraints, sql` OR `)})`;
}

function assertPrimaryKeyMatch(
  db: Database,
  tableName: string,
  primaryKey: PrimaryKey,
) {
  const sqlAndBindings = format(
    sql`SELECT name FROM pragma_table_info(${tableName}) WHERE pk > 0`,
  );
  const stmt = db.prepare(sqlAndBindings.text);
  const pkColumns = new Set(
    stmt.all(...sqlAndBindings.values).map(row => row.name),
  );

  assert(pkColumns.size === primaryKey.length);

  for (const key of primaryKey) {
    assert(pkColumns.has(key));
  }
}

function toSQLiteTypes(
  columns: readonly string[],
  row: Row,
): readonly unknown[] {
  return columns.map(col => toSQLiteType(row[col]));
}

function pickColumns(columns: readonly string[], row: Row): readonly Value[] {
  return columns.map(col => row[col]);
}

function toSQLiteType(v: unknown): unknown {
  return v === false ? 0 : v === true ? 1 : v ?? null;
}

function* mapFromSQLiteTypes(
  valueTypes: Record<string, SchemaValue>,
  rowIterator: IterableIterator<Row>,
): IterableIterator<Row> {
  for (const row of rowIterator) {
    fromSQLiteTypes(valueTypes, row);
    yield row;
  }
}

function fromSQLiteTypes(valueTypes: Record<string, SchemaValue>, row: Row) {
  for (const key in row) {
    row[key] = fromSQLiteType(valueTypes[key].type, row[key]);
  }
}

function fromSQLiteType(valueType: ValueType, v: Value): Value {
  switch (valueType) {
    case 'boolean':
      return !!v;
    default:
      if (typeof v === 'bigint') {
        if (v > Number.MAX_SAFE_INTEGER || v < Number.MIN_SAFE_INTEGER) {
          throw new UnsupportedValueError(
            `value ${v} is outside of supported bounds`,
          );
        }
        return Number(v);
      }
      return v;
  }
}

export class UnsupportedValueError extends Error {
  constructor(msg: string) {
    super(msg);
  }
}
