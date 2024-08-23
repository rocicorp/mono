import type {
  Output,
  FetchRequest,
  Constraint,
  Input,
} from 'zql/src/zql/ivm2/operator.js';
import {Schema, ValueType} from 'zql/src/zql/ivm2/schema.js';
import {
  generateWithStart,
  generateWithOverlay,
  type Overlay,
} from 'zql/src/zql/ivm2/memory-source.js';
import type {Ordering} from 'zql/src/zql/ast2/ast.js';
import {Node, Row, Value, makeComparator} from 'zql/src/zql/ivm2/data.js';
import {Database, Statement} from 'better-sqlite3';
import {compile, format, sql} from '../internal/sql.js';
import type {Stream} from 'zql/src/zql/ivm2/stream.js';
import type {Source, SourceChange} from 'zql/src/zql/ivm2/source.js';
import type {SQLQuery} from '@databases/sql';
import {assert} from 'shared/src/asserts.js';
import {StatementCache} from '../internal/statement-cache.js';
import {Connector} from 'zql/src/zql/ivm2/connector.js';

type OutputRegistration = {
  output: Output;
  sort: Ordering;
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
  readonly #outputs: OutputRegistration[] = [];
  readonly #insertStmt: Statement;
  readonly #deleteStmt: Statement;
  readonly #table: string;
  readonly #columns: Record<string, ValueType>;
  readonly #primaryKey: readonly string[];
  readonly #statementCache: StatementCache;
  readonly #checkExistsStmt: Statement;
  #overlay?: Overlay | undefined;

  constructor(
    db: Database,
    tableName: string,
    columns: Record<string, ValueType>,
    primaryKey: readonly [string, ...string[]],
  ) {
    this.#table = tableName;
    this.#columns = columns;
    this.#primaryKey = primaryKey;
    this.#statementCache = new StatementCache(db);

    assertPrimaryKeysMatch(db, tableName, primaryKey);

    this.#insertStmt = db.prepare(
      compile(
        sql`INSERT INTO ${sql.ident(tableName)} (${sql.join(
          Object.keys(columns).map(c => sql.ident(c)),
          sql`, `,
        )}) VALUES (${sql.__dangerous__rawValue(
          new Array(Object.keys(columns).length).fill('?').join(', '),
        )})`,
      ),
    );

    this.#deleteStmt = db.prepare(
      compile(
        sql`DELETE FROM ${sql.ident(tableName)} WHERE ${sql.join(
          primaryKey.map(k => sql`${sql.ident(k)} = ?`),
          sql` AND `,
        )}`,
      ),
    );

    this.#checkExistsStmt = db.prepare(
      compile(
        sql`SELECT 1 AS "exists" FROM ${sql.ident(tableName)} WHERE ${sql.join(
          primaryKey.map(k => sql`${sql.ident(k)} = ?`),
          sql` AND `,
        )} LIMIT 1`,
      ),
    );
  }

  #getRegistrationForOutput(output: Output): OutputRegistration {
    const reg = this.#outputs.find(r => r.output === output);
    assert(reg, 'Output not found');
    return reg;
  }

  #getSchema(output: Output): Schema {
    const reg = this.#getRegistrationForOutput(output);
    return {
      columns: this.#columns,
      primaryKey: this.#primaryKey,
      compareRows: makeComparator(reg.sort),
    };
  }

  #input: Input = {
    getSchema: output => this.#getSchema(output),
    fetch: (req, output) => this.#fetch(req, output),
    cleanup: (req, output) => this.#cleanup(req, output),
    setOutput: output => this.#setOutput(output),
  };

  connect(sort: Ordering) {
    const connector = new Connector(this.#input);
    this.#outputs.push({
      output: connector,
      sort: makeOrderUnique(sort, this.#primaryKey),
    });
    return connector;
  }

  #cleanup(req: FetchRequest, output: Output): Stream<Node> {
    return this.#fetch(req, output);
  }

  #setOutput(_: Output) {
    // does nothing, MemorySource uses connect() instead.
  }

  *#fetch(
    req: FetchRequest,
    output: Output,
    beforeRequest?: FetchRequest | undefined,
  ): Stream<Node> {
    const {start} = req;
    let newReq = req;
    const reg = this.#getRegistrationForOutput(output);
    const {sort} = reg;

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
        sort,
      );
      const sqlAndBindings = format(preSql);

      newReq = {...req, start: undefined};
      this.#statementCache.use(sqlAndBindings.text, cachedStatement => {
        for (const beforeRow of cachedStatement.statement.iterate(
          ...sqlAndBindings.values.map(v => toSQLiteType(v)),
        )) {
          newReq.start = {row: beforeRow, basis: 'at'};
          break;
        }
      });

      yield* this.#fetch(newReq, output, req);
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
        sort,
      );
      const sqlAndBindings = format(query);

      const cachedStatement = this.#statementCache.get(sqlAndBindings.text);
      try {
        const rowIterator = cachedStatement.statement.iterate(
          ...sqlAndBindings.values.map(v => toSQLiteType(v)),
        );

        const callingOutputIndex = this.#outputs.findIndex(
          reg => reg.output === output,
        );
        assert(callingOutputIndex !== -1, 'Output not found');

        const reg = this.#outputs[callingOutputIndex];
        const {sort} = reg;
        const comparator = makeComparator(sort);

        let overlay: Overlay | undefined;
        if (this.#overlay) {
          if (callingOutputIndex <= this.#overlay.outputIndex) {
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
        this.#statementCache.return(cachedStatement);
      }
    }
  }

  push(change: SourceChange) {
    // need to check for the existence of the row before modifying
    // the db so we don't push it to outputs if it does/doest not exist.
    const exists =
      this.#checkExistsStmt.get(...pickColumns(this.#primaryKey, change.row))
        ?.exists === 1;
    if (change.type === 'add') {
      assert(!exists, 'Row already exists');
    } else {
      assert(exists, 'Row not found');
    }

    for (const [outputIndex, {output}] of this.#outputs.entries()) {
      this.#overlay = {outputIndex, change};
      output.push(
        {
          type: change.type,
          node: {
            row: change.row,
            relationships: {},
          },
        },
        this.#input,
      );
    }
    this.#overlay = undefined;
    if (change.type === 'add') {
      this.#insertStmt.run(
        ...toSQLiteTypes(Object.keys(this.#columns), change.row),
      );
    } else {
      change.type satisfies 'remove';
      this.#deleteStmt.run(...toSQLiteTypes(this.#primaryKey, change.row));
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

function assertPrimaryKeysMatch(
  db: Database,
  tableName: string,
  primaryKeys: readonly string[],
) {
  const sqlAndBindings = format(
    sql`SELECT name FROM pragma_table_info(${tableName}) WHERE pk > 0`,
  );
  const stmt = db.prepare(sqlAndBindings.text);
  const pkColumns = new Set(
    stmt.all(...sqlAndBindings.values).map(row => row.name),
  );

  assert(pkColumns.size === primaryKeys.length);

  for (const key of primaryKeys) {
    assert(pkColumns.has(key));
  }
}

function makeOrderUnique(
  order: Ordering,
  primaryKeys: readonly string[],
): Ordering {
  const uniqueOrder = [...order];
  for (const key of primaryKeys) {
    if (!order.some(([k]) => k === key)) {
      uniqueOrder.push([key, 'asc']);
    }
  }
  return uniqueOrder;
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
  valueTypes: Record<string, ValueType>,
  rowIterator: IterableIterator<Row>,
): IterableIterator<Row> {
  for (const row of rowIterator) {
    fromSQLiteTypes(valueTypes, row);
    yield row;
  }
}

function fromSQLiteTypes(valueTypes: Record<string, ValueType>, row: Row) {
  for (const key in row) {
    row[key] = fromSQLiteType(valueTypes[key], row[key]);
  }
}

function fromSQLiteType(valueType: ValueType, v: Value): Value {
  switch (valueType) {
    case 'boolean':
      return v === 0 ? false : v === 1 ? true : v;
    default:
      return v;
  }
}
