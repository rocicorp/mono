import type {
  Input,
  Output,
  Schema,
  FetchRequest,
  HydrateRequest,
  Constraint,
} from 'zql/src/zql/ivm2/operator.js';
import type {SourceChange} from 'zql/src/zql/ivm2/memory-source.js';
import type {Ordering} from 'zql/src/zql/ast2/ast.js';
import {Node, Row, makeComparator} from 'zql/src/zql/ivm2/data.js';
import {Database, Statement} from 'better-sqlite3';
import {compile, format, sql} from '../internal/sql.js';
import {Stream} from 'zql/src/zql/ivm2/stream.js';
import {SQLQuery} from '@databases/sql';
import {assert} from 'shared/src/asserts.js';
import {StatementCache} from '../internal/statement-cache.js';

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
export class TableSource implements Input {
  readonly #outputs: Output[] = [];
  readonly #insertStmt: Statement;
  readonly #deleteStmt: Statement;
  readonly #order: Ordering;
  readonly #table: string;
  readonly #schema: Schema;
  readonly #statementCache: StatementCache;

  constructor(
    db: Database,
    tableName: string,
    columns: readonly string[],
    order: Ordering,
    primaryKeys: readonly string[],
  ) {
    this.#order = makeOrderUnique(order, primaryKeys);
    this.#schema = {
      compareRows: makeComparator(this.#order),
    };
    this.#table = tableName;
    this.#statementCache = new StatementCache(db);

    assertPrimaryKeysMatch(db, tableName, primaryKeys);

    this.#insertStmt = db.prepare(
      compile(
        sql`INSERT INTO ${sql.ident(tableName)} (${sql.join(
          columns.map(c => sql.ident(c)),
          sql`, `,
        )}) VALUES (${sql.__dangerous__rawValue(
          new Array(columns.length).fill('?').join(', '),
        )})`,
      ),
    );

    this.#deleteStmt = db.prepare(
      // TODO: we need to know the columns which comprise the primary key. Defaulting to `id` for now.
      compile(
        sql`DELETE FROM ${sql.ident(tableName)} WHERE ${sql.ident('id')} = ?`,
      ),
    );
  }

  schema(): Schema {
    return this.#schema;
  }

  addOutput(output: Output): void {
    this.#outputs.push(output);
  }

  hydrate(req: HydrateRequest, output: Output) {
    return this.fetch(req, output);
  }

  *fetch(req: FetchRequest, output: Output): Stream<Node> {
    const {start} = req;
    let newReq = req;

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
        this.#order,
      );
      const sqlAndBindings = format(preSql);

      newReq = {...req, start: undefined};
      this.#statementCache.use(sqlAndBindings.text, cachedStatement => {
        for (const beforeRow of cachedStatement.statement.iterate(
          ...sqlAndBindings.values,
        )) {
          newReq.start = {row: beforeRow, basis: 'at'};
          break;
        }
      });

      yield* this.fetch(newReq, output);
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
        this.#order,
      );
      const sqlAndBindings = format(query);

      const cachedStatement = this.#statementCache.get(sqlAndBindings.text);
      try {
        const rowIterator = cachedStatement.statement.iterate(
          ...sqlAndBindings.values,
        );
        for (const row of rowIterator) {
          yield {row, relationships: new Map()};
        }
      } finally {
        this.#statementCache.return(cachedStatement);
      }
    }
  }

  push(change: SourceChange) {
    for (const output of this.#outputs) {
      output.push(
        {
          type: change.type,
          node: {
            row: change.row,
            relationships: new Map(),
          },
        },
        this,
      );
    }
    if (change.type === 'add') {
      this.#insertStmt.run(...Object.values(change.row));
    } else {
      assert(change.type === 'remove');
      this.#deleteStmt.run(change.row.id);
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
