import type {
  Ordering,
  OrderPart,
  Primitive,
  Selector,
} from 'zql/src/zql/ast/ast.js';
import {DifferenceStream} from 'zql/src/zql/ivm/graph/difference-stream.js';
import {
  createPullResponseMessage,
  PullMsg,
  Request,
} from 'zql/src/zql/ivm/graph/message.js';
import type {MaterialiteForSourceInternal} from 'zql/src/zql/ivm/materialite.js';
import type {Entry} from 'zql/src/zql/ivm/multiset.js';
import type {Source, SourceInternal} from 'zql/src/zql/ivm/source/source.js';
import type {PipelineEntity, Version} from 'zql/src/zql/ivm/types.js';
import {genMap, genCached} from 'zql/src/zql/util/iterables.js';
import type {Database, Statement} from 'better-sqlite3';
import type {HoistedCondition} from 'zql/src/zql/ivm/graph/message.js';
import type {HashIndex} from 'zql/src/zql/ivm/source/source-hash-index.js';
import {StatementCache} from './internal/statement-cache.js';
import {TableSourceHashIndex} from './table-source-hash-index.js';
import {mergeRequests} from 'zql/src/zql/ivm/source/set-source.js';
import {assert} from 'shared/src/asserts.js';
import {compile, sql} from './internal/sql.js';

const resolved = Promise.resolve();

// ID is only used for debugging.
let id = 0;

/**
 * An IVM source that is backed by a table in the database.
 *
 * When callers add or remove data from the source, downstream
 * IVM pipelines will be run.
 *
 * The source will also update the underlying database table with
 * the new data being added or removed.
 *
 * As of this commit, the source does not yet write to SQLite.
 */
export class TableSource<T extends PipelineEntity> implements Source<T> {
  readonly #stream: DifferenceStream<T>;
  readonly #internal: SourceInternal;
  readonly #name: string;
  readonly #materialite: MaterialiteForSourceInternal;
  readonly #db: Database;
  // The query used to get history varies with what downstream operators
  // request. We keep a cache to avoid preparing each unique request more than
  // once.
  readonly #historyStatements: StatementCache;
  readonly #historyRequests: Map<number, PullMsg> = new Map();
  readonly #insertStmt: Statement;
  readonly #deleteStmt: Statement;

  // Field for debugging.
  #id = id++;
  // Pending changes to be committed in the current transaction.
  #pending: Entry<T>[] = [];

  constructor(
    db: Database,
    materialite: MaterialiteForSourceInternal,
    name: string,
    columns: string[],
  ) {
    this.#materialite = materialite;
    this.#name = name;
    this.#stream = new DifferenceStream<T>();
    this.#stream.setUpstream({
      commit: () => {},
      messageUpstream: (message: Request) => {
        this.processMessage(message);
      },
      destroy: () => {},
    });
    this.#db = db;
    this.#historyStatements = new StatementCache(db);
    let str = compile(
      sql`INSERT INTO ${sql.ident(name)} (${sql.join(
        columns.map(c => sql.ident(c)),
        sql`, `,
      )}) VALUES (${sql.__dangerous__rawValue(
        new Array(columns.length).fill('?').join(', '),
      )})`,
    );
    this.#insertStmt = db.prepare(str);
    str = compile(
      sql`DELETE FROM ${sql.ident(name)} WHERE ${sql.ident('id')} = ?`,
    );
    this.#deleteStmt = db.prepare(str);

    this.#internal = {
      onCommitEnqueue: (version: Version) => {
        if (this.#pending.length === 0 && this.#historyRequests.size === 0) {
          return;
        }

        if (this.#historyRequests.size > 0) {
          assert(this.#pending.length === 0);
          for (const request of this.#historyRequests.values()) {
            this.#sendHistory(request);
          }
          this.#historyRequests.clear();
          return;
        }

        if (this.#pending.length !== 0) {
          this.#writeAndSendPending(version);
        }

        this.#pending = [];
      },
      onCommitted: (version: Version) => {
        this.#stream.commit(version);
      },
      onRollback: () => {
        this.#pending = [];
      },
    };
  }

  get stream(): DifferenceStream<T> {
    return this.#stream;
  }

  /**
   * This method is required so ZQL can work unchanged on the server.
   * This method will be replaced with `pull` in the future.
   */
  getOrCreateAndMaintainNewHashIndex<K extends Primitive>(
    column: Selector,
  ): HashIndex<K, T> {
    return new TableSourceHashIndex(this.#db, this.#name, column);
  }

  add(v: T): this {
    this.#pending.push([v, 1]);
    this.#materialite.addDirtySource(this.#internal);
    return this;
  }

  delete(v: T): this {
    this.#pending.push([v, -1]);
    this.#materialite.addDirtySource(this.#internal);
    return this;
  }

  processMessage(message: Request): void {
    switch (message.type) {
      case 'pull': {
        this.#materialite.addDirtySource(this.#internal);
        this.#historyRequests.set(
          message.id,
          mergeRequests(message, this.#historyRequests.get(message.id)),
        );
        break;
      }
    }
  }

  #sendHistory(msg: PullMsg): void {
    const {hoistedConditions} = msg;
    const conditionsForThisSource = hoistedConditions.filter(
      c => c.selector[0] === this.#name,
    );
    const sort = this.#getSort(msg);

    const sortedConditions = conditionsForThisSource
      .concat()
      .sort((a, b) =>
        a.selector[1] > b.selector[1]
          ? 1
          : a.selector[1] === b.selector[1]
          ? 0
          : -1,
      );
    const sql = conditionsAndSortToSQL(this.#name, sortedConditions, sort);

    const stmt = this.#historyStatements.get(sql);

    try {
      this.#stream.newDifference(
        this.#materialite.getVersion(),
        // cached since multiple downstreams may pull on the same iterator.
        // E.g., if the stream is forked.
        genCached(
          genMap(
            // using `iterate` allows us to enforce `limit` in the `view`
            // by having the `view` stop pulling.
            stmt.statement.iterate(...getConditionBindParams(sortedConditions)),
            v => [v, 1],
          ),
        ),
        createPullResponseMessage(msg, this.#name, sort),
      );
    } finally {
      this.#historyStatements.return(stmt);
    }
  }

  // TODO(mlaw): we'll need to optimize this.
  // We're essentially changing the `one at a time` case from this:
  // https://jsbm.dev/QeaEw5incvxQy
  // instead of doing `single Iterator`.
  #writeAndSendPending(version: Version): void {
    // do the SQLite writes for each item in pending.
    for (const entry of this.#pending) {
      if (entry[1] > 0) {
        // apply the insert
        this.#insertStmt.run(...Object.values(entry[0]));
      }

      // run through the pipeline
      this.#stream.newDifference(version, [entry], undefined);

      if (entry[1] < 0) {
        // apply the delete
        this.#deleteStmt.run(entry[0].id);
      }
    }
  }

  #getSort(msg: PullMsg): Ordering | undefined {
    // returns the set of fields we were able to sort by from the request.
    // undefined if none.
    if (msg.order === undefined) {
      return undefined;
    }

    const orderParts: OrderPart[] = [];
    for (const orderPart of msg.order) {
      const selector = orderPart[0];
      if (selector[0] === this.#name) {
        orderParts.push(orderPart);
      } else {
        break;
      }
    }

    if (orderParts.length === 0) {
      return undefined;
    }

    return orderParts;
  }

  seed(_: Iterable<T>): this {
    throw new Error('Should not be called for table-source');
  }

  isSeeded(): boolean {
    return true;
  }

  awaitSeeding(): PromiseLike<void> {
    return resolved;
  }

  toString(): string {
    return this.#name + ' ' + this.#id;
  }
}

/**
 * When receiving a `pull` request from downstream,
 * the source needs to convert that to SQL. This function
 * does this conversion.
 */
export function conditionsAndSortToSQL(
  table: string,
  conditions: HoistedCondition[],
  sort: Ordering | undefined,
) {
  let query = sql`SELECT * FROM ${sql.ident(table)}`;
  if (conditions.length > 0) {
    query = sql`${query} WHERE ${sql.join(
      conditions.map(c => {
        if (c.op === 'IN') {
          // we use `json_each` so we do not create a different number of bind params each time we see an `IN`
          return sql`${sql.ident(c.selector[1])} ${sql.__dangerous__rawValue(
            c.op,
          )} (SELECT value FROM json_each(?))`;
        } else if (c.op === 'ILIKE') {
          // The default configuration of SQLite only supports case-insensitive comparisons of ASCII characters
          return sql`${sql.ident(c.selector[1])} LIKE ?`;
        }
        return sql`${sql.ident(c.selector[1])} ${sql.__dangerous__rawValue(
          c.op,
        )} ?`;
      }),
      sql` AND `,
    )}`;
  }
  if (sort) {
    query = sql`${query} ORDER BY ${sql.join(
      sort.map(
        s => sql`${sql.ident(s[0][1])} ${sql.__dangerous__rawValue(s[1])}`,
      ),
      sql`, `,
    )}`;
  }

  return compile(query);
}

export function getConditionBindParams(conditions: HoistedCondition[]) {
  return conditions.map(c =>
    c.op === 'IN' ? JSON.stringify(c.value) : c.value,
  );
}
