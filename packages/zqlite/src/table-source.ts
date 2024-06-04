import type {
  Ordering,
  Primitive,
  Selector,
} from '@rocicorp/zql/src/zql/ast/ast.js';
import {DifferenceStream} from '@rocicorp/zql/src/zql/ivm/graph/difference-stream.js';
import {
  createPullResponseMessage,
  PullMsg,
  Request,
} from '@rocicorp/zql/src/zql/ivm/graph/message.js';
import type {MaterialiteForSourceInternal} from '@rocicorp/zql/src/zql/ivm/materialite.js';
import type {Entry} from '@rocicorp/zql/src/zql/ivm/multiset.js';
import type {HashIndex} from '@rocicorp/zql/src/zql/ivm/source/source-hash-index.js';
import type {
  Source,
  SourceInternal,
} from '@rocicorp/zql/src/zql/ivm/source/source.js';
import type {PipelineEntity, Version} from '@rocicorp/zql/src/zql/ivm/types.js';
import {genMap, genCached} from '@rocicorp/zql/src/zql/util/iterables.js';
import type {Statement} from 'better-sqlite3';
import type {DB} from './internal/DB.js';
import {TableSourceHashIndex} from './table-source-hash-index.js';
import {conditionsAndSortToSQL, getConditionBindParams} from './util/sql.js';

const resolved = Promise.resolve();

// Write in a TX on `commitEnqueued` event.
let id = 0;
export class TableSource<T extends PipelineEntity> implements Source<T> {
  readonly #stream: DifferenceStream<T>;
  readonly #internal: SourceInternal;
  readonly #name: string;
  readonly #materialite: MaterialiteForSourceInternal;
  readonly #db: DB;
  readonly #cols: string[];
  #id = id++;
  #pending: Entry<T>[] = [];

  constructor(
    db: DB,
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

    this.#internal = {
      onCommitEnqueue: (_v: Version) => {
        // fk checks must be _off_
        if (this.#pending.length === 0) {
          return;
        }
        insertOrDeleteTx(this.#pending, insertStmt, deleteStmt);
      },
      onCommitted: (version: Version) => {
        this.#stream.commit(version);
      },
      onRollback: () => {
        this.#pending = [];
      },
    };

    const sortedCols = columns.concat().sort();
    const insertSQL = `INSERT INTO "${name}" (${sortedCols
      .map(c => `"${c}"`)
      .join(', ')}) VALUES (${sortedCols
      .map(() => '?')
      .join(', ')}) ON CONFLICT DO UPDATE SET ${sortedCols
      .map(c => `"${c}" = excluded."${c}"`)
      .join(', ')}`;
    const deleteSQL = `DELETE FROM "${name}" WHERE id = ?`;

    const insertOrDeleteTx = this.#db.transaction(this.#insertOrDelete);
    const insertStmt = this.#db.getStmt(insertSQL);
    const deleteStmt = this.#db.getStmt(deleteSQL);

    this.#cols = sortedCols;
  }

  #insertOrDelete = (
    pending: Entry<T>[],
    insertStmt: Statement,
    deleteStmt: Statement,
  ) => {
    for (const [v, delta] of pending) {
      if (delta > 0) {
        insertStmt.run(...this.#cols.map(c => v[c]));
      } else if (delta < 0) {
        deleteStmt.run(v.id);
      }
    }
  };

  get stream(): DifferenceStream<T> {
    return this.#stream;
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
        this.#sendHistory(message);
        break;
      }
    }
  }

  #sendHistory(msg: PullMsg): void {
    const hoistedConditions = msg?.hoistedConditions;
    const conditionsForThisSource = (hoistedConditions || []).filter(
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
    const stmt = this.#db.getStmt(sql);

    try {
      this.#stream.newDifference(
        this.#materialite.getVersion(),
        // cached since multiple downstreams may pull on the same iterator.
        // E.g., if the stream is forked.
        genCached(
          genMap(
            // using `iterate` allows us to enforce `limit` in the `view`
            // by having the `view` stop pulling.
            stmt.iterate(...getConditionBindParams(sortedConditions)),
            v => [v, 1],
          ),
        ),
        createPullResponseMessage(msg, this.#name, sort),
      );
    } finally {
      this.#db.returnStmt(sql);
    }
  }

  #getSort(msg: PullMsg): Ordering | undefined {
    // returns the set of fields we were able to sort by from the request.
    // undefined if none.
    if (msg.order === undefined) {
      return undefined;
    }

    const selectors: Selector[] = [];
    for (const selector of msg.order[0]) {
      if (selector[0] === this.#name) {
        selectors.push(selector);
      } else {
        break;
      }
    }

    if (selectors.length === 0) {
      return undefined;
    }

    return [selectors, msg.order[1]];
  }

  getOrCreateAndMaintainNewHashIndex<K extends Primitive>(
    column: Selector,
  ): HashIndex<K, T> {
    return new TableSourceHashIndex(this.#db, this.#name, column);
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
