import {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import {listTables} from 'zero-cache/src/db/lite-tables.js';
import {mapLiteDataTypeToZqlSchemaValue} from 'zero-cache/src/types/lite.js';
import {RowKey} from 'zero-cache/src/types/row-key.js';
import {TableSpec} from 'zero-cache/src/types/specs.js';
import {AST} from 'zql/src/zql/ast/ast.js';
import {buildPipeline} from 'zql/src/zql/builder/builder.js';
import {Change} from 'zql/src/zql/ivm/change.js';
import {Node, Row} from 'zql/src/zql/ivm/data.js';
import {Input, Storage} from 'zql/src/zql/ivm/operator.js';
import {Schema} from 'zql/src/zql/ivm/schema.js';
import {Source, SourceChange} from 'zql/src/zql/ivm/source.js';
import {TableSource} from 'zqlite/src/table-source.js';
import {ClientGroupStorage} from './database-storage.js';
import {SnapshotDiff, Snapshotter} from './snapshotter.js';

export type RowAdd = {
  readonly type: 'add';
  readonly queryHash: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: Row;
};

export type RowRemove = {
  readonly type: 'remove';
  readonly queryHash: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: undefined;
};

export type RowEdit = {
  readonly type: 'edit';
  readonly queryHash: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: Row;
};

export type RowChange = RowAdd | RowRemove | RowEdit;

/** Normalized TableSpec information for faster processing. */
type NormalizedTableSpec = TableSpec & {
  readonly boolColumns: Set<string>;
};

function normalize(tableSpec: TableSpec): NormalizedTableSpec {
  const {primaryKey, columns} = tableSpec;
  return {
    ...tableSpec,
    // Primary keys are normalized here so that downstream
    // normalization does not need to create new objects.
    // See row-key.ts: normalizedKeyOrder()
    primaryKey: [...primaryKey].sort(),
    boolColumns: new Set(
      Object.entries(columns)
        .filter(([_, spec]) => spec.dataType === 'BOOL')
        .map(([col]) => col),
    ),
  };
}

/**
 * Manages the state of IVM pipelines for a given ViewSyncer (i.e. client group).
 */
export class PipelineDriver {
  readonly #tables = new Map<string, TableSource>();
  readonly #pipelines = new Map<string, Input>();

  readonly #lc: LogContext;
  readonly #snapshotter: Snapshotter;
  readonly #storage: ClientGroupStorage;
  #tableSpecs: Map<string, NormalizedTableSpec> | null = null;
  #streamer: Streamer | null = null;

  constructor(
    lc: LogContext,
    snapshotter: Snapshotter,
    storage: ClientGroupStorage,
  ) {
    this.#lc = lc;
    this.#snapshotter = snapshotter;
    this.#storage = storage;
  }

  /**
   * Initializes the PipelineDriver to the current head of the database.
   * Queries can then be added (i.e. hydrated) with {@link addQuery()}.
   *
   * Must only be called once.
   */
  init() {
    assert(!this.#snapshotter.initialized(), 'Already initialized');

    const {db} = this.#snapshotter.init().current();
    this.#tableSpecs = new Map(
      listTables(db.db).map(spec => [spec.name, normalize(spec)]),
    );
  }

  /**
   * @returns Whether the PipelineDriver has been initialized.
   */
  initialized(): boolean {
    return this.#snapshotter.initialized();
  }

  /**
   * Returns the current version of the database. This will reflect the
   * latest version change when calling {@link advance()} once the
   * iteration has begun.
   */
  currentVersion(): string {
    assert(this.initialized(), 'Not yet initialized');
    return this.#snapshotter.current().version;
  }

  release() {
    this.#snapshotter.release();
  }

  advanceWithoutDiff(): string {
    return this.#snapshotter.advance().curr.version;
  }

  /**
   * Clears storage used for the pipelines. Call this when the
   * PipelineDriver will no longer be used.
   */
  destroy() {
    this.#storage.destroy();
    this.#snapshotter.destroy();
  }

  /** @return The Set of query hashes for all added queries. */
  addedQueries(): Set<string> {
    return new Set(this.#pipelines.keys());
  }

  /**
   * Adds a pipeline for the query. The method will hydrated the query using
   * the the driver's current snapshot of the database and return a stream
   * of results. Henceforth, updates to the query will be returned when the
   * driver is {@link advance}d. The query and its pipeline can be removed with
   * {@link removeQuery()}.
   *
   * @return The rows from the initial hydration of the query.
   */
  *addQuery(hash: string, query: AST): Iterable<RowChange> {
    assert(this.initialized());
    assert(!this.#pipelines.has(hash), `query ${hash} already added`);
    const input = buildPipeline(query, {
      getSource: name => this.#getSource(name),
      createStorage: () => this.#createStorage(),
      staticQueryParameters: undefined,
    });
    this.#pipelines.set(hash, input);

    const schema = input.getSchema();
    input.setOutput({
      push: change => {
        const streamer = this.#streamer;
        assert(streamer, 'must #startAccumulating() before pushing changes');
        streamer.accumulate(hash, schema, [change]);
      },
    });

    const res = input.fetch({});
    const streamer = new Streamer().accumulate(hash, schema, toAdds(res));
    yield* streamer.stream();
  }

  /**
   * Removes the pipeline for the query. This is a no-op if the query
   * was not added.
   */
  removeQuery(hash: string) {
    const input = this.#pipelines.get(hash);
    if (input) {
      this.#pipelines.delete(hash);
      input.destroy();
    }
  }

  /** Removes pipelines for all queries. */
  clear() {
    for (const input of this.#pipelines.values()) {
      input.destroy();
    }
    this.#pipelines.clear();
  }

  /**
   * Returns the value of the row with the given primary key `pk`,
   * or `undefined` if there is no such row. The pipeline must have been
   * initialized.
   */
  getRow(table: string, pk: RowKey): Row | undefined {
    assert(this.initialized(), 'Not yet initialized');
    const source = must(this.#tables.get(table));
    return source.getRow(pk as Row);
  }

  /**
   * Advances to the new head of the database.
   *
   * @return The resulting row changes for all added queries. Note that the
   *         `changes` must be iterated over in their entirety in order to
   *         advance the database snapshot.
   */
  advance(): {
    version: string;
    numChanges: number;
    changes: Iterable<RowChange>;
  } {
    assert(this.initialized());
    const diff = this.#snapshotter.advance();
    const {prev, curr, changes} = diff;
    this.#lc.debug?.(`${prev.version} => ${curr.version}: ${changes} changes`);

    return {
      version: curr.version,
      numChanges: changes,
      changes: this.#advance(diff),
    };
  }

  *#advance(diff: SnapshotDiff): Iterable<RowChange> {
    for (const {table, prevValue, nextValue} of diff) {
      if (prevValue) {
        if (nextValue) {
          yield* this.#push(table, {
            type: 'edit',
            row: nextValue as Row,
            oldRow: prevValue as Row,
          });
        } else {
          yield* this.#push(table, {type: 'remove', row: prevValue as Row});
        }
      } else if (nextValue) {
        yield* this.#push(table, {type: 'add', row: nextValue as Row});
      }
    }

    // Set the new snapshot on all TableSources.
    const {curr} = diff;
    for (const table of this.#tables.values()) {
      table.setDB(curr.db.db);
    }
    this.#lc.debug?.(`Advanced to ${curr.version}`);
  }

  /** Implements `BuilderDelegate.getSource()` */
  #getSource(tableName: string): Source {
    assert(this.#tableSpecs, 'Pipelines have not be initialized');
    let source = this.#tables.get(tableName);
    if (source) {
      return source;
    }

    const tableSpec = this.#tableSpecs.get(tableName);
    if (!tableSpec) {
      throw new Error(`Unknown table ${tableName}`);
    }
    const {columns, primaryKey} = tableSpec;
    assert(primaryKey.length);

    const {db} = this.#snapshotter.current();
    source = new TableSource(
      db.db,
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
    this.#lc.debug?.(`created TableSource for ${tableName}`);
    return source;
  }

  /** Implements `BuilderDelegate.createStorage()` */
  #createStorage(): Storage {
    return this.#storage.createStorage();
  }

  *#push(table: string, change: SourceChange): Iterable<RowChange> {
    const source = this.#tables.get(table);
    assert(source, `TableSource for ${table} not found`);

    this.#startAccumulating();
    source.push(change);
    yield* this.#stopAccumulating().stream();
  }

  #startAccumulating() {
    assert(this.#streamer === null);
    this.#streamer = new Streamer();
  }

  #stopAccumulating(): Streamer {
    const streamer = this.#streamer;
    assert(streamer);
    this.#streamer = null;
    return streamer;
  }
}

class Streamer {
  readonly #changes: [
    hash: string,
    schema: Schema,
    changes: Iterable<Change>,
  ][] = [];

  accumulate(hash: string, schema: Schema, changes: Iterable<Change>): this {
    this.#changes.push([hash, schema, changes]);
    return this;
  }

  *stream(): Iterable<RowChange> {
    for (const [hash, schema, changes] of this.#changes) {
      yield* this.#streamChanges(hash, schema, changes);
    }
  }

  *#streamChanges(
    queryHash: string,
    schema: Schema,
    changes: Iterable<Change>,
  ): Iterable<RowChange> {
    for (const change of changes) {
      const {type} = change;

      switch (type) {
        case 'add':
        case 'remove': {
          yield* this.#streamNodes(queryHash, schema, type, [change.node]);
          break;
        }
        case 'child': {
          const {child} = change;
          const childSchema = must(
            schema.relationships[child.relationshipName],
          );

          yield* this.#streamChanges(queryHash, childSchema, [child.change]);
          break;
        }
        case 'edit':
          yield* this.#streamNodes(queryHash, schema, type, [
            {row: change.row, relationships: {}},
          ]);
          break;
        default:
          unreachable(type);
      }
    }
  }

  *#streamNodes(
    queryHash: string,
    schema: Schema,
    op: 'add' | 'remove' | 'edit',
    nodes: Iterable<Node>,
  ): Iterable<RowChange> {
    const {tableName: table, primaryKey} = schema;

    for (const node of nodes) {
      const {relationships, row} = node;
      const rowKey = Object.fromEntries(primaryKey.map(col => [col, row[col]]));

      yield {
        type: op,
        queryHash,
        table,
        rowKey,
        row: op === 'remove' ? undefined : row,
      } as RowChange;

      for (const [relationship, children] of Object.entries(relationships)) {
        const childSchema = must(schema.relationships[relationship]);

        yield* this.#streamNodes(queryHash, childSchema, op, children);
      }
    }
  }
}

function* toAdds(nodes: Iterable<Node>): Iterable<Change> {
  for (const node of nodes) {
    yield {type: 'add', node};
  }
}
