import {LogContext} from '@rocicorp/logger';
import {TableSource} from '@rocicorp/zqlite/src/v2/table-source.js';
import {assert} from 'shared/src/asserts.js';
import {mapLiteDataTypeToZqlValueType} from 'zero-cache/src/types/lite.js';
import {AST} from 'zql/src/zql/ast2/ast.js';
import {buildPipeline} from 'zql/src/zql/builder/builder.js';
import {Change} from 'zql/src/zql/ivm2/change.js';
import {Node, Row} from 'zql/src/zql/ivm2/data.js';
import {Input, Storage} from 'zql/src/zql/ivm2/operator.js';
import {Schema} from 'zql/src/zql/ivm2/schema.js';
import {Source, SourceChange} from 'zql/src/zql/ivm2/source.js';
import {listTables} from '../replicator/tables/list.js';
import {TableSpec} from '../replicator/tables/specs.js';
import {ClientGroupStorage} from './database-storage.js';
import {Snapshotter} from './snapshotter.js';

export type RowAdd = {
  readonly queryHash: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: Row;
};

export type RowRemove = {
  readonly queryHash: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: undefined;
};

export type RowChange = RowAdd | RowRemove;

/**
 * Manages the state of IVM pipelines for a given ViewSyncer (i.e. client group).
 */
export class PipelineDriver {
  readonly #tables = new Map<string, TableSource>();
  readonly #pipelines = new Map<string, Input>();

  readonly #lc: LogContext;
  readonly #snapshotter: Snapshotter;
  readonly #storage: ClientGroupStorage;
  #tableSpecs: Map<string, TableSpec> | null = null;
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
      listTables(db.db).map(spec => [spec.name, spec]),
    );
  }

  /**
   * @returns Whether the PipelineDriver has been initialized.
   */
  initialized(): boolean {
    return this.#snapshotter.initialized();
  }

  /**
   * Clears storage used for the pipelines. Call this when the
   * PipelineDriver will no longer be used.
   */
  destroy() {
    this.#storage.destroy();
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
   * Removes the pipeline for the query.
   */
  removeQuery(hash: string) {
    const input = this.#pipelines.get(hash);
    assert(input, `Query ${hash} does not exist`);

    this.#pipelines.delete(hash);
    input.destroy();
  }

  /**
   * Advances to the new head of the database.
   *
   * @return The resulting row changes for all added queries.
   */
  *advance(): Iterable<RowChange> {
    assert(this.initialized());
    const diff = this.#snapshotter.advance();
    const {prev, curr, changes} = diff;

    this.#lc.debug?.(`${prev.version} => ${curr.version}: ${changes} changes`);

    for (const {table, prevValue, nextValue} of diff) {
      if (prevValue) {
        yield* this.#push(table, {type: 'remove', row: prevValue as Row});
      }
      if (nextValue) {
        yield* this.#push(table, {type: 'add', row: nextValue as Row});
      }
    }

    // Set the new snapshot on all TableSources.
    for (const table of this.#tables.values()) {
      table.setDB(this.#snapshotter.current().db.db);
    }
    this.#lc.debug?.(`Advanced to ${curr.version}`);
  }

  /** Implements `Host.getSource()` */
  #getSource(tableName: string): Source {
    assert(this.#tableSpecs, 'Pipelines have not be initialized');
    let source = this.#tables.get(tableName);
    if (source) {
      return source;
    }

    const tableSpec = this.#tableSpecs.get(tableName);
    if (!tableSpec) {
      throw new Error(`Unknown table ${tableSpec}`);
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
          mapLiteDataTypeToZqlValueType(dataType),
        ]),
      ),
      [primaryKey[0], ...primaryKey.slice(1)],
    );
    this.#tables.set(tableName, source);
    this.#lc.debug?.(`created TableSource for ${tableName}`);
    return source;
  }

  /** Implements `Host.createStorage()` */
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
      if (type === 'child') {
        const {child} = change;
        const childSchema = schema.relationships[child.relationshipName];
        assert(childSchema);

        yield* this.#streamChanges(queryHash, childSchema, [child.change]);
      } else {
        const {node} = change;

        yield* this.#streamNodes(queryHash, schema, type, [node]);
      }
    }
  }

  *#streamNodes(
    queryHash: string,
    schema: Schema,
    op: 'add' | 'remove',
    nodes: Iterable<Node>,
  ): Iterable<RowChange> {
    const {tableName: table, primaryKey} = schema;

    for (const node of nodes) {
      const {row, relationships} = node;
      const rowKey = Object.fromEntries(primaryKey.map(col => [col, row[col]]));

      yield {queryHash, table, rowKey, row: op === 'add' ? row : undefined};

      for (const [relationship, children] of Object.entries(relationships)) {
        const childSchema = schema.relationships[relationship];
        assert(childSchema);

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
