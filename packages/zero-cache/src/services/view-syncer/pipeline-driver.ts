import {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {must} from '../../../../shared/src/must.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import {buildPipeline} from '../../../../zql/src/builder/builder.ts';
import type {Change} from '../../../../zql/src/ivm/change.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import type {Input, Storage} from '../../../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../../../zql/src/ivm/schema.ts';
import type {Source, SourceChange} from '../../../../zql/src/ivm/source.ts';
import {
  runtimeDebugFlags,
  runtimeDebugStats,
} from '../../../../zqlite/src/runtime-debug.ts';
import {TableSource} from '../../../../zqlite/src/table-source.ts';
import {
  loadPermissions,
  type LoadedPermissions,
} from '../../auth/load-permissions.ts';
import type {LogConfig} from '../../config/zero-config.ts';
import {computeZqlSpecs} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../db/specs.ts';
import type {RowKey} from '../../types/row-key.ts';
import type {SchemaVersions} from '../../types/schema-versions.ts';
import {getSubscriptionState} from '../replicator/schema/replication-state.ts';
import type {ClientGroupStorage} from './database-storage.ts';
import {Snapshotter, type SnapshotDiff} from './snapshotter.ts';

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

type Pipeline = {
  readonly input: Input;
  readonly hydrationTimeMs: number;
};

/**
 * Manages the state of IVM pipelines for a given ViewSyncer (i.e. client group).
 */
export class PipelineDriver {
  readonly #tables = new Map<string, TableSource>();
  readonly #pipelines = new Map<string, Pipeline>();

  readonly #lc: LogContext;
  readonly #snapshotter: Snapshotter;
  readonly #storage: ClientGroupStorage;
  readonly #clientGroupID: string;
  readonly #logConfig: LogConfig;
  #tableSpecs: Map<string, LiteAndZqlSpec> | null = null;
  #streamer: Streamer | null = null;
  #replicaVersion: string | null = null;

  constructor(
    lc: LogContext,
    logConfig: LogConfig,
    snapshotter: Snapshotter,
    storage: ClientGroupStorage,
    clientGroupID: string,
  ) {
    this.#lc = lc.withContext('clientGroupID', clientGroupID);
    this.#snapshotter = snapshotter;
    this.#storage = storage;
    this.#clientGroupID = clientGroupID;
    this.#logConfig = logConfig;
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
    this.#tableSpecs = computeZqlSpecs(this.#lc, db.db);
    const {replicaVersion} = getSubscriptionState(db);
    this.#replicaVersion = replicaVersion;
  }

  /**
   * @returns Whether the PipelineDriver has been initialized.
   */
  initialized(): boolean {
    return this.#snapshotter.initialized();
  }

  /** @returns The replica version. The PipelineDriver must have been initialized. */
  get replicaVersion(): string {
    return must(this.#replicaVersion, 'Not yet initialized');
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

  /**
   * Returns the current supported schema version range of the database.  This
   * will reflect changes to supported schema version range when calling
   * {@link advance()} once the iteration has begun.
   */
  currentSchemaVersions(): SchemaVersions {
    assert(this.initialized(), 'Not yet initialized');
    return this.#snapshotter.current().schemaVersions;
  }

  /**
   * Returns the current upstream zero.permissions, or `null` if none are defined.
   */
  currentPermissions(): LoadedPermissions {
    assert(this.initialized(), 'Not yet initialized');
    return loadPermissions(this.#lc, this.#snapshotter.current().db);
  }

  advanceWithoutDiff(): string {
    const {db, version} = this.#snapshotter.advanceWithoutDiff().curr;
    for (const table of this.#tables.values()) {
      table.setDB(db.db);
    }
    return version;
  }

  /**
   * Clears the current pipelines and TableSources, returning the PipelineDriver
   * to its initial state. This should be called in response to a schema change,
   * as TableSources need to be recomputed.
   */
  reset() {
    const tableSpecs = this.#tableSpecs;
    assert(tableSpecs);
    for (const {input} of this.#pipelines.values()) {
      input.destroy();
    }
    this.#pipelines.clear();
    this.#tables.clear();
    tableSpecs.clear();

    const {db} = this.#snapshotter.current();
    computeZqlSpecs(this.#lc, db.db, tableSpecs);
    const {replicaVersion} = getSubscriptionState(db);
    this.#replicaVersion = replicaVersion;
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

  totalHydrationTimeMs(): number {
    let total = 0;
    for (const pipeline of this.#pipelines.values()) {
      total += pipeline.hydrationTimeMs;
    }
    return total;
  }

  /**
   * Adds a pipeline for the query. The method will hydrated the query using
   * the the driver's current snapshot of the database and return a stream
   * of results. Henceforth, updates to the query will be returned when the
   * driver is {@link advance}d. The query and its pipeline can be removed with
   * {@link removeQuery()}.
   *
   * If a query with an identical hash has already been added, this method
   * is a no-op and no RowChanges are generated.
   *
   * @return The rows from the initial hydration of the query.
   */
  *addQuery(hash: string, query: AST): Iterable<RowChange> {
    assert(this.initialized());
    if (this.#pipelines.has(hash)) {
      this.#lc.info?.(`query ${hash} already added`, query);
      return;
    }
    const input = buildPipeline(query, {
      getSource: name => this.#getSource(name),
      createStorage: () => this.#createStorage(),
    });
    const schema = input.getSchema();
    input.setOutput({
      push: change => {
        const streamer = this.#streamer;
        assert(streamer, 'must #startAccumulating() before pushing changes');
        streamer.accumulate(hash, schema, [change]);
      },
    });

    const start = Date.now();

    if (runtimeDebugFlags.trackRowsVended) {
      runtimeDebugStats.resetRowsVended(this.#clientGroupID);
    }

    const res = input.fetch({});
    const streamer = new Streamer(must(this.#tableSpecs)).accumulate(
      hash,
      schema,
      toAdds(res),
    );
    yield* streamer.stream();

    const hydrationTimeMs = Date.now() - start;
    if (runtimeDebugFlags.trackRowsVended) {
      if (hydrationTimeMs > 200) {
        let totalRowsConsidered = 0;
        const lc = this.#lc
          .withContext('hash', hash)
          .withContext('hydrationTimeMs', hydrationTimeMs);
        for (const tableName of this.#tables.keys()) {
          const entires = [
            ...(runtimeDebugStats
              .getRowsVended(this.#clientGroupID)
              ?.get(tableName)
              ?.entries() ?? []),
          ];
          totalRowsConsidered += entires.reduce(
            (acc, entry) => acc + entry[1],
            0,
          );
          lc.debug?.(tableName + ' VENDED: ', entires);
        }
        lc.debug?.(`Total rows considered: ${totalRowsConsidered}`);
      }
      runtimeDebugStats.resetRowsVended(this.#clientGroupID);
    }

    this.#pipelines.set(hash, {input, hydrationTimeMs});
  }

  /**
   * Removes the pipeline for the query. This is a no-op if the query
   * was not added.
   */
  removeQuery(hash: string) {
    const pipeline = this.#pipelines.get(hash);
    if (pipeline) {
      this.#pipelines.delete(hash);
      pipeline.input.destroy();
    }
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
    assert(this.initialized() && this.#tableSpecs);
    const diff = this.#snapshotter.advance(this.#tableSpecs);
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
      throw new Error(
        `table '${tableName}' is not one of: ${[...this.#tableSpecs.keys()]
          .filter(t => !t.includes('.') && !t.startsWith('_litestream_'))
          .sort()}. ` +
          `Check the spelling and ensure that the table has a primary key.`,
      );
    }
    const {primaryKey} = tableSpec.tableSpec;
    assert(primaryKey?.length);

    const {db} = this.#snapshotter.current();
    source = new TableSource(
      this.#lc,
      this.#logConfig,
      this.#clientGroupID,
      db.db,
      tableName,
      tableSpec.zqlSpec,
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
    if (!source) {
      return;
    }

    this.#startAccumulating();
    for (const _ of source.genPush(change)) {
      yield* this.#stopAccumulating().stream();
      this.#startAccumulating();
    }
    this.#stopAccumulating();
  }

  #startAccumulating() {
    assert(this.#streamer === null);
    this.#streamer = new Streamer(must(this.#tableSpecs));
  }

  #stopAccumulating(): Streamer {
    const streamer = this.#streamer;
    assert(streamer);
    this.#streamer = null;
    return streamer;
  }
}

class Streamer {
  #tableSpecs: Map<string, LiteAndZqlSpec>;

  constructor(tableSpecs: Map<string, LiteAndZqlSpec>) {
    this.#tableSpecs = tableSpecs;
  }

  readonly #changes: [
    hash: string,
    schema: SourceSchema,
    changes: Iterable<Change>,
  ][] = [];

  accumulate(
    hash: string,
    schema: SourceSchema,
    changes: Iterable<Change>,
  ): this {
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
    schema: SourceSchema,
    changes: Iterable<Change>,
  ): Iterable<RowChange> {
    // We do not sync rows gathered by the permissions
    // system to the client.
    if (schema.system === 'permissions') {
      return;
    }

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
            {row: change.node.row, relationships: {}},
          ]);
          break;
        default:
          unreachable(type);
      }
    }
  }

  *#streamNodes(
    queryHash: string,
    schema: SourceSchema,
    op: 'add' | 'remove' | 'edit',
    nodes: Iterable<Node>,
  ): Iterable<RowChange> {
    const {tableName: table, system} = schema;

    // The primaryKey here is used for referencing rows in CVR and del-row
    // patches sent in pokes. This is the "unionKey", i.e. the union of all
    // columns in unique indexes. This allows clients to migrate from, e.g.
    // pk1 to pk2, as del-patches will be keyed by [...pk1, ...pk2].
    const primaryKey = must(this.#tableSpecs.get(table)).tableSpec.unionKey;

    // We do not sync rows gathered by the permissions
    // system to the client.
    if (system === 'permissions') {
      return;
    }

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
        yield* this.#streamNodes(queryHash, childSchema, op, children());
      }
    }
  }
}

function* toAdds(nodes: Iterable<Node>): Iterable<Change> {
  for (const node of nodes) {
    yield {type: 'add', node};
  }
}
