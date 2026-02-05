import type {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {deepEqual, type JSONValue} from '../../../../shared/src/json.ts';
import {must} from '../../../../shared/src/must.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import {buildPipeline} from '../../../../zql/src/builder/builder.ts';
import {
  Debug,
  runtimeDebugFlags,
} from '../../../../zql/src/builder/debug-delegate.ts';
import type {Change} from '../../../../zql/src/ivm/change.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import {type Input, type Storage} from '../../../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../../../zql/src/ivm/schema.ts';
import type {
  Source,
  SourceChange,
  SourceInput,
} from '../../../../zql/src/ivm/source.ts';
import type {ConnectionCostModel} from '../../../../zql/src/planner/planner-connection.ts';
import {MeasurePushOperator} from '../../../../zql/src/query/measure-push-operator.ts';
import type {ClientGroupStorage} from '../../../../zqlite/src/database-storage.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {createSQLiteCostModel} from '../../../../zqlite/src/sqlite-cost-model.ts';
import {TableSource} from '../../../../zqlite/src/table-source.ts';
import {
  reloadPermissionsIfChanged,
  type LoadedPermissions,
} from '../../auth/load-permissions.ts';
import type {LogConfig, ZeroConfig} from '../../config/zero-config.ts';
import {computeZqlSpecs, mustGetTableSpec} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../../db/specs.ts';
import {
  getOrCreateCounter,
  getOrCreateHistogram,
} from '../../observability/metrics.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {type RowKey} from '../../types/row-key.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {getSubscriptionState} from '../replicator/schema/replication-state.ts';
import {checkClientSchema} from './client-schema.ts';
import type {LiteralValue} from '../../../../zero-protocol/src/ast.ts';
import {resolveSimpleScalarSubqueries} from '../../../../zqlite/src/resolve-scalar-subqueries.ts';
import type {Snapshotter} from './snapshotter.ts';
import {ResetPipelinesSignal, type SnapshotDiff} from './snapshotter.ts';

export type RowAdd = {
  readonly type: 'add';
  readonly queryID: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: Row;
};

export type RowRemove = {
  readonly type: 'remove';
  readonly queryID: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: undefined;
};

export type RowEdit = {
  readonly type: 'edit';
  readonly queryID: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: Row;
};

export type RowChange = RowAdd | RowRemove | RowEdit;

type Pipeline = {
  readonly input: Input;
  readonly hydrationTimeMs: number;
  readonly transformedAst: AST;
  readonly transformationHash: string;
  /** Pre-resolution AST (set on the parent pipeline that has companions). */
  readonly originalAST?: AST | undefined;
  /** Query IDs of companion pipelines associated with this parent. */
  readonly companionQueryIDs?: string[] | undefined;
  /** For companion pipelines: the parent query ID. */
  readonly parentQueryID?: string | undefined;
  /** Source inputs connected to table sources for this pipeline. */
  readonly sourceInputs: SourceInput[];
};

type QueryInfo = {
  readonly transformedAst: AST;
  readonly transformationHash: string;
};

type AdvanceContext = {
  readonly timer: Timer;
  readonly totalHydrationTimeMs: number;
  readonly numChanges: number;
  pos: number;
};

type HydrateContext = {
  readonly timer: Timer;
};

export type Timer = {
  elapsedLap: () => number;
  totalElapsed: () => number;
};

/**
 * No matter how fast hydration is, advancement is given at least this long to
 * complete before doing a pipeline reset.
 */
const MIN_ADVANCEMENT_TIME_LIMIT_MS = 50;

/**
 * Manages the state of IVM pipelines for a given ViewSyncer (i.e. client group).
 */
export class PipelineDriver {
  readonly #tables = new Map<string, TableSource>();
  // Query id to pipeline
  readonly #pipelines = new Map<string, Pipeline>();

  readonly #lc: LogContext;
  readonly #snapshotter: Snapshotter;
  readonly #storage: ClientGroupStorage;
  readonly #shardID: ShardID;
  readonly #logConfig: LogConfig;
  readonly #config: ZeroConfig | undefined;
  readonly #tableSpecs = new Map<string, LiteAndZqlSpec>();
  readonly #costModels: WeakMap<Database, ConnectionCostModel> | undefined;
  readonly #yieldThresholdMs: () => number;
  #streamer: Streamer | null = null;
  #hydrateContext: HydrateContext | null = null;
  #advanceContext: AdvanceContext | null = null;
  /** Parent query IDs whose companions received diffs during advancement. */
  readonly #parentsToReResolve = new Set<string>();
  #replicaVersion: string | null = null;
  #primaryKeys: Map<string, PrimaryKey> | null = null;
  #permissions: LoadedPermissions | null = null;

  readonly #advanceTime = getOrCreateHistogram('sync', 'ivm.advance-time', {
    description:
      'Time to advance all queries for a given client group for in response to a single change.',
    unit: 's',
  });

  readonly #conflictRowsDeleted = getOrCreateCounter(
    'sync',
    'ivm.conflict-rows-deleted',
    'Number of rows deleted because they conflicted with added row',
  );

  readonly #inspectorDelegate: InspectorDelegate;

  constructor(
    lc: LogContext,
    logConfig: LogConfig,
    snapshotter: Snapshotter,
    shardID: ShardID,
    storage: ClientGroupStorage,
    clientGroupID: string,
    inspectorDelegate: InspectorDelegate,
    yieldThresholdMs: () => number,
    enablePlanner?: boolean | undefined,
    config?: ZeroConfig | undefined,
  ) {
    this.#lc = lc.withContext('clientGroupID', clientGroupID);
    this.#snapshotter = snapshotter;
    this.#storage = storage;
    this.#shardID = shardID;
    this.#logConfig = logConfig;
    this.#config = config;
    this.#inspectorDelegate = inspectorDelegate;
    this.#costModels = enablePlanner ? new WeakMap() : undefined;
    this.#yieldThresholdMs = yieldThresholdMs;
  }

  /**
   * Initializes the PipelineDriver to the current head of the database.
   * Queries can then be added (i.e. hydrated) with {@link addQuery()}.
   *
   * Must only be called once.
   */
  init(clientSchema: ClientSchema) {
    assert(!this.#snapshotter.initialized(), 'Already initialized');
    this.#snapshotter.init();
    this.#initAndResetCommon(clientSchema);
  }

  /**
   * @returns Whether the PipelineDriver has been initialized.
   */
  initialized(): boolean {
    return this.#snapshotter.initialized();
  }

  /**
   * Clears the current pipelines and TableSources, returning the PipelineDriver
   * to its initial state. This should be called in response to a schema change,
   * as TableSources need to be recomputed.
   */
  reset(clientSchema: ClientSchema) {
    for (const {input} of this.#pipelines.values()) {
      input.destroy();
    }
    this.#pipelines.clear();
    this.#tables.clear();
    this.#initAndResetCommon(clientSchema);
  }

  #initAndResetCommon(clientSchema: ClientSchema) {
    const {db} = this.#snapshotter.current();
    const fullTables = new Map<string, LiteTableSpec>();
    computeZqlSpecs(
      this.#lc,
      db.db,
      {includeBackfillingColumns: false},
      this.#tableSpecs,
      fullTables,
    );
    checkClientSchema(
      this.#shardID,
      clientSchema,
      this.#tableSpecs,
      fullTables,
    );
    const primaryKeys = this.#primaryKeys ?? new Map<string, PrimaryKey>();
    this.#primaryKeys = primaryKeys;
    primaryKeys.clear();
    for (const [table, spec] of this.#tableSpecs.entries()) {
      if (table.startsWith(upstreamSchema(this.#shardID))) {
        primaryKeys.set(table, spec.tableSpec.primaryKey);
      }
    }
    buildPrimaryKeys(clientSchema, primaryKeys);
    const {replicaVersion} = getSubscriptionState(db);
    this.#replicaVersion = replicaVersion;
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
   * Returns the current upstream {app}.permissions, or `null` if none are defined.
   */
  currentPermissions(): LoadedPermissions | null {
    assert(this.initialized(), 'Not yet initialized');
    const res = reloadPermissionsIfChanged(
      this.#lc,
      this.#snapshotter.current().db,
      this.#shardID.appID,
      this.#permissions,
      this.#config,
    );
    if (res.changed) {
      this.#permissions = res.permissions;
      this.#lc.debug?.(
        'Reloaded permissions',
        JSON.stringify(this.#permissions),
      );
    }
    return this.#permissions;
  }

  advanceWithoutDiff(): string {
    const {db, version} = this.#snapshotter.advanceWithoutDiff().curr;
    for (const table of this.#tables.values()) {
      table.setDB(db.db);
    }
    return version;
  }

  #ensureCostModelExistsIfEnabled(db: Database) {
    let existing = this.#costModels?.get(db);
    if (existing) {
      return existing;
    }
    if (this.#costModels) {
      const costModel = createSQLiteCostModel(db, this.#tableSpecs);
      this.#costModels.set(db, costModel);
      return costModel;
    }
    return undefined;
  }

  /**
   * Clears storage used for the pipelines. Call this when the
   * PipelineDriver will no longer be used.
   */
  destroy() {
    this.#storage.destroy();
    this.#snapshotter.destroy();
  }

  /** @return Map from query ID to PipelineInfo for all added queries. */
  queries(): ReadonlyMap<string, QueryInfo> {
    return this.#pipelines;
  }

  totalHydrationTimeMs(): number {
    let total = 0;
    for (const pipeline of this.#pipelines.values()) {
      total += pipeline.hydrationTimeMs;
    }
    return total;
  }

  /**
   * Adds a pipeline for the query. The method will hydrate the query using the
   * driver's current snapshot of the database and return a stream of results.
   * Henceforth, updates to the query will be returned when the driver is
   * {@link advance}d. The query and its pipeline can be removed with
   * {@link removeQuery()}.
   *
   * If a query with the same queryID is already added, the existing pipeline
   * will be removed and destroyed before adding the new pipeline.
   *
   * @param timer The caller-controlled {@link Timer} used to determine the
   *        final hydration time. (The caller may pause and resume the timer
   *        when yielding the thread for time-slicing).
   * @return The rows from the initial hydration of the query.
   */
  *addQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
  ): Iterable<RowChange | 'yield'> {
    assert(
      this.initialized(),
      'Pipeline driver must be initialized before adding queries',
    );
    this.removeQuery(queryID);
    const debugDelegate = runtimeDebugFlags.trackRowsVended
      ? new Debug()
      : undefined;

    const costModel = this.#ensureCostModelExistsIfEnabled(
      this.#snapshotter.current().db.db,
    );

    const sourceInputs: SourceInput[] = [];
    const input = buildPipeline(
      query,
      {
        debug: debugDelegate,
        enableNotExists: true, // Server-side can handle NOT EXISTS
        getSource: name => this.#getSource(name),
        createStorage: () => this.#createStorage(),
        decorateSourceInput: (si: SourceInput, _queryID: string): Input => {
          sourceInputs.push(si);
          return new MeasurePushOperator(
            si,
            queryID,
            this.#inspectorDelegate,
            'query-update-server',
          );
        },
        decorateInput: input => input,
        addEdge() {},
        decorateFilterInput: input => input,
      },
      queryID,
      costModel,
    );
    const schema = input.getSchema();
    input.setOutput({
      push: change => {
        const streamer = this.#streamer;
        assert(streamer, 'must #startAccumulating() before pushing changes');
        streamer.accumulate(queryID, schema, [change]);
        return [];
      },
    });

    assert(
      this.#advanceContext === null,
      'Cannot hydrate while advance is in progress',
    );
    this.#hydrateContext = {
      timer,
    };
    try {
      yield* hydrateInternal(input, queryID, must(this.#primaryKeys));
    } finally {
      this.#hydrateContext = null;
    }

    const hydrationTimeMs = timer.totalElapsed();
    if (runtimeDebugFlags.trackRowCountsVended) {
      if (hydrationTimeMs > this.#logConfig.slowHydrateThreshold) {
        let totalRowsConsidered = 0;
        const lc = this.#lc
          .withContext('queryID', queryID)
          .withContext('hydrationTimeMs', hydrationTimeMs);
        for (const tableName of this.#tables.keys()) {
          const entries = Object.entries(
            debugDelegate?.getVendedRowCounts()[tableName] ?? {},
          );
          totalRowsConsidered += entries.reduce(
            (acc, entry) => acc + entry[1],
            0,
          );
          lc.info?.(tableName + ' VENDED: ', entries);
        }
        lc.info?.(`Total rows considered: ${totalRowsConsidered}`);
      }
    }
    debugDelegate?.reset();

    // Note: This hydrationTime is a wall-clock overestimate, as it does
    // not take time slicing into account. The view-syncer resets this
    // to a more precise processing-time measurement with setHydrationTime().
    this.#pipelines.set(queryID, {
      input,
      hydrationTimeMs,
      transformedAst: query,
      transformationHash,
      sourceInputs,
    });
  }

  /**
   * Adds a pipeline for the query, resolving any simple scalar subqueries
   * as companion IVM pipelines. Companion pipelines are hydrated first, their
   * scalar values are extracted and substituted into the parent AST, then the
   * parent pipeline is hydrated with the resolved AST.
   *
   * Returns the row changes from both companion and parent hydrations.
   * Companion query IDs (if any) are pushed into `outCompanionQueryIDs`.
   */
  *addQueryWithCompanions(
    transformationHash: string,
    queryID: string,
    ast: AST,
    timer: Timer,
    outCompanionQueryIDs?: string[] | undefined,
  ): Iterable<RowChange | 'yield'> {
    const companionQueryIDs: string[] = [];
    const companionRows: (RowChange | 'yield')[] = [];
    let companionIndex = 0;

    const {ast: resolvedAST} = resolveSimpleScalarSubqueries(
      ast,
      this.#tableSpecs,
      (subqueryAST, childField) => {
        const companionID = `scalar:${queryID}:${companionIndex++}`;
        companionQueryIDs.push(companionID);

        // Hydrate companion as a full IVM pipeline, collecting rows.
        for (const change of this.addQuery(
          transformationHash,
          companionID,
          subqueryAST,
          timer,
        )) {
          companionRows.push(change);
        }

        // Extract scalar value from the first hydrated add row.
        const firstAdd = companionRows.find(
          c => c !== 'yield' && c.type === 'add' && c.queryID === companionID,
        ) as RowAdd | undefined;
        const value =
          (firstAdd?.row?.[childField] as LiteralValue | undefined) ??
          undefined;

        // Tag the companion pipeline with its parent.
        const pipeline = this.#pipelines.get(companionID);
        if (pipeline) {
          this.#pipelines.set(companionID, {
            ...pipeline,
            parentQueryID: queryID,
          });
        }

        return value;
      },
    );

    // Yield buffered companion rows first.
    yield* companionRows;

    // Add the main query with the resolved AST.
    yield* this.addQuery(transformationHash, queryID, resolvedAST, timer);

    // Tag the parent pipeline with companion tracking info.
    const parentPipeline = this.#pipelines.get(queryID);
    if (parentPipeline && companionQueryIDs.length > 0) {
      this.#pipelines.set(queryID, {
        ...parentPipeline,
        originalAST: ast,
        companionQueryIDs,
      });
    }

    // Communicate companion IDs to the caller.
    if (outCompanionQueryIDs) {
      outCompanionQueryIDs.push(...companionQueryIDs);
    }
  }

  /**
   * Removes the pipeline for the query. If the query has companion pipelines,
   * they are also removed. This is a no-op if the query was not added.
   */
  removeQuery(queryID: string) {
    const pipeline = this.#pipelines.get(queryID);
    if (pipeline) {
      // Remove companion pipelines if this is a parent query.
      if (pipeline.companionQueryIDs) {
        for (const companionID of pipeline.companionQueryIDs) {
          const companion = this.#pipelines.get(companionID);
          if (companion) {
            this.#pipelines.delete(companionID);
            companion.input.destroy();
          }
        }
      }
      this.#pipelines.delete(queryID);
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
   * @param timer The caller-controlled {@link Timer} that will be used to
   *        measure the progress of the advancement and abort with a
   *        {@link ResetPipelinesSignal} if it is estimated to take longer
   *        than a hydration.
   * @return The resulting row changes for all added queries. Note that the
   *         `changes` must be iterated over in their entirety in order to
   *         advance the database snapshot.
   */
  advance(
    timer: Timer,
    onBeforeReResolve?: (queryIDs: string[]) => void,
  ): {
    version: string;
    numChanges: number;
    changes: Iterable<RowChange | 'yield'>;
  } {
    assert(
      this.initialized(),
      'Pipeline driver must be initialized before advancing',
    );
    const diff = this.#snapshotter.advance(this.#tableSpecs);
    const {prev, curr, changes} = diff;
    this.#lc.debug?.(
      `advance ${prev.version} => ${curr.version}: ${changes} changes`,
    );

    return {
      version: curr.version,
      numChanges: changes,
      changes: this.#advance(diff, timer, changes, onBeforeReResolve),
    };
  }

  *#advance(
    diff: SnapshotDiff,
    timer: Timer,
    numChanges: number,
    onBeforeReResolve?: (queryIDs: string[]) => void,
  ): Iterable<RowChange | 'yield'> {
    assert(
      this.#hydrateContext === null,
      'Cannot advance while hydration is in progress',
    );
    this.#parentsToReResolve.clear();
    this.#advanceContext = {
      timer,
      totalHydrationTimeMs: this.totalHydrationTimeMs(),
      numChanges,
      pos: 0,
    };
    try {
      for (const {table, prevValues, nextValue} of diff) {
        // Advance progress is checked each time a row is fetched
        // from a TableSource during push processing, but some pushes
        // don't read any rows.  Check progress here before processing
        // the next change.
        if (this.#shouldAdvanceYieldMaybeAbortAdvance()) {
          yield 'yield';
        }
        const start = performance.now();
        let type;
        try {
          const tableSource = this.#tables.get(table);
          if (!tableSource) {
            // no pipelines read from this table, so no need to process the change
            continue;
          }
          const primaryKey = mustGetPrimaryKey(this.#primaryKeys, table);
          let editOldRow: Row | undefined = undefined;
          for (const prevValue of prevValues) {
            if (
              nextValue &&
              deepEqual(
                getRowKey(primaryKey, prevValue as Row) as JSONValue,
                getRowKey(primaryKey, nextValue as Row) as JSONValue,
              )
            ) {
              editOldRow = prevValue;
            } else {
              if (nextValue) {
                this.#conflictRowsDeleted.add(1);
              }
              yield* this.#push(tableSource, {
                type: 'remove',
                row: prevValue,
              });
            }
          }
          if (nextValue) {
            if (editOldRow) {
              yield* this.#push(tableSource, {
                type: 'edit',
                row: nextValue,
                oldRow: editOldRow,
              });
            } else {
              yield* this.#push(tableSource, {
                type: 'add',
                row: nextValue,
              });
            }
          }
        } finally {
          this.#advanceContext.pos++;
        }

        const elapsed = performance.now() - start;
        this.#advanceTime.record(elapsed / 1000, {
          table,
          type,
        });
      }

      // Clear disabled inputs before re-resolving companions so that
      // the rebuilt pipelines can push normally.
      for (const table of this.#tables.values()) {
        table.clearDisabledConnections();
      }

      // Set the new snapshot on all TableSources.
      const {curr} = diff;
      for (const table of this.#tables.values()) {
        table.setDB(curr.db.db);
      }
      this.#ensureCostModelExistsIfEnabled(curr.db.db);
      this.#lc.debug?.(`Advanced to ${curr.version}`);

      // The advance (diff processing) phase is complete. Clear the context
      // before re-resolving companions, which hydrates new pipelines via
      // addQueryWithCompanions (addQuery asserts advanceContext is null).
      this.#advanceContext = null;

      // Re-resolve parent pipelines whose companion scalar values changed.
      yield* this.#reResolveCompanions(timer, onBeforeReResolve);
    } finally {
      this.#advanceContext = null;
    }
  }

  /**
   * Tears down and rebuilds parent pipelines whose companion pipelines
   * received diffs during advancement. The set of affected parents is
   * populated by {@link #push} as it yields row changes.
   */
  *#reResolveCompanions(
    timer: Timer,
    onBeforeReResolve?: (queryIDs: string[]) => void,
  ): Iterable<RowChange | 'yield'> {
    if (this.#parentsToReResolve.size === 0) {
      return;
    }

    // Copy and clear before rebuilding, since addQueryWithCompanions
    // may push through companions that re-populate the set.
    const parentIDs = [...this.#parentsToReResolve];
    this.#parentsToReResolve.clear();

    // Notify the caller of all query IDs being re-resolved so it can
    // strip their old refCounts before re-hydration adds arrive.
    if (onBeforeReResolve) {
      const allQueryIDs: string[] = [];
      for (const parentID of parentIDs) {
        const parent = this.#pipelines.get(parentID);
        if (!parent?.originalAST) {
          continue;
        }
        allQueryIDs.push(parentID);
        if (parent.companionQueryIDs) {
          allQueryIDs.push(...parent.companionQueryIDs);
        }
      }
      if (allQueryIDs.length > 0) {
        onBeforeReResolve(allQueryIDs);
      }
    }

    for (const parentID of parentIDs) {
      const parent = this.#pipelines.get(parentID);
      if (!parent?.originalAST) {
        continue;
      }
      const {originalAST, transformationHash} = parent;

      // Remove the parent (which also removes its companions).
      this.removeQuery(parentID);

      // Re-create from originalAST. This re-resolves scalar subqueries
      // with current companion values.
      yield* this.addQueryWithCompanions(
        transformationHash,
        parentID,
        originalAST,
        timer,
      );
    }
  }

  /** Implements `BuilderDelegate.getSource()` */
  #getSource(tableName: string): Source {
    let source = this.#tables.get(tableName);
    if (source) {
      return source;
    }

    const tableSpec = mustGetTableSpec(this.#tableSpecs, tableName);
    const primaryKey = mustGetPrimaryKey(this.#primaryKeys, tableName);

    const {db} = this.#snapshotter.current();
    source = new TableSource(
      this.#lc,
      this.#logConfig,
      db.db,
      tableName,
      tableSpec.zqlSpec,
      primaryKey,
      () => this.#shouldYield(),
    );
    this.#tables.set(tableName, source);
    this.#lc.debug?.(`created TableSource for ${tableName}`);
    return source;
  }

  #shouldYield(): boolean {
    if (this.#hydrateContext) {
      return this.#hydrateContext.timer.elapsedLap() > this.#yieldThresholdMs();
    }
    if (this.#advanceContext) {
      return this.#shouldAdvanceYieldMaybeAbortAdvance();
    }
    throw new Error('shouldYield called outside of hydration or advancement');
  }

  /**
   * Cancel the advancement processing, by throwing a ResetPipelinesSignal, if
   * it has taken longer than half the total hydration time to make it through
   * half of the advancement, or if processing time exceeds total hydration
   * time.  This serves as both a circuit breaker for very large transactions,
   * as well as a bound on the amount of time the previous connection locks
   * the inactive WAL file (as the lock prevents WAL2 from switching to the
   * free WAL when the current one is over the size limit, which can make
   * the WAL grow continuously and compound slowness).
   * This is checked:
   * 1. before starting to process each change in an advancement is processed
   * 2. whenever a row is fetched from a TableSource during push processing
   */
  #shouldAdvanceYieldMaybeAbortAdvance(): boolean {
    const {
      pos,
      numChanges,
      timer: advanceTimer,
      totalHydrationTimeMs,
    } = must(this.#advanceContext);
    const elapsed = advanceTimer.totalElapsed();
    if (
      elapsed > MIN_ADVANCEMENT_TIME_LIMIT_MS &&
      (elapsed > totalHydrationTimeMs ||
        (elapsed > totalHydrationTimeMs / 2 && pos <= numChanges / 2))
    ) {
      throw new ResetPipelinesSignal(
        `Advancement exceeded timeout at ${pos} of ${numChanges} changes ` +
          `after ${elapsed} ms. Advancement time limited based on total ` +
          `hydration time of ${totalHydrationTimeMs} ms.`,
      );
    }
    return advanceTimer.elapsedLap() > this.#yieldThresholdMs();
  }

  /** Implements `BuilderDelegate.createStorage()` */
  #createStorage(): Storage {
    return this.#storage.createStorage();
  }

  *#push(
    source: TableSource,
    change: SourceChange,
  ): Iterable<RowChange | 'yield'> {
    this.#startAccumulating();
    try {
      for (const val of source.genPush(change)) {
        if (val === 'yield') {
          yield 'yield';
        }
        for (const changeOrYield of this.#stopAccumulating().stream()) {
          if (changeOrYield !== 'yield') {
            const pipeline = this.#pipelines.get(changeOrYield.queryID);
            if (pipeline?.parentQueryID !== undefined) {
              this.#parentsToReResolve.add(pipeline.parentQueryID);
              // Disable the parent's source inputs so that subsequent
              // pushes in this advancement skip the parent pipeline,
              // which will be torn down and rebuilt in #reResolveCompanions.
              const parentPipeline = this.#pipelines.get(
                pipeline.parentQueryID,
              );
              if (parentPipeline) {
                for (const si of parentPipeline.sourceInputs) {
                  for (const tableSource of this.#tables.values()) {
                    tableSource.disableConnection(si);
                  }
                }
              }
              // Suppress companion changes: the companion will be torn
              // down and re-hydrated in #reResolveCompanions, so yielding
              // the incremental change would cause double-counting in the
              // CVR.
              continue;
            }
          }
          yield changeOrYield;
        }
        this.#startAccumulating();
      }
    } finally {
      if (this.#streamer !== null) {
        this.#stopAccumulating();
      }
    }
  }

  #startAccumulating() {
    assert(this.#streamer === null, 'Streamer already started');
    this.#streamer = new Streamer(must(this.#primaryKeys));
  }

  #stopAccumulating(): Streamer {
    const streamer = this.#streamer;
    assert(streamer, 'Streamer not started');
    this.#streamer = null;
    return streamer;
  }
}

class Streamer {
  readonly #primaryKeys: Map<string, PrimaryKey>;

  constructor(primaryKeys: Map<string, PrimaryKey>) {
    this.#primaryKeys = primaryKeys;
  }

  readonly #changes: [
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | 'yield'>,
  ][] = [];

  accumulate(
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | 'yield'>,
  ): this {
    this.#changes.push([queryID, schema, changes]);
    return this;
  }

  *stream(): Iterable<RowChange | 'yield'> {
    for (const [queryID, schema, changes] of this.#changes) {
      yield* this.#streamChanges(queryID, schema, changes);
    }
  }

  *#streamChanges(
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | 'yield'>,
  ): Iterable<RowChange | 'yield'> {
    // We do not sync rows gathered by the permissions
    // system to the client.
    if (schema.system === 'permissions') {
      return;
    }

    for (const change of changes) {
      if (change === 'yield') {
        yield change;
        continue;
      }
      const {type} = change;

      switch (type) {
        case 'add':
        case 'remove': {
          yield* this.#streamNodes(queryID, schema, type, () => [change.node]);
          break;
        }
        case 'child': {
          const {child} = change;
          const childSchema = must(
            schema.relationships[child.relationshipName],
          );

          yield* this.#streamChanges(queryID, childSchema, [child.change]);
          break;
        }
        case 'edit':
          yield* this.#streamNodes(queryID, schema, type, () => [
            {row: change.node.row, relationships: {}},
          ]);
          break;
        default:
          unreachable(type);
      }
    }
  }

  *#streamNodes(
    queryID: string,
    schema: SourceSchema,
    op: 'add' | 'remove' | 'edit',
    nodes: () => Iterable<Node | 'yield'>,
  ): Iterable<RowChange | 'yield'> {
    const {tableName: table, system} = schema;

    const primaryKey = must(this.#primaryKeys.get(table));

    // We do not sync rows gathered by the permissions
    // system to the client.
    if (system === 'permissions') {
      return;
    }

    for (const node of nodes()) {
      if (node === 'yield') {
        yield node;
        continue;
      }
      const {relationships, row} = node;
      const rowKey = getRowKey(primaryKey, row);

      yield {
        type: op,
        queryID,
        table,
        rowKey,
        row: op === 'remove' ? undefined : row,
      } as RowChange;

      for (const [relationship, children] of Object.entries(relationships)) {
        const childSchema = must(schema.relationships[relationship]);
        yield* this.#streamNodes(queryID, childSchema, op, children);
      }
    }
  }
}

function* toAdds(nodes: Iterable<Node | 'yield'>): Iterable<Change | 'yield'> {
  for (const node of nodes) {
    if (node === 'yield') {
      yield node;
      continue;
    }
    yield {type: 'add', node};
  }
}

function getRowKey(cols: PrimaryKey, row: Row): RowKey {
  return Object.fromEntries(cols.map(col => [col, must(row[col])]));
}

/**
 * Core hydration logic used by {@link PipelineDriver#addQuery}, extracted to a
 * function for reuse by bin-analyze so that bin-analyze's hydration logic
 * is as close as possible to zero-cache's real hydration logic.
 */
export function* hydrate(
  input: Input,
  hash: string,
  clientSchema: ClientSchema,
): Iterable<RowChange | 'yield'> {
  const res = input.fetch({});
  const streamer = new Streamer(buildPrimaryKeys(clientSchema)).accumulate(
    hash,
    input.getSchema(),
    toAdds(res),
  );
  yield* streamer.stream();
}

export function* hydrateInternal(
  input: Input,
  hash: string,
  primaryKeys: Map<string, PrimaryKey>,
): Iterable<RowChange | 'yield'> {
  const res = input.fetch({});
  const streamer = new Streamer(primaryKeys).accumulate(
    hash,
    input.getSchema(),
    toAdds(res),
  );
  yield* streamer.stream();
}

function buildPrimaryKeys(
  clientSchema: ClientSchema,
  primaryKeys: Map<string, PrimaryKey> = new Map<string, PrimaryKey>(),
) {
  for (const [tableName, {primaryKey}] of Object.entries(clientSchema.tables)) {
    primaryKeys.set(tableName, primaryKey as unknown as PrimaryKey);
  }
  return primaryKeys;
}

function mustGetPrimaryKey(
  primaryKeys: Map<string, PrimaryKey> | null,
  table: string,
): PrimaryKey {
  const pKeys = must(primaryKeys, 'primaryKey map must be non-null');

  return must(
    pKeys.get(table),
    `table '${table}' is not one of: ${[...pKeys.keys()].sort()}. ` +
      `Check the spelling and ensure that the table has a primary key.`,
  );
}
