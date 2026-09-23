import type {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {deepEqual, type JSONValue} from '../../../../shared/src/json.ts';
import {getOrInsertComputed} from '../../../../shared/src/map.ts';
import {must} from '../../../../shared/src/must.ts';
import {randInt} from '../../../../shared/src/rand.ts';
import type {AST, LiteralValue} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import {buildPipeline} from '../../../../zql/src/builder/builder.ts';
import {
  Debug,
  runtimeDebugFlags,
} from '../../../../zql/src/builder/debug-delegate.ts';
import {ChangeIndex} from '../../../../zql/src/ivm/change-index.ts';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import type {Change} from '../../../../zql/src/ivm/change.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import {
  skipYields,
  throwOutput,
  type FetchRequest,
  type Input,
  type InputBase,
  type Operator,
  type Output,
  type Storage,
} from '../../../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../../../zql/src/ivm/schema.ts';
import {
  type Source,
  type SourceChange,
  type SourceInput,
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../../../../zql/src/ivm/source.ts';
import type {Stream} from '../../../../zql/src/ivm/stream.ts';
import type {ConnectionCostModel} from '../../../../zql/src/planner/planner-connection.ts';
import {MeasurePushOperator} from '../../../../zql/src/query/measure-push-operator.ts';
import type {ClientGroupStorage} from '../../../../zqlite/src/database-storage.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {
  resolveSimpleScalarSubqueries,
  type CompanionSubquery,
  type IgnoredScalarHint,
} from '../../../../zqlite/src/resolve-scalar-subqueries.ts';
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
  getOrCreateLatencyHistogram,
} from '../../observability/metrics.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {type RowKey} from '../../types/row-key.ts';
import {type ShardID} from '../../types/shards.ts';
import {
  getSubscriptionState,
  ZERO_VERSION_COLUMN_NAME,
} from '../replicator/schema/replication-state.ts';
import {checkClientSchema} from './client-schema.ts';
import {rowIDSignatureUnit} from './row-set-signature.ts';
import type {Snapshotter} from './snapshotter.ts';
import {ResetPipelinesSignal, type SnapshotDiff} from './snapshotter.ts';

type RowOp<Op extends Omit<ChangeType, ChangeType.CHILD>> = {
  readonly type: Op;
  readonly queryID: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: Row;
};

export type RowAdd = RowOp<ChangeType.ADD>;

export type RowRemove = RowOp<ChangeType.REMOVE>;

export type RowEdit = RowOp<ChangeType.EDIT>;

export type RowChange = RowAdd | RowRemove | RowEdit;

type CompanionPipeline = {
  readonly input: Input;
  readonly childField: string;
  readonly resolvedValue: LiteralValue | null | undefined;
};

type Pipeline = {
  readonly input: Input;
  readonly hydrationTimeMs: number;
  /**
   * The driver's own processing time to hydrate the pipeline, measured like
   * its advancement time (see {@link AdvanceContext}): without the time the
   * consumer of the hydration spends between pulls. Unlike `hydrationTimeMs`,
   * this does not depend on how the caller processed the rows, so it is the
   * pipeline's advancement budget.
   */
  readonly ivmHydrationTimeMs: number;
  /** The tables that the pipeline and its companions read. */
  readonly tables: ReadonlySet<string>;
  readonly hydrationRowCount: number;
  readonly hydrationReason: PipelineHydrationReason;
  readonly pipelineRunID: string;
  readonly pipelineReadyAtMs: number;
  readonly transformedAst: AST;
  readonly originalAst: AST;
  readonly transformationHash: string;
  readonly queryName?: string | undefined;
  readonly companions: readonly CompanionPipeline[];
};

export type QueryInfo = {
  readonly transformedAst: AST;
  readonly originalAst?: AST | undefined;
  readonly transformationHash: string;
  readonly queryName?: string | undefined;
};

type QueryLogInfo = {
  readonly queryHash: string;
  readonly transformationHash: string;
  readonly queryName?: string | undefined;
};

type QueryPipelineLifecycleEvent =
  | 'query-pipeline-hydrate-start'
  | 'query-pipeline-hydrate-finish'
  | 'query-pipeline-hydrate-failed'
  | 'query-pipeline-hydrate-aborted'
  | 'query-pipeline-stop';

export type PipelineHydrationReason =
  | 'query-set-sync'
  | 'unchanged-query-rehydrate'
  | 'advancement-reset';

type PipelineStopReason =
  | 'replace-query'
  | 'remove-query'
  | 'reset'
  | 'destroy'
  | 'advancement-reset';

/**
 * Which advancement check a pipeline failed: its time in the current change,
 * its projected time for the whole advancement, or its time so far.
 */
export type PartialResetReason =
  | 'slow-change'
  | 'projected-overrun'
  | 'timeout';

/**
 * A pipeline that {@link PipelineDriver.advance} dropped for going over its
 * advancement budget, with what is needed to rebuild it without transforming
 * the query again.
 */
export type DroppedQuery = {
  readonly transformationHash: string;
  readonly transformedAst: AST;
  readonly originalAst: AST;
  readonly queryName?: string | undefined;
  readonly reason: PartialResetReason;
  /** The hydration time that set the pipeline's advancement budget. */
  readonly hydrationTimeMs: number;
};

/**
 * Thrown when a pipeline goes over its advancement budget, to abandon the
 * pipeline's work on the current change. It is caught where the pipeline's
 * work started (by the pipeline's guard, or around the streaming of its
 * output), so the change continues to the other pipelines.
 *
 * It is a {@link ResetPipelinesSignal}, so it is not logged as a query failure,
 * and a signal that escapes by mistake resets the whole client group.
 */
export class DropPipelineSignal extends ResetPipelinesSignal {
  readonly queryID: string;
  readonly dropReason: PartialResetReason;

  constructor(queryID: string, dropReason: PartialResetReason, msg: string) {
    super(msg, 'advancement-timeout');
    this.queryID = queryID;
    this.dropReason = dropReason;
  }
}

type QueryPipelineLifecycleLog = {
  readonly zeroEvent: QueryPipelineLifecycleEvent;
  readonly pipelineRunID: string;
  readonly queryHash: string;
  readonly transformationHash: string;
  readonly queryName?: string | undefined;
  readonly hydrationReason?: PipelineHydrationReason | undefined;
  readonly stopReason?: PipelineStopReason | undefined;
  readonly hydrationTimeMs?: number | undefined;
  readonly ivmHydrationTimeMs?: number | undefined;
  readonly hydrationRowCount?: number | undefined;
  readonly pipelineLifetimeMs?: number | undefined;
};

/** The advancement accounting of one pipeline, for one advancement. */
type PipelineAdvancement = {
  readonly queryID: string;
  readonly pipeline: Pipeline;
  /** The number of changes in the diff to the tables the pipeline reads. */
  readonly numChanges: number;
  /** The driver's time spent on the pipeline in this advancement. */
  advanceMs: number;
  /** The part of `advanceMs` spent in the change numbered `changeSeq`. */
  changeMs: number;
  changeSeq: number;
  dropped: DropPipelineSignal | undefined;
};

type AdvanceContext = {
  readonly timer: Timer;
  readonly totalHydrationTimeMs: number;
  readonly numChanges: number;
  currentChangeStartMs: number | undefined;
  pos: number;

  // Per-pipeline accounting.
  //
  // The driver's time is charged to the pipeline in the `current` slot. The
  // slot is set while a pipeline's guard pushes or reconciles a change, and
  // while the pipeline's output is streamed (which fetches the rows of lazy
  // relationships). Each time the slot changes, the time since `mark` is
  // charged to the pipeline that held it. The time the consumer of
  // advance() spends between pulls is charged to no pipeline, and neither is
  // time with an empty slot (e.g. reading the diff).

  /** Whether pipelines are dropped when they go over their own budget. */
  readonly partialReset: boolean;
  readonly changesByTable: ReadonlyMap<string, number>;
  /** The number of changes processed so far, by table. */
  readonly posByTable: Map<string, number>;
  /** Numbers the changes of the diff, starting at 1. */
  changeSeq: number;
  readonly pipelines: Map<string, PipelineAdvancement>;
  current: PipelineAdvancement | undefined;
  mark: number;
  /** The sum of the advancement budgets of the pipelines. */
  readonly totalBudgetMs: number;
  /** The sum of the advancement budgets of the dropped pipelines. */
  droppedBudgetMs: number;
  /** Pipelines dropped in the current change, destroyed at its end. */
  readonly toDestroy: string[];
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
const MIN_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES = 8;
const PROJECTED_ADVANCEMENT_SAMPLE_FRACTION = 0.25;
const MAX_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES = 50;
const MIN_PROJECTED_ADVANCEMENT_SAMPLE_MS = 5;
const MIN_PROJECTED_ADVANCEMENT_CHANGES = 16;
const PROJECTED_ADVANCEMENT_RESET_MULTIPLIER = 1.5;
const LATE_ADVANCEMENT_FINISH_PROGRESS = 0.8;
/**
 * When the pipelines dropped in an advancement account for more than this
 * fraction of the client group's hydration time, the whole group is reset
 * instead, as rebuilding the dropped pipelines would cost about as much.
 */
const PARTIAL_RESET_ESCALATION_FRACTION = 0.5;

function randomID() {
  return randInt(1, Number.MAX_SAFE_INTEGER).toString(36);
}

function projectedAdvancementTimeMs(
  elapsedMs: number,
  processedChanges: number,
  numChanges: number,
): number | undefined {
  if (processedChanges <= 0 || numChanges <= 0) {
    return undefined;
  }
  return (elapsedMs / processedChanges) * numChanges;
}

function advancementResetTimeLimitMs(totalHydrationTimeMs: number): number {
  return Math.max(totalHydrationTimeMs, 1);
}

function minProjectedAdvancementSampleChanges(numChanges: number): number {
  return Math.max(
    MIN_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES,
    Math.min(
      MAX_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES,
      Math.ceil(numChanges * PROJECTED_ADVANCEMENT_SAMPLE_FRACTION),
    ),
  );
}

function shouldResetProjectedAdvancement(
  elapsedMs: number,
  projectedTotalTimeMs: number | undefined,
  processedChanges: number,
  numChanges: number,
  totalHydrationTimeMs: number,
): boolean {
  if (
    projectedTotalTimeMs === undefined ||
    numChanges < MIN_PROJECTED_ADVANCEMENT_CHANGES ||
    processedChanges < minProjectedAdvancementSampleChanges(numChanges) ||
    elapsedMs < MIN_PROJECTED_ADVANCEMENT_SAMPLE_MS
  ) {
    return false;
  }

  return (
    projectedTotalTimeMs >
    advancementResetTimeLimitMs(totalHydrationTimeMs) *
      PROJECTED_ADVANCEMENT_RESET_MULTIPLIER
  );
}

function shouldFinishLateAdvancement(
  processedChanges: number,
  numChanges: number,
): boolean {
  return (
    numChanges > 0 &&
    processedChanges / numChanges >= LATE_ADVANCEMENT_FINISH_PROGRESS
  );
}

function shouldResetSlowCurrentChange(
  currentChangeElapsedMs: number,
  totalHydrationTimeMs: number,
): boolean {
  return (
    currentChangeElapsedMs > MIN_ADVANCEMENT_TIME_LIMIT_MS &&
    currentChangeElapsedMs > advancementResetTimeLimitMs(totalHydrationTimeMs)
  );
}

type AdvancementOverrun = {
  readonly reason: PartialResetReason;
  readonly message: string;
};

/**
 * Checks whether an advancement should be abandoned because it is projected
 * to take longer than a hydration: either the whole batch projects to be more
 * expensive than hydration, or the current source change alone exceeds the
 * hydration budget. The late-finish exception only applies to batch-level
 * checks; a single pathological push always fails.
 *
 * This applies to a client group (all of its pipelines, with the sum of their
 * hydration times as the budget) and to a single pipeline (with its own).
 *
 * @param budgetName describes `hydrationTimeMs` in the returned message.
 */
function checkAdvancementBudget(
  elapsed: number,
  currentChangeElapsedMs: number | undefined,
  pos: number,
  numChanges: number,
  hydrationTimeMs: number,
  budgetName: string,
): AdvancementOverrun | undefined {
  // Only built for a failed check, as this runs for every row fetched.
  const limit = () =>
    ` Advancement time limited based on ${budgetName} of ` +
    `${hydrationTimeMs} ms.`;
  if (
    currentChangeElapsedMs !== undefined &&
    shouldResetSlowCurrentChange(currentChangeElapsedMs, hydrationTimeMs)
  ) {
    return {
      reason: 'slow-change',
      message:
        `Advancement exceeded timeout processing current change at ${pos} of ` +
        `${numChanges} changes after ${currentChangeElapsedMs} ms ` +
        `(${elapsed} ms total).` +
        limit(),
    };
  }
  const projectedTotalTimeMs = projectedAdvancementTimeMs(
    elapsed,
    pos,
    numChanges,
  );
  const shouldFinish = shouldFinishLateAdvancement(pos, numChanges);
  if (
    !shouldFinish &&
    shouldResetProjectedAdvancement(
      elapsed,
      projectedTotalTimeMs,
      pos,
      numChanges,
      hydrationTimeMs,
    )
  ) {
    const projection =
      projectedTotalTimeMs === undefined
        ? ''
        : ` Projected total advancement time is ${projectedTotalTimeMs} ms.`;
    return {
      reason: 'projected-overrun',
      message:
        `Advancement projected to exceed hydration time at ${pos} of ` +
        `${numChanges} changes after ${elapsed} ms.` +
        projection +
        limit(),
    };
  }
  if (
    !shouldFinish &&
    elapsed > MIN_ADVANCEMENT_TIME_LIMIT_MS &&
    (elapsed > hydrationTimeMs ||
      (elapsed > hydrationTimeMs / 2 && pos <= numChanges / 2))
  ) {
    return {
      reason: 'timeout',
      message:
        `Advancement exceeded timeout at ${pos} of ${numChanges} changes ` +
        `after ${elapsed} ms.` +
        limit(),
    };
  }
  return undefined;
}

/**
 * Manages the state of IVM pipelines for a given ViewSyncer (i.e. client group).
 */
export class PipelineDriver {
  readonly #tables = new Map<string, TableSource>();
  // Query id to pipeline
  readonly #pipelines = new Map<string, Pipeline>();
  /**
   * XOR signature of the set of rows currently attached to each active
   * query, maintained as RowChanges are yielded from {@link addQuery} and
   * {@link advance}. ADDs / REMOVEs XOR the row's unit in (XOR is
   * self-inverse, so one op serves both directions); EDITs are no-ops.
   * Hydration implicitly reseeds from `0n` because {@link addQuery} calls
   * {@link removeQuery} first, which deletes the entry.
   */
  readonly #rowSetSignatures = new Map<string, bigint>();

  readonly #lc: LogContext;
  readonly #snapshotter: Snapshotter;
  readonly #storage: ClientGroupStorage;
  readonly #shardID: ShardID;
  readonly #logConfig: LogConfig;
  readonly #config: ZeroConfig | undefined;
  readonly #tableSpecs = new Map<string, LiteAndZqlSpec>();
  readonly #allTableNames = new Set<string>();
  readonly #costModels: WeakMap<Database, ConnectionCostModel> | undefined;
  readonly #yieldThresholdMs: () => number;
  #streamer: Streamer | null = null;
  #hydrateContext: HydrateContext | null = null;
  #advanceContext: AdvanceContext | null = null;
  /**
   * The pipelines dropped by the last {@link advance}. Kept until the next
   * advance() or reset(). See {@link droppedQueries}.
   */
  #droppedQueries = new Map<string, DroppedQuery>();
  readonly #guardDelegate: PipelineGuardDelegate = {
    dropped: queryID => this.#advanceContext?.pipelines.get(queryID)?.dropped,
    enter: queryID => this.#enterPipeline(queryID),
    exit: previous => this.#exitPipeline(previous),
    checkAtExit: queryID => this.#checkPipelineAtExit(queryID),
    drop: signal => this.#dropPipeline(signal),
  };
  #replicaVersion: string | null = null;
  #primaryKeys: Map<string, PrimaryKey> | null = null;
  #permissions: LoadedPermissions | null = null;

  readonly #advanceTime = getOrCreateLatencyHistogram(
    'sync',
    'ivm.advance-time',
    'Time to advance all queries for a given client group in response to a single change.',
  );

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
    enablePlanner?: boolean,
    config?: ZeroConfig,
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
    for (const [queryID, pipeline] of this.#pipelines) {
      this.#pipelines.delete(queryID);
      this.#destroyPipeline(queryID, pipeline, 'reset');
    }
    this.#tables.clear();
    this.#allTableNames.clear();
    this.#rowSetSignatures.clear();
    this.#droppedQueries = new Map();
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
    this.#allTableNames.clear();
    for (const table of fullTables.keys()) {
      this.#allTableNames.add(table);
    }
    const primaryKeys = this.#primaryKeys ?? new Map<string, PrimaryKey>();
    this.#primaryKeys = primaryKeys;
    primaryKeys.clear();
    for (const [table, spec] of this.#tableSpecs.entries()) {
      primaryKeys.set(table, spec.tableSpec.primaryKey);
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

  /**
   * Advances the snapshot to the head of the database without diffing the
   * change log, in preparation for hydrating queries at head.
   *
   * Throws a {@link ResetPipelinesSignal} if the change log records a
   * schema change since the previous snapshot. The table specs (and any
   * TableSources built from them) were computed at or before that snapshot
   * and are stale with respect to the new head, so the caller must
   * {@link reset()} before hydrating. ({@link advance()} detects this when
   * the diff encounters the RESET op; this path skips the diff and so must
   * check explicitly.)
   */
  advanceWithoutDiff(): string {
    const {prev, curr} = this.#snapshotter.advanceWithoutDiff();
    if (curr.schemaChangedSince(prev.version)) {
      throw new ResetPipelinesSignal(
        `schema changed between ${prev.version} and ${curr.version}`,
        'schema-change',
      );
    }
    for (const table of this.#tables.values()) {
      table.setDB(curr.db.db);
    }
    return curr.version;
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
    for (const [queryID, pipeline] of this.#pipelines) {
      this.#pipelines.delete(queryID);
      this.#destroyPipeline(queryID, pipeline, 'destroy');
    }
    this.#tables.clear();
    this.#rowSetSignatures.clear();
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
   * The pipelines that the last {@link advance} dropped for going over their
   * advancement budget, by query ID, once its changes have been consumed.
   * Dropped pipelines are destroyed during the advancement, so the caller
   * must add them again (at the new head) to keep them.
   *
   * This is kept until the next advance() or {@link reset()}, so that a
   * reset that follows can reuse the dropped queries' ASTs.
   *
   * Pipelines are dropped only when partial pipeline resets are enabled.
   */
  droppedQueries(): ReadonlyMap<string, DroppedQuery> {
    return this.#droppedQueries;
  }

  get #partialReset(): boolean {
    return this.#config?.partialPipelineReset === true;
  }

  #logQueryPipelineLifecycle({
    zeroEvent,
    pipelineRunID,
    queryHash,
    transformationHash,
    queryName,
    hydrationReason,
    stopReason,
    hydrationTimeMs,
    ivmHydrationTimeMs,
    hydrationRowCount,
    pipelineLifetimeMs,
  }: QueryPipelineLifecycleLog): void {
    let lc = this.#lc
      .withContext('zeroEvent', zeroEvent)
      .withContext('pipelineRunID', pipelineRunID)
      .withContext('queryHash', queryHash)
      .withContext('transformationHash', transformationHash);
    if (queryName !== undefined) {
      lc = lc.withContext('queryName', queryName);
    }
    if (hydrationReason !== undefined) {
      lc = lc.withContext('hydrationReason', hydrationReason);
    }
    if (stopReason !== undefined) {
      lc = lc.withContext('stopReason', stopReason);
    }
    if (hydrationTimeMs !== undefined) {
      lc = lc.withContext('hydrationTimeMs', hydrationTimeMs);
    }
    if (ivmHydrationTimeMs !== undefined) {
      lc = lc.withContext('ivmHydrationTimeMs', ivmHydrationTimeMs);
    }
    if (hydrationRowCount !== undefined) {
      lc = lc.withContext('hydrationRowCount', hydrationRowCount);
    }
    if (pipelineLifetimeMs !== undefined) {
      lc = lc.withContext('pipelineLifetimeMs', pipelineLifetimeMs);
    }
    lc.info?.('query pipeline lifecycle');
  }

  /**
   * A `{scalar: true}` that cannot be honored degrades silently to a plain
   * EXISTS, so the author gets none of the plan they asked for and no signal
   * that they didn't. Say so, with the unique keys that were actually
   * available — the client schema knows only primary keys, so this is the only
   * place the advice can be correct.
   */
  #warnIgnoredScalarHints(queryID: string, hints: IgnoredScalarHint[]): void {
    for (const {table, uniqueKeys} of hints) {
      const keys = uniqueKeys.map(k => `(${k.join(', ')})`).join(', ');
      this.#lc.warn?.(
        `Ignoring {scalar: true} on the "${table}" subquery of query ` +
          `${queryID}: it does not constrain every column of any unique key ` +
          `${keys.length > 0 ? `[${keys}]` : '(none on this table)'} to a ` +
          `literal with "=", so it is not provably limited to one row. ` +
          `The gate runs as a plain EXISTS.`,
      );
    }
  }

  #disableCorrelatedPredicatePushdown(): boolean {
    return this.#config?.enableCorrelatedPredicatePushdown === false;
  }

  /**
   * @param queryID The query that owns the companion pipelines. Their guards
   *        use its ID, so that their advancement time is charged to it and
   *        they are dropped with it.
   * @param getSource Records the tables that the companions read.
   */
  #resolveScalarSubqueries(
    ast: AST,
    queryID: string,
    getSource: (name: string) => Source,
  ): {
    ast: AST;
    companionRows: {table: string; row: Row}[];
    companions: CompanionSubquery[];
    companionInputs: Input[];
    ignoredScalarHints: IgnoredScalarHint[];
  } {
    const companionRows: {table: string; row: Row}[] = [];
    const companionInputs: Input[] = [];

    const executor = (
      subqueryAST: AST,
      childField: string,
    ): LiteralValue | null | undefined => {
      const input = buildPipeline(
        subqueryAST,
        {
          disableCorrelatedPredicatePushdown:
            this.#disableCorrelatedPredicatePushdown(),
          getSource,
          createStorage: () => this.#createStorage(),
          decorateSourceInput: (input: SourceInput): Input =>
            new PipelineGuard(input, queryID, this.#guardDelegate),
          decorateInput: input => input,
          addEdge() {},
          decorateFilterInput: input => input,
        },
        'scalar-subquery',
      );
      // Tracked before it is fetched so that a failure in this or a later
      // subquery can tear it down below. A companion with no result is kept
      // alive too: it detects a future insert that creates the row.
      companionInputs.push(input);
      // Consume the full stream rather than using first() to avoid
      // triggering early return on Take's #initialFetch assertion.
      // The subquery AST already has limit: 1, so at most one row is produced.
      let node: Node | undefined;
      for (const n of skipYields(input.fetch({}))) {
        node ??= n;
      }
      if (!node) {
        return undefined;
      }
      companionRows.push({table: subqueryAST.table, row: node.row as Row});
      return (node.row[childField] as LiteralValue) ?? null;
    };

    let resolved: AST;
    let companions: CompanionSubquery[];
    let ignoredScalarHints: IgnoredScalarHint[];
    try {
      ({
        ast: resolved,
        companions,
        ignoredScalarHints,
      } = resolveSimpleScalarSubqueries(ast, this.#tableSpecs, executor));
    } catch (e) {
      for (const input of companionInputs) {
        input.destroy();
      }
      throw e;
    }
    return {
      ast: resolved,
      companionRows,
      companions,
      companionInputs,
      ignoredScalarHints,
    };
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
  addQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
    queryName?: string,
    hydrationReason: PipelineHydrationReason = 'query-set-sync',
  ): Iterable<RowChange | 'yield'> {
    return this.#trackRowSetSignatures(
      this.#addQueryImpl(
        transformationHash,
        queryID,
        query,
        timer,
        queryName,
        hydrationReason,
      ),
    );
  }

  *#addQueryImpl(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
    queryName?: string,
    hydrationReason: PipelineHydrationReason = 'query-set-sync',
  ): Iterable<RowChange | 'yield'> {
    assert(
      this.initialized(),
      'Pipeline driver must be initialized before adding queries',
    );
    this.removeQuery(queryID, 'replace-query');
    const pipelineRunID = randomID();
    this.#logQueryPipelineLifecycle({
      zeroEvent: 'query-pipeline-hydrate-start',
      pipelineRunID,
      queryHash: queryID,
      transformationHash,
      queryName,
      hydrationReason,
    });
    const debugDelegate = runtimeDebugFlags.trackRowsVended
      ? new Debug(true)
      : undefined;

    const costModel = this.#ensureCostModelExistsIfEnabled(
      this.#snapshotter.current().db.db,
    );

    assert(
      this.#advanceContext === null,
      'Cannot hydrate while advance is in progress',
    );
    this.#hydrateContext = {
      timer,
    };
    let hydrationFinished = false;
    let hydrationFailed = false;
    let hydrationRowCount = 0;
    // The driver's own hydration time: the time between pulls, when the
    // consumer processes the rows, is not counted.
    let ivmHydrationTimeMs = 0;
    let mark = timer.totalElapsed();
    const pause = () => {
      ivmHydrationTimeMs += timer.totalElapsed() - mark;
    };
    const resume = () => {
      mark = timer.totalElapsed();
    };
    const tables = new Set<string>();
    const getSource = (name: string) => {
      tables.add(name);
      return this.#getSource(name);
    };
    // The inputs built so far, held outside the try so that a hydration that
    // does not finish (aborted by the consumer or failed) can tear them down.
    // Only a finished hydration hands them over to #pipelines.
    let builtInputs: Input[] = [];
    try {
      const {
        ast: resolvedQuery,
        companionRows,
        companions: companionMeta,
        companionInputs,
        ignoredScalarHints,
      } = this.#resolveScalarSubqueries(query, queryID, getSource);
      builtInputs = [...companionInputs];

      this.#warnIgnoredScalarHints(queryID, ignoredScalarHints);

      const input = buildPipeline(
        resolvedQuery,
        {
          debug: debugDelegate,
          enableNotExists: true, // Server-side can handle NOT EXISTS
          disableCorrelatedPredicatePushdown:
            this.#disableCorrelatedPredicatePushdown(),
          enablePlannerAwarePushdown:
            this.#config?.enablePlannerAwarePushdown !== false,
          getSource,
          createStorage: () => this.#createStorage(),
          // The guard is next to the pipeline, so that it catches a
          // DropPipelineSignal before the operators that wrap it see it.
          decorateSourceInput: (input: SourceInput, _queryID: string): Input =>
            new PipelineGuard(
              new MeasurePushOperator(
                new QueryFailureLoggingOperator(
                  this.#lc,
                  input,
                  queryID,
                  transformationHash,
                  queryName,
                ),
                queryID,
                this.#inspectorDelegate,
                'query-update-server',
              ),
              queryID,
              this.#guardDelegate,
            ),
          decorateInput: input => input,
          addEdge() {},
          decorateFilterInput: input => input,
        },
        queryID,
        costModel,
      );
      builtInputs.push(input);
      const schema = input.getSchema();
      input.setOutput({
        push: change => this.#streamPushed(queryID, schema, change),
      });

      for (const change of hydrateInternal(
        input,
        queryID,
        must(this.#primaryKeys),
        this.#tableSpecs,
      )) {
        if (change !== 'yield') {
          hydrationRowCount++;
        }
        pause();
        yield change;
        resume();
      }

      for (const {table, row} of companionRows) {
        const primaryKey = mustGetPrimaryKey(this.#primaryKeys, table);
        hydrationRowCount++;
        pause();
        yield {
          type: ChangeType.ADD,
          queryID,
          table,
          rowKey: getRowKey(primaryKey, row),
          row,
        } as RowChange;
        resume();
      }

      pause();
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

      // Set up live companion pipelines for reactive scalar subquery monitoring.
      const liveCompanions: CompanionPipeline[] = [];
      for (let i = 0; i < companionMeta.length; i++) {
        const meta = companionMeta[i];
        const companionInput = companionInputs[i];
        const companionSchema = companionInput.getSchema();
        const {childField, resolvedValue} = meta;
        companionInput.setOutput({
          push: (change: Change) => {
            let newValue: LiteralValue | null | undefined;
            switch (change[ChangeIndex.TYPE]) {
              case ChangeType.ADD:
              case ChangeType.EDIT:
                newValue =
                  (change[ChangeIndex.NODE].row[childField] as LiteralValue) ??
                  null;
                break;
              case ChangeType.REMOVE:
                newValue = undefined;
                break;
              case ChangeType.CHILD:
                return [];
            }
            if (!scalarValuesEqual(newValue, resolvedValue)) {
              throw new ResetPipelinesSignal(
                `Scalar subquery value changed for ${meta.ast.table}: ` +
                  `${String(resolvedValue)} -> ${String(newValue)}`,
                'scalar-subquery',
              );
            }
            return this.#streamPushed(queryID, companionSchema, change);
          },
        });
        liveCompanions.push({input: companionInput, childField, resolvedValue});
      }

      // Note: This hydrationTime is a wall-clock overestimate, as it does
      // not take time slicing into account. The view-syncer resets this
      // to a more precise processing-time measurement with setHydrationTime().
      const pipelineReadyAtMs = Date.now();
      this.#pipelines.set(queryID, {
        input,
        hydrationTimeMs,
        ivmHydrationTimeMs,
        tables,
        hydrationRowCount,
        hydrationReason,
        pipelineRunID,
        pipelineReadyAtMs,
        transformedAst: resolvedQuery,
        originalAst: query,
        transformationHash,
        ...(queryName !== undefined && {queryName}),
        companions: liveCompanions,
      });
      hydrationFinished = true;
      this.#logQueryPipelineLifecycle({
        zeroEvent: 'query-pipeline-hydrate-finish',
        pipelineRunID,
        queryHash: queryID,
        transformationHash,
        queryName,
        hydrationReason,
        hydrationTimeMs,
        ivmHydrationTimeMs,
        hydrationRowCount,
      });
    } catch (e) {
      hydrationFailed = true;
      this.#logQueryPipelineLifecycle({
        zeroEvent: 'query-pipeline-hydrate-failed',
        pipelineRunID,
        queryHash: queryID,
        transformationHash,
        queryName,
        hydrationReason,
        hydrationTimeMs: timer.totalElapsed(),
        hydrationRowCount,
      });
      logQueryFailure(
        this.#lc,
        {queryHash: queryID, transformationHash, queryName},
        'query hydration failed',
        e,
      );
      throw e;
    } finally {
      if (!hydrationFinished && !hydrationFailed) {
        this.#logQueryPipelineLifecycle({
          zeroEvent: 'query-pipeline-hydrate-aborted',
          pipelineRunID,
          queryHash: queryID,
          transformationHash,
          queryName,
          hydrationReason,
          hydrationTimeMs: timer.totalElapsed(),
          hydrationRowCount,
        });
      }
      if (!hydrationFinished) {
        for (const input of builtInputs) {
          input.destroy();
        }
        this.#pruneUnusedTables();
        // Rows may already have been yielded through #trackRowSetSignatures,
        // and rowSetSignature() must not report a signature for a query
        // without an active pipeline.
        this.#rowSetSignatures.delete(queryID);
      }
      this.#hydrateContext = null;
    }
  }

  /**
   * Removes the pipeline for the query. This is a no-op if the query
   * was not added.
   */
  removeQuery(
    queryID: string,
    stopReason: PipelineStopReason = 'remove-query',
  ) {
    const pipeline = this.#pipelines.get(queryID);
    if (pipeline) {
      this.#pipelines.delete(queryID);
      this.#destroyPipeline(queryID, pipeline, stopReason);
      this.#pruneUnusedTables();
    }
    this.#rowSetSignatures.delete(queryID);
  }

  #pruneUnusedTables() {
    for (const [table, source] of this.#tables.entries()) {
      if (!source.hasConnections()) {
        this.#tables.delete(table);
      }
    }
  }

  #destroyPipeline(
    queryID: string,
    pipeline: Pipeline,
    stopReason: PipelineStopReason,
  ): void {
    this.#logQueryPipelineLifecycle({
      zeroEvent: 'query-pipeline-stop',
      pipelineRunID: pipeline.pipelineRunID,
      queryHash: queryID,
      transformationHash: pipeline.transformationHash,
      queryName: pipeline.queryName,
      hydrationReason: pipeline.hydrationReason,
      stopReason,
      hydrationTimeMs: pipeline.hydrationTimeMs,
      hydrationRowCount: pipeline.hydrationRowCount,
      pipelineLifetimeMs: Date.now() - pipeline.pipelineReadyAtMs,
    });
    pipeline.input.destroy();
    for (const companion of pipeline.companions) {
      companion.input.destroy();
    }
  }

  /**
   * Current XOR signature of the row-set attached to `queryID`, or
   * `undefined` if no pipeline for the query is currently active.
   * Maintained incrementally by {@link addQuery} and {@link advance}.
   */
  rowSetSignature(queryID: string): bigint | undefined {
    return this.#rowSetSignatures.get(queryID);
  }

  /**
   * Wraps an iterable of RowChanges, XORing each row's unit hash into the
   * query's signature (ADDs and REMOVEs share the same op; EDITs are no-ops).
   * Used to intercept the yield streams from {@link addQuery} and
   * {@link advance}.
   */
  *#trackRowSetSignatures(
    changes: Iterable<RowChange | 'yield'>,
  ): Iterable<RowChange | 'yield'> {
    for (const change of changes) {
      if (change !== 'yield' && change.type !== ChangeType.EDIT) {
        const cur = this.#rowSetSignatures.get(change.queryID) ?? 0n;
        const unit = rowIDSignatureUnit({
          schema: '',
          table: change.table,
          rowKey: change.rowKey as RowKey,
        });
        this.#rowSetSignatures.set(change.queryID, cur ^ unit);
      }
      yield change;
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
   * With partial pipeline resets enabled, a pipeline that goes over its own
   * advancement budget is dropped: it stops receiving changes, its partial
   * output for the change it was dropped in is discarded, and it is
   * destroyed at the end of that change. The other pipelines advance to the
   * end. Once the `changes` have been consumed, {@link droppedQueries}
   * returns the dropped pipelines, which the caller must add again.
   *
   * @param timer The caller-controlled {@link Timer} that will be used to
   *        measure the progress of the advancement and abort with a
   *        {@link ResetPipelinesSignal} if it is estimated to take longer
   *        than a hydration.
   * @return The resulting row changes for all added queries. Note that the
   *         `changes` must be iterated over in their entirety in order to
   *         advance the database snapshot.
   */
  advance(timer: Timer): {
    version: string;
    numChanges: number;
    changes: Iterable<RowChange | 'yield'>;
  } {
    assert(
      this.initialized(),
      'Pipeline driver must be initialized before advancing',
    );
    const diff = this.#snapshotter.advance(
      this.#tableSpecs,
      this.#allTableNames,
      this.#tables,
      // Sources skip changes that none of this client group's pipelines can
      // observe, so a `prev` they write to diverges from other groups'.
      'divergent',
    );
    const {prev, curr, changes} = diff;
    this.#lc.debug?.(
      `advance ${prev.version} => ${curr.version}: ${changes} changes`,
    );
    this.#droppedQueries = new Map();

    return {
      version: curr.version,
      numChanges: changes,
      changes: this.#trackRowSetSignatures(
        this.#excludeConsumerTime(this.#advance(diff, timer, changes)),
      ),
    };
  }

  /**
   * Stops charging the driver's time to pipelines while the consumer of
   * {@link advance} processes a change (e.g. to update the CVR for a batch
   * of rows from many pipelines).
   */
  *#excludeConsumerTime(
    changes: Iterable<RowChange | 'yield'>,
  ): Iterable<RowChange | 'yield'> {
    for (const change of changes) {
      this.#charge();
      yield change;
      const ctx = this.#advanceContext;
      if (ctx) {
        ctx.mark = ctx.timer.totalElapsed();
      }
    }
  }

  *#advance(
    diff: SnapshotDiff,
    timer: Timer,
    numChanges: number,
  ): Iterable<RowChange | 'yield'> {
    assert(
      this.#hydrateContext === null,
      'Cannot advance while hydration is in progress',
    );
    const totalHydrationTimeMs = this.totalHydrationTimeMs();
    let totalBudgetMs = 0;
    for (const pipeline of this.#pipelines.values()) {
      totalBudgetMs += advancementResetTimeLimitMs(pipeline.ivmHydrationTimeMs);
    }
    this.#advanceContext = {
      timer,
      totalHydrationTimeMs,
      numChanges,
      currentChangeStartMs: undefined,
      pos: 0,
      partialReset: this.#partialReset,
      changesByTable: diff.changesByTable,
      posByTable: new Map(),
      changeSeq: 0,
      pipelines: new Map(),
      current: undefined,
      mark: timer.totalElapsed(),
      totalBudgetMs,
      droppedBudgetMs: 0,
      toDestroy: [],
    };
    this.#lc.debug?.(
      `starting pipeline advancement of ${numChanges} changes with an ` +
        `advancement time limited based on total hydration time of ` +
        `${totalHydrationTimeMs} ms.`,
    );
    try {
      for (const {table, prevValues, nextValue} of diff) {
        // Advance progress is checked each time a row is fetched
        // from a TableSource during push processing, but some pushes
        // don't read any rows.  Check progress here before processing
        // the next change.
        if (this.#shouldAdvanceYieldMaybeAbortAdvance()) {
          yield 'yield';
        }
        const start = timer.totalElapsed();
        const advanceContext = must(this.#advanceContext);
        advanceContext.currentChangeStartMs = start;
        advanceContext.changeSeq++;

        try {
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
                yield* this.#push(
                  tableSource,
                  makeSourceChangeRemove(prevValue as Row),
                );
              }
            }
            if (nextValue) {
              if (editOldRow) {
                yield* this.#push(
                  tableSource,
                  makeSourceChangeEdit(nextValue as Row, editOldRow),
                );
              } else {
                yield* this.#push(
                  tableSource,
                  makeSourceChangeAdd(nextValue as Row),
                );
              }
            }
          } finally {
            advanceContext.pos++;
            const {posByTable} = advanceContext;
            posByTable.set(table, (posByTable.get(table) ?? 0) + 1);
          }

          this.#shouldAdvanceYieldMaybeAbortAdvance(false);
        } finally {
          advanceContext.currentChangeStartMs = undefined;
        }

        // The change has been pushed to every connection and its output has
        // been streamed, so the pipelines dropped while processing it can be
        // destroyed.
        this.#destroyDroppedPipelines(advanceContext);

        const elapsed = timer.totalElapsed() - start;
        this.#advanceTime.recordMs(elapsed, {
          table,
        });
      }

      // Set the new snapshot on all TableSources.
      const {curr} = diff;
      for (const table of this.#tables.values()) {
        table.setDB(curr.db.db);
      }
      this.#ensureCostModelExistsIfEnabled(curr.db.db);
      this.#lc.debug?.(`Advanced to ${curr.version}`);
    } finally {
      const advanceContext = this.#advanceContext;
      this.#advanceContext = null;
      if (advanceContext) {
        // A dropped pipeline holds partially applied state, so it is never
        // left behind, even when the advancement is abandoned.
        this.#destroyDroppedPipelines(advanceContext);
      }
    }
  }

  /** Implements `BuilderDelegate.getSource()` */
  #getSource(tableName: string): Source {
    return getOrInsertComputed(this.#tables, tableName, tableName => {
      const tableSpec = mustGetTableSpec(this.#tableSpecs, tableName);
      const primaryKey = mustGetPrimaryKey(this.#primaryKeys, tableName);

      const {db} = this.#snapshotter.current();
      const source = new TableSource(
        this.#lc,
        this.#logConfig,
        db.db,
        tableName,
        tableSpec.zqlSpec,
        primaryKey,
        () => this.#shouldYield(),
        // Pipelines only read tables through their connections, and the
        // sources are moved to the next snapshot after every advancement.
        {skipUnobservableChanges: true},
      );
      this.#lc.debug?.(`created TableSource for ${tableName}`);
      return source;
    });
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
   * Cancels advancement processing when the client group goes over its
   * budget (see {@link checkAdvancementBudget}), by throwing a
   * {@link ResetPipelinesSignal}.
   *
   * With partial pipeline resets enabled, first checks the pipeline that the
   * driver is working on against its own budget, and drops it by throwing a
   * {@link DropPipelineSignal}. A pipeline that does no work cannot go over
   * its budget, so only the current one needs to be checked.
   */
  #shouldAdvanceYieldMaybeAbortAdvance(checkYield = true): boolean {
    const ctx = must(this.#advanceContext);
    const {
      currentChangeStartMs,
      pos,
      numChanges,
      timer: advanceTimer,
      totalHydrationTimeMs,
    } = ctx;
    const elapsed = advanceTimer.totalElapsed();
    const currentChangeElapsedMs =
      currentChangeStartMs === undefined
        ? undefined
        : elapsed - currentChangeStartMs;
    const overrun = checkAdvancementBudget(
      elapsed,
      currentChangeElapsedMs,
      pos,
      numChanges,
      totalHydrationTimeMs,
      'total hydration time',
    );
    if (overrun) {
      throw new ResetPipelinesSignal(overrun.message, 'advancement-timeout');
    }
    const {current} = ctx;
    if (current) {
      this.#charge(elapsed);
      const drop = this.#checkPipeline(ctx, current);
      if (drop) {
        throw drop;
      }
    }
    return checkYield && advanceTimer.elapsedLap() > this.#yieldThresholdMs();
  }

  /** Charges the driver's time since the last charge to the current pipeline. */
  #charge(now?: number): void {
    const ctx = this.#advanceContext;
    if (!ctx) {
      return;
    }
    now ??= ctx.timer.totalElapsed();
    const {current} = ctx;
    if (current) {
      const ms = now - ctx.mark;
      current.advanceMs += ms;
      if (current.changeSeq !== ctx.changeSeq) {
        current.changeSeq = ctx.changeSeq;
        current.changeMs = 0;
      }
      current.changeMs += ms;
    }
    ctx.mark = now;
  }

  /**
   * Charges the driver's time to the pipeline of `queryID` until
   * {@link #exitPipeline} is called with the returned (previous) pipeline.
   */
  #enterPipeline(queryID: string): PipelineAdvancement | undefined {
    const ctx = this.#advanceContext;
    if (!ctx) {
      return undefined;
    }
    this.#charge();
    const previous = ctx.current;
    ctx.current = getOrInsertComputed(ctx.pipelines, queryID, queryID => {
      const pipeline = must(
        this.#pipelines.get(queryID),
        `No pipeline for query ${queryID}`,
      );
      let numChanges = 0;
      for (const table of pipeline.tables) {
        numChanges += ctx.changesByTable.get(table) ?? 0;
      }
      return {
        queryID,
        pipeline,
        numChanges,
        advanceMs: 0,
        changeMs: 0,
        changeSeq: ctx.changeSeq,
        dropped: undefined,
      };
    });
    return previous;
  }

  #exitPipeline(previous: PipelineAdvancement | undefined): void {
    const ctx = this.#advanceContext;
    if (!ctx) {
      return;
    }
    this.#charge();
    ctx.current = previous;
  }

  /**
   * Checks the pipeline when its work (a push, a reconcile, or the streaming
   * of its output) is done, and records a drop without throwing.
   */
  #checkPipelineAtExit(queryID: string): void {
    const ctx = this.#advanceContext;
    const pipeline = ctx?.pipelines.get(queryID);
    if (!ctx || !pipeline) {
      return;
    }
    this.#charge();
    const drop = this.#checkPipeline(ctx, pipeline);
    if (drop) {
      this.#dropPipeline(drop);
    }
  }

  #checkPipeline(
    ctx: AdvanceContext,
    p: PipelineAdvancement,
  ): DropPipelineSignal | undefined {
    if (!ctx.partialReset || p.dropped) {
      return undefined;
    }
    // The changes to the tables that the pipeline reads, rather than all of
    // the changes, measure its progress: the diff is in commit order, so the
    // changes to its tables may all come early (or late) in the diff.
    let pos = 0;
    for (const table of p.pipeline.tables) {
      pos += ctx.posByTable.get(table) ?? 0;
    }
    const overrun = checkAdvancementBudget(
      p.advanceMs,
      p.changeSeq === ctx.changeSeq ? p.changeMs : undefined,
      pos,
      p.numChanges,
      p.pipeline.ivmHydrationTimeMs,
      'pipeline hydration time',
    );
    return overrun
      ? new DropPipelineSignal(p.queryID, overrun.reason, overrun.message)
      : undefined;
  }

  /**
   * Records that the pipeline of `signal.queryID` is dropped: from now on its
   * guards do not push or reconcile any change into it, its output is
   * discarded, and it is destroyed at the end of the current change.
   *
   * Throws a (whole-group) {@link ResetPipelinesSignal} instead if the
   * dropped pipelines account for most of the group's hydration time.
   */
  #dropPipeline(signal: DropPipelineSignal): void {
    const ctx = must(this.#advanceContext);
    const {queryID} = signal;
    const p = must(ctx.pipelines.get(queryID));
    if (p.dropped) {
      return;
    }
    p.dropped = signal;
    ctx.toDestroy.push(queryID);
    const {pipeline} = p;
    this.#droppedQueries.set(queryID, {
      transformationHash: pipeline.transformationHash,
      transformedAst: pipeline.transformedAst,
      originalAst: pipeline.originalAst,
      ...(pipeline.queryName !== undefined && {queryName: pipeline.queryName}),
      reason: signal.dropReason,
      hydrationTimeMs: pipeline.ivmHydrationTimeMs,
    });

    let lc = this.#lc
      .withContext('queryHash', queryID)
      .withContext('transformationHash', pipeline.transformationHash);
    if (pipeline.queryName !== undefined) {
      lc = lc.withContext('queryName', pipeline.queryName);
    }
    let pos = 0;
    for (const table of pipeline.tables) {
      pos += ctx.posByTable.get(table) ?? 0;
    }
    lc.info?.(`resetting pipeline: ${signal.message}`, {
      reason: signal.dropReason,
      advancementTimeMs: p.advanceMs,
      hydrationTimeMs: pipeline.ivmHydrationTimeMs,
      pos,
      numChanges: p.numChanges,
    });

    ctx.droppedBudgetMs += advancementResetTimeLimitMs(
      pipeline.ivmHydrationTimeMs,
    );
    if (
      ctx.droppedBudgetMs >
      ctx.totalBudgetMs * PARTIAL_RESET_ESCALATION_FRACTION
    ) {
      throw new ResetPipelinesSignal(
        `Dropped pipelines account for ${ctx.droppedBudgetMs} ms of the ` +
          `total hydration time of ${ctx.totalBudgetMs} ms. ` +
          signal.message,
        'advancement-timeout',
      );
    }
  }

  #destroyDroppedPipelines(ctx: AdvanceContext): void {
    for (const queryID of ctx.toDestroy) {
      this.removeQuery(queryID, 'advancement-reset');
    }
    ctx.toDestroy.length = 0;
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
        yield* this.#streamOutput(this.#stopAccumulating());
        this.#startAccumulating();
      }
    } finally {
      if (this.#streamer !== null) {
        this.#stopAccumulating();
      }
    }
  }

  /**
   * Converts a change pushed out of a query pipeline into row changes during
   * the push. The relationships of a pushed node are lazy, and operators such
   * as Join compute them from the state of the push in progress: while a child
   * change is pushed to each matching parent in turn, parents not yet pushed
   * must not see it. Read after the push has moved on to the next parent, or
   * after it has finished, a node's relationships show the later state, and
   * rows that are also pushed separately are counted twice.
   */
  *#streamPushed(
    queryID: string,
    schema: SourceSchema,
    change: Change,
  ): Stream<'yield'> {
    const streamer = this.#streamer;
    assert(streamer, 'must #startAccumulating() before pushing changes');
    for (const rowChange of streamer.streamChange(queryID, schema, change)) {
      if (rowChange === 'yield') {
        yield rowChange;
        continue;
      }
      // #push replaces the streamer after each 'yield', so add to the
      // current one.
      must(this.#streamer).add(rowChange);
    }
  }

  /**
   * Streams the row changes that the pipelines produced during their pushes
   * (see {@link #streamPushed}), without those of a dropped pipeline: its
   * partial output for the change it was dropped in is discarded. The
   * pipelines' guards charged the time to produce them.
   */
  *#streamOutput(streamer: Streamer): Iterable<RowChange | 'yield'> {
    for (const rowChange of streamer.stream()) {
      if (
        rowChange === 'yield' ||
        !this.#guardDelegate.dropped(rowChange.queryID)
      ) {
        yield rowChange;
      }
    }
  }

  #startAccumulating() {
    assert(this.#streamer === null, 'Streamer already started');
    this.#streamer = new Streamer(
      must(this.#primaryKeys),
      this.#tableSpecs,
      (queryID, error) =>
        this.#logQueryFailure(queryID, 'query pipeline failed', error),
    );
  }

  #stopAccumulating(): Streamer {
    const streamer = this.#streamer;
    assert(streamer, 'Streamer not started');
    this.#streamer = null;
    return streamer;
  }

  #logQueryFailure(queryID: string, message: string, error: unknown): void {
    const pipeline = this.#pipelines.get(queryID);
    const queryInfo = pipeline
      ? {
          queryHash: queryID,
          transformationHash: pipeline.transformationHash,
          queryName: pipeline.queryName,
        }
      : undefined;
    logQueryFailure(this.#lc, queryInfo, message, error);
  }
}

class Streamer {
  readonly #primaryKeys: Map<string, PrimaryKey>;
  readonly #tableSpecs: Map<string, LiteAndZqlSpec>;
  readonly #logQueryFailure:
    | ((queryID: string, error: unknown) => void)
    | undefined;

  constructor(
    primaryKeys: Map<string, PrimaryKey>,
    tableSpecs: Map<string, LiteAndZqlSpec>,
    logQueryFailure?: (queryID: string, error: unknown) => void,
  ) {
    this.#primaryKeys = primaryKeys;
    this.#tableSpecs = tableSpecs;
    this.#logQueryFailure = logQueryFailure;
  }

  readonly #changes: [
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | 'yield'>,
  ][] = [];

  /** Row changes that were already produced by {@link streamChange}. */
  readonly #rowChanges: RowChange[] = [];

  add(rowChange: RowChange) {
    this.#rowChanges.push(rowChange);
  }

  streamChange(
    queryID: string,
    schema: SourceSchema,
    change: Change,
  ): Iterable<RowChange | 'yield'> {
    return this.#streamChanges(queryID, schema, [change]);
  }

  accumulate(
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | 'yield'>,
  ): this {
    this.#changes.push([queryID, schema, changes]);
    return this;
  }

  *stream(): Iterable<RowChange | 'yield'> {
    yield* this.#rowChanges;
    for (const [queryID, schema, changes] of this.#changes) {
      try {
        yield* this.#streamChanges(queryID, schema, changes);
      } catch (e) {
        this.#logQueryFailure?.(queryID, e);
        throw e;
      }
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
      const type = change[ChangeIndex.TYPE];
      switch (type) {
        case ChangeType.REMOVE:
        case ChangeType.ADD: {
          yield* this.#streamNodes(queryID, schema, type, () => [
            change[ChangeIndex.NODE],
          ]);
          break;
        }

        case ChangeType.CHILD: {
          const child = change[ChangeIndex.CHILD_DATA];
          const childSchema = must(
            schema.relationships[child.relationshipName],
          );

          yield* this.#streamChanges(queryID, childSchema, [child.change]);
          break;
        }
        case ChangeType.EDIT:
          yield* this.#streamNodes(queryID, schema, type, () => [
            {row: change[ChangeIndex.NODE].row, relationships: {}},
          ]);
          break;
        default:
          unreachable(change[ChangeIndex.TYPE]);
      }
    }
  }

  *#streamNodes(
    queryID: string,
    schema: SourceSchema,
    op: ChangeType.ADD | ChangeType.REMOVE | ChangeType.EDIT,
    nodes: () => Iterable<Node | 'yield'>,
  ): Iterable<RowChange | 'yield'> {
    const {tableName: table, system} = schema;

    const primaryKey = must(this.#primaryKeys.get(table));
    const spec = must(this.#tableSpecs.get(table)).tableSpec;

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
      const {relationships} = node;
      let {row} = node;
      const rowKey = getRowKey(primaryKey, row);
      if (op !== ChangeType.REMOVE) {
        const rowVersion = row[ZERO_VERSION_COLUMN_NAME];
        if (
          typeof rowVersion === 'string' &&
          rowVersion < (spec.minRowVersion ?? '00')
        ) {
          row = {...row, [ZERO_VERSION_COLUMN_NAME]: spec.minRowVersion};
        }
      }

      yield {
        type: op,
        queryID,
        table,
        rowKey,
        row: op === ChangeType.REMOVE ? undefined : row,
      } as RowChange;

      for (const [relationship, children] of Object.entries(relationships)) {
        const childSchema = must(schema.relationships[relationship]);
        yield* this.#streamNodes(queryID, childSchema, op, children);
      }
    }
  }
}

/** How a {@link PipelineGuard} reports to the {@link PipelineDriver}. */
interface PipelineGuardDelegate {
  /**
   * The signal that dropped the pipeline of `queryID` in the current
   * advancement, if any.
   */
  dropped(queryID: string): DropPipelineSignal | undefined;
  /**
   * Charges the driver's time to the pipeline of `queryID`, until
   * {@link exit} is called with the returned value.
   */
  enter(queryID: string): PipelineAdvancement | undefined;
  exit(previous: PipelineAdvancement | undefined): void;
  /** Checks the pipeline's budget, recording a drop without throwing. */
  checkAtExit(queryID: string): void;
  drop(signal: DropPipelineSignal): void;
}

/**
 * Sits between a pipeline and each of its source connections, so that every
 * push and reconcile into the pipeline passes through it. It charges the
 * driver's time to the pipeline while the pipeline processes a change, and
 * it contains a drop of the pipeline: it catches the
 * {@link DropPipelineSignal} that a budget check throws from within the
 * pipeline, and it does not pass any further push or reconcile to a dropped
 * pipeline, so that the source continues with its other connections.
 *
 * Skipping the reconcile of a dropped pipeline is necessary for correctness:
 * a pipeline dropped during a push holds partially applied state, which
 * Take's refill would assert on.
 *
 * The companion pipelines of a query's scalar subqueries use guards with the
 * query's ID, so that they are charged and dropped with it.
 */
class PipelineGuard implements Operator {
  readonly #input: Input;
  readonly #queryID: string;
  readonly #delegate: PipelineGuardDelegate;
  #output: Output = throwOutput;

  constructor(input: Input, queryID: string, delegate: PipelineGuardDelegate) {
    this.#input = input;
    this.#queryID = queryID;
    this.#delegate = delegate;
    input.setOutput(this);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  destroy(): void {
    this.#input.destroy();
  }

  fetch(req: FetchRequest): Stream<Node | 'yield'> {
    return this.#input.fetch(req);
  }

  push(change: Change): Stream<'yield'> {
    return this.#guard(() => this.#output.push(change, this));
  }

  reconcile(_pusher: InputBase): Stream<'yield'> {
    return this.#guard(() => this.#output.reconcile?.(this) ?? []);
  }

  *#guard(work: () => Stream<'yield'>): Stream<'yield'> {
    const delegate = this.#delegate;
    const queryID = this.#queryID;
    if (delegate.dropped(queryID)) {
      return;
    }
    const previous = delegate.enter(queryID);
    try {
      const it = work()[Symbol.iterator]();
      for (;;) {
        // Checked before resuming the pipeline, so that a pipeline dropped
        // while suspended does no more work. This is a safeguard: the
        // pipeline's output is streamed during its push (see
        // PipelineDriver.#streamPushed), so no check runs while it is
        // suspended.
        const dropped = delegate.dropped(queryID);
        if (dropped) {
          // Throwing into the suspended operators, rather than returning
          // from them, unwinds them as an abort. Take and Cap assert, when a
          // fetch returns early, that it was not cut short.
          if (it.throw) {
            it.throw(dropped);
          } else {
            it.return?.();
          }
          return;
        }
        const next = it.next();
        if (next.done) {
          break;
        }
        yield next.value;
      }
      delegate.checkAtExit(queryID);
    } catch (e) {
      if (e instanceof DropPipelineSignal && e.queryID === queryID) {
        delegate.drop(e);
        return;
      }
      throw e;
    } finally {
      delegate.exit(previous);
    }
  }
}

class QueryFailureLoggingOperator implements Input, Output {
  readonly #lc: LogContext;
  readonly #input: Input;
  readonly #queryHash: string;
  readonly #transformationHash: string;
  readonly #queryName: string | undefined;
  #output: Output = throwOutput;

  constructor(
    lc: LogContext,
    input: Input,
    queryHash: string,
    transformationHash: string,
    queryName?: string,
  ) {
    this.#lc = lc;
    this.#input = input;
    this.#queryHash = queryHash;
    this.#transformationHash = transformationHash;
    this.#queryName = queryName;
    input.setOutput(this);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  destroy(): void {
    this.#input.destroy();
  }

  fetch(req: FetchRequest): Iterable<Node | 'yield'> {
    return this.#input.fetch(req);
  }

  *push(change: Change): Iterable<'yield'> {
    try {
      yield* this.#output.push(change, this);
    } catch (e) {
      logQueryFailure(
        this.#lc,
        {
          queryHash: this.#queryHash,
          transformationHash: this.#transformationHash,
          queryName: this.#queryName,
        },
        'query pipeline failed',
        e,
      );
      throw e;
    }
  }

  *reconcile(_pusher: InputBase): Stream<'yield'> {
    if (this.#output.reconcile) {
      try {
        yield* this.#output.reconcile(this);
      } catch (e) {
        logQueryFailure(
          this.#lc,
          {
            queryHash: this.#queryHash,
            transformationHash: this.#transformationHash,
            queryName: this.#queryName,
          },
          'query pipeline failed during reconcile',
          e,
        );
        throw e;
      }
    }
  }
}

function logQueryFailure(
  lc: LogContext,
  queryInfo: QueryLogInfo | undefined,
  message: string,
  error: unknown,
): void {
  if (error instanceof ResetPipelinesSignal) {
    return;
  }
  let queryLC = lc;
  if (queryInfo) {
    queryLC = queryLC
      .withContext('queryHash', queryInfo.queryHash)
      .withContext('transformationHash', queryInfo.transformationHash);
    if (queryInfo.queryName !== undefined) {
      queryLC = queryLC.withContext('queryName', queryInfo.queryName);
    }
  }
  queryLC.error?.(message, error);
}

function* toAdds(nodes: Iterable<Node | 'yield'>): Iterable<Change | 'yield'> {
  for (const node of nodes) {
    if (node === 'yield') {
      yield node;
      continue;
    }
    yield [ChangeType.ADD, node, null];
  }
}

function getRowKey(cols: PrimaryKey, row: Row): RowKey {
  return Object.fromEntries(cols.map(col => [col, must(row[col])]));
}

/**
 * Core hydration logic used by {@link PipelineDriver#addQuery}, extracted to a
 * function for reuse by the analyze-query RPC path so that analysis hydrates
 * queries the same way the view-syncer does in production.
 */
export function hydrate(
  input: Input,
  hash: string,
  clientSchema: ClientSchema,
  tableSpecs: Map<string, LiteAndZqlSpec>,
): Iterable<RowChange | 'yield'> {
  const res = input.fetch({});
  const streamer = new Streamer(
    buildPrimaryKeys(clientSchema),
    tableSpecs,
  ).accumulate(hash, input.getSchema(), toAdds(res));
  return streamer.stream();
}

export function hydrateInternal(
  input: Input,
  hash: string,
  primaryKeys: Map<string, PrimaryKey>,
  tableSpecs: Map<string, LiteAndZqlSpec>,
): Iterable<RowChange | 'yield'> {
  const res = input.fetch({});
  const streamer = new Streamer(primaryKeys, tableSpecs).accumulate(
    hash,
    input.getSchema(),
    toAdds(res),
  );
  return streamer.stream();
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

  const rv = pKeys.get(table);
  assert(
    rv,
    () =>
      // oxlint-disable-next-line e18e/prefer-array-to-sorted
      `table '${table}' is not one of: ${JSON.stringify([...pKeys.keys()].sort())}. ` +
      `Check the spelling and ensure that the table has a primary key.`,
  );
  return rv;
}

/**
 * Compares two scalar subquery resolved values for equality.
 * Unlike `valuesEqual` in data.ts (which treats null != null for join
 * semantics), this uses identity semantics: undefined === undefined
 * (no row matched), null === null (row matched but field was NULL).
 */
function scalarValuesEqual(
  a: LiteralValue | null | undefined,
  b: LiteralValue | null | undefined,
): boolean {
  return a === b;
}
