import type {LogContext} from '@rocicorp/logger';
import type {NoIndexDiff} from '../../../replicache/src/btree/node.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {getBrowserGlobal} from '../../../shared/src/browser-env.ts';
import type {DocumentVisibilityWatcher} from '../../../shared/src/document-visible.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import type {DebugDelegate} from '../../../zql/src/builder/debug-delegate.ts';
import type {Input} from '../../../zql/src/ivm/operator.ts';
import type {Source, SourceInput} from '../../../zql/src/ivm/source.ts';
import {MeasurePushOperator} from '../../../zql/src/query/measure-push-operator.ts';
import type {MetricsDelegate} from '../../../zql/src/query/metrics-delegate.ts';
import {QueryDelegateBase} from '../../../zql/src/query/query-delegate-base.ts';
import type {
  AttachPipeline,
  CommitListener,
} from '../../../zql/src/query/query-delegate.ts';
import type {RunOptions} from '../../../zql/src/query/query.ts';
import {type IVMSourceBranch} from './ivm-branch.ts';
import type {QueryManager} from './query-manager.ts';

export type AddQuery = QueryManager['addLegacy'];
export type AddCustomQuery = QueryManager['addCustom'];

export type UpdateQuery = QueryManager['updateLegacy'];
export type UpdateCustomQuery = QueryManager['updateCustom'];
export type FlushQueryChanges = QueryManager['flushBatch'];

/**
 * How long pipelines are hydrated for before yielding to the event loop. Short
 * enough to keep a frame or two, long enough that the yields themselves (a
 * `setTimeout(0)` is clamped to 4ms in browsers) stay in the noise.
 */
const HYDRATE_SLICE_MS = 12;

/**
 * ZeroContext glues together zql and Replicache. It listens to changes in
 * Replicache data and pushes them into IVM and on tells the server about new
 * queries.
 */
export class ZeroContext extends QueryDelegateBase {
  readonly #lc: LogContext;

  // It is a bummer to have to maintain separate MemorySources here and copy the
  // data in from the Replicache db. But we want the data to be accessible via
  // pipelines *synchronously* and the core Replicache infra is all async. So
  // that needs to be fixed.
  readonly #mainSources: IVMSourceBranch;

  readonly addServerQuery: AddQuery;
  readonly addCustomQuery: AddCustomQuery;
  readonly updateServerQuery: UpdateQuery;
  readonly updateCustomQuery: UpdateCustomQuery;
  readonly flushQueryChanges: () => void;
  readonly #batchViewUpdates: (applyViewUpdates: () => void) => void;
  readonly #commitListeners: Set<CommitListener> = new Set();

  // Pipeline construction is deferred until `markPipelinesReady()` is called.
  // Zero calls it once the replica has been loaded into the IVM sources, so
  // cold boot loads the sources once instead of pushing every row through
  // every already-materialized pipeline. Contexts whose sources are already
  // populated at construction (custom mutator transactions, tests) call it
  // right away.
  #pipelinesReady = false;
  readonly #pendingPipelines: Set<AttachPipeline> = new Set();
  #hydratingPipelines = false;
  readonly #visibilityWatcher: DocumentVisibilityWatcher | undefined;

  readonly assertValidRunOptions: (options?: RunOptions) => void;

  /**
   * Client-side queries start out as "unknown" and are then updated to
   * "complete" once the server has sent back the query result.
   */
  readonly defaultQueryComplete = false;

  readonly addMetric: MetricsDelegate['addMetric'];

  constructor(
    lc: LogContext,
    mainSources: IVMSourceBranch,
    addQuery: AddQuery,
    addCustomQuery: AddCustomQuery,
    updateQuery: UpdateQuery,
    updateCustomQuery: UpdateCustomQuery,
    flushQueryChanges: () => void,
    batchViewUpdates: (applyViewUpdates: () => void) => void,
    addMetric: MetricsDelegate['addMetric'],
    assertValidRunOptions: (options?: RunOptions) => void,
    visibilityWatcher?: DocumentVisibilityWatcher | undefined,
  ) {
    super();
    this.#lc = lc;
    this.#mainSources = mainSources;
    this.addServerQuery = addQuery;
    this.updateServerQuery = updateQuery;
    this.updateCustomQuery = updateCustomQuery;
    this.#batchViewUpdates = batchViewUpdates;
    this.assertValidRunOptions = assertValidRunOptions;
    this.addCustomQuery = addCustomQuery;
    this.flushQueryChanges = flushQueryChanges;
    this.addMetric = addMetric;
    this.#visibilityWatcher = visibilityWatcher;
  }

  applyFiltersAnyway?: boolean | undefined;

  debug?: DebugDelegate | undefined;

  getSource(name: string): Source | undefined {
    return this.#mainSources.getSource(name);
  }

  override get pipelinesReady(): boolean {
    return this.#pipelinesReady;
  }

  override onPipelinesReady(cb: AttachPipeline): () => void {
    assert(
      !this.#pipelinesReady,
      'onPipelinesReady called while pipelines are ready',
    );
    this.#pendingPipelines.add(cb);
    return () => {
      this.#pendingPipelines.delete(cb);
    };
  }

  /**
   * Build and hydrate every pipeline deferred since construction, in
   * materialization order, as a single view-update batch. Views materialized
   * before this is called are empty and `unknown` in the meantime, exactly as
   * they would be over empty sources. A pipeline that fails to build is
   * logged and its view reports an error; the rest are still built.
   *
   * Returns `this` so it can be chained onto the constructor call.
   */
  markPipelinesReady(): this {
    if (this.#pipelinesReady) {
      return this;
    }
    assert(!this.#hydratingPipelines, 'Pipelines are already being hydrated');
    if (this.#pendingPipelines.size === 0) {
      this.#pipelinesReady = true;
      return this;
    }
    this.batchViewUpdates(() => {
      const releases: (() => void)[] = [];
      this.#attachPending(releases, () => false);
      this.#release(releases);
    });
    return this;
  }

  /**
   * Like {@link markPipelinesReady} but hydrates in time slices, yielding to
   * the event loop in between so a burst of queries registered at startup does
   * not monopolize the thread. It only ever yields *between* pipelines.
   *
   * No view is exposed until all of them are hydrated: they are released, and
   * the commit listeners notified, in one synchronous batch at the end.
   * Queries materialized in the meantime are still deferred and join that
   * batch. The caller must not call {@link processChanges} until the returned
   * promise resolves.
   */
  async hydratePendingPipelines(
    sliceMs = HYDRATE_SLICE_MS,
    yieldToEventLoop: () => Promise<void> = this.#yieldToEventLoop,
  ): Promise<void> {
    if (this.#pipelinesReady) {
      return;
    }
    assert(!this.#hydratingPipelines, 'Pipelines are already being hydrated');
    if (this.#pendingPipelines.size === 0) {
      this.#pipelinesReady = true;
      return;
    }
    this.#hydratingPipelines = true;
    const releases: (() => void)[] = [];
    for (;;) {
      const sliceEnd = performance.now() + sliceMs;
      this.batchViewUpdates(() => {
        this.#attachPending(releases, () => performance.now() >= sliceEnd);
      });
      if (this.#pendingPipelines.size === 0) {
        break;
      }
      await yieldToEventLoop();
    }
    this.#hydratingPipelines = false;
    this.batchViewUpdates(() => this.#release(releases));
  }

  #yieldToEventLoop = (): Promise<void> => {
    // Not in React Native or Safari.
    const scheduler = getBrowserGlobal('scheduler');
    if (typeof scheduler?.yield === 'function') {
      return scheduler.yield();
    }
    // Timers are throttled to a second or more in a background tab, which would
    // stretch startup to minutes, and a hidden page has nothing to keep
    // responsive anyway.
    if (this.#visibilityWatcher?.visibilityState === 'hidden') {
      return Promise.resolve();
    }
    return new Promise(resolve => setTimeout(resolve, 0));
  };

  /**
   * Attaches pending pipelines, always at least one, until none are left or
   * `shouldStop` says the slice is over.
   */
  #attachPending(releases: (() => void)[], shouldStop: () => boolean) {
    for (const attach of this.#pendingPipelines) {
      this.#pendingPipelines.delete(attach);
      try {
        releases.push(attach());
      } catch (e) {
        // The failing view has already been marked as errored; keep
        // building the others rather than aborting startup.
        this.#lc.error?.(
          ErrorKind.Internal,
          'Failed to build a deferred query pipeline',
          e,
        );
      }
      if (shouldStop()) {
        return;
      }
    }
  }

  /** Must be called inside `batchViewUpdates`. */
  #release(releases: (() => void)[]) {
    this.#pipelinesReady = true;
    try {
      for (const release of releases) {
        try {
          release();
        } catch (e) {
          // One view must not keep the rest from being released.
          this.#lc.error?.(
            ErrorKind.Internal,
            'Failed to release a deferred query pipeline',
            e,
          );
        }
      }
    } finally {
      this.#endTransaction();
    }
  }

  mapAst(ast: AST): AST {
    return ast;
  }

  override decorateSourceInput(input: SourceInput, queryID: string): Input {
    return new MeasurePushOperator(input, queryID, this, 'query-update-client');
  }

  onTransactionCommit(cb: CommitListener): () => void {
    this.#commitListeners.add(cb);
    return () => {
      this.#commitListeners.delete(cb);
    };
  }

  override batchViewUpdates<T>(applyViewUpdates: () => T) {
    let result: T | undefined;
    let viewChangesPerformed = false;
    this.#batchViewUpdates(() => {
      result = applyViewUpdates();
      viewChangesPerformed = true;
    });
    assert(
      viewChangesPerformed,
      'batchViewUpdates must call applyViewUpdates synchronously.',
    );
    return result as T;
  }

  processChanges(
    expectedHead: Hash | undefined,
    newHead: Hash,
    changes: NoIndexDiff,
  ) {
    // Views hydrated so far would flush ahead of the rest of their batch.
    assert(
      !this.#hydratingPipelines,
      'processChanges called while pipelines are being hydrated',
    );
    this.batchViewUpdates(() => {
      try {
        this.#mainSources.advance(expectedHead, newHead, changes);
      } finally {
        this.#endTransaction();
      }
    });
  }

  #endTransaction() {
    for (const listener of this.#commitListeners) {
      try {
        listener();
      } catch (e) {
        // We should not fatal the inner-workings of Zero due to the user's application
        // code throwing an error.
        // Hence we wrap notifications in a try-catch block.
        this.#lc.error?.(
          ErrorKind.Internal,
          'Failed notifying a commit listener of IVM updates',
          e,
        );
      }
    }
  }
}
