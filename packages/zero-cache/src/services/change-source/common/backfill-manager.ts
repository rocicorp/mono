import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {assert} from '../../../../../shared/src/asserts.ts';
import {stringify} from '../../../../../shared/src/bigint-json.ts';
import {CustomKeyMap} from '../../../../../shared/src/custom-key-map.ts';
import {must} from '../../../../../shared/src/must.ts';
import {randInt} from '../../../../../shared/src/rand.ts';
import {getOrCreateCounter} from '../../../observability/metrics.ts';
import {JSON_STRINGIFIED, type JSONFormat} from '../../../types/lite.ts';
import {
  stateVersionFromString,
  stateVersionToString,
} from '../../../types/state-version.ts';
import {
  isRowKeyChange,
  isRowKeyRedefinition,
  rowKeyColumnsOf,
} from '../../replicator/change-log-cookies.ts';
import type {
  BackfillCompleted,
  BackfillRequest,
  BackfillRequestMessage,
  BackfillStarted,
  ChangeStreamMessage,
  Identifier,
  Mark,
  MessageBackfill,
} from '../protocol/current.ts';
import type {
  Cancelable,
  ChangeStreamMultiplexer,
  Listener,
} from './change-stream-multiplexer.ts';

function tableKey({schema, name}: Identifier) {
  return `${schema}.${name}`;
}

/**
 * A {@link MessageBackfill} or {@link BackfillCompleted} paired with the
 * approximate number of upstream bytes it represents. The BackfillManager uses
 * `byteSize` to bound the size of each replica transaction, committing and
 * reopening once {@link COMMIT_THRESHOLD_BYTES} is reached.
 */
export type BackfillMessage = {
  message: BackfillStarted | MessageBackfill | BackfillCompleted;
  byteSize: number;
};

export type BackfillStreamer = (
  req: BackfillRequest,
) => AsyncGenerator<BackfillMessage>;

/**
 * A backfill the manager owes, and everything it knows about resuming it.
 *
 * `minWatermark` lives here rather than on the running state so that a row key
 * change on a table whose run is not currently active is not forgotten.
 */
type RequiredBackfill = {
  request: BackfillRequest;

  /**
   * The version of the most recent row-key-changing update on the table. A
   * run snapshotted before it is canceled; a mark recorded before it is
   * dropped.
   */
  minWatermark: string;

  /**
   * Where the next run of this table should start: the mark this manager's
   * own replica got to, or null for the beginning. Only ever set from the
   * initial requests; everything that happens in a session lowers it to null.
   */
  resumeFrom: Mark | null;

  /**
   * The run that got the replica to `resumeFrom`, and the position in it of
   * the batch that did, which the next run announces that it resumes, so that
   * the subscribers following it from at least there follow the next run
   * instead. Null with `resumeFrom` null.
   */
  resumes: {runID: string; seq: number} | null;

  /**
   * The columns of `request` that this manager does not owe the change log
   * -- it completed them this session, or never had them -- and runs again,
   * from the beginning, only for subscribers that declared they still need
   * them. They are never run together with the columns it does owe, and their
   * runs are flagged `rerun`: a subscriber that does not follow runs has
   * completed them, and would overwrite newer values with a rerun's older
   * ones.
   */
  rerun: Set<string>;

  /**
   * The columns of the running run that a subscriber not following it
   * declared it needs. They join `rerun` when the run completes them.
   */
  rerunAfter: Set<string>;
};

/**
 * Commit (and reopen) the current backfill transaction once this many bytes of
 * backfill data have accumulated in it. Bounds the size of a single replica
 * COMMIT/checkpoint so it can't monopolize the replica.
 */
const COMMIT_THRESHOLD_BYTES = 8 * 1024 * 1024;

type RunningBackfillState = {
  request: BackfillRequest;
  canceledReason?: string | undefined;
  minWatermark: string;

  /** Whether this run is of columns in the entry's `rerun` set. */
  rerun: boolean;

  /** Set from the run's own announcement, once it has been pushed. */
  announcement?: BackfillStarted | undefined;
};

// The backfill run counters of the resumable-backfills rollout. A restart is
// the cost of a change that invalidates the running run; a rerun is the cost
// of a subscriber that arrived mid-run from elsewhere, or that needs columns
// this session already finished.
const runs = getOrCreateCounter(
  'replication',
  'backfill_runs',
  'Backfill runs started, by whether they started from the beginning, ' +
    'resumed from a mark, or reran columns already finished.',
);
const restarts = getOrCreateCounter(
  'replication',
  'backfill_restarts',
  'Backfill runs canceled and restarted, by what made them restart.',
);
const reruns = getOrCreateCounter(
  'replication',
  'backfill_reruns',
  'Backfill reruns queued: columns run again from the beginning, apart from ' +
    'the columns still owed, for a subscriber that was not following their ' +
    'run or that needs columns already finished.',
);

const MIN_BACKOFF_INTERVAL_MS = 2_000;
const MAX_BACKOFF_INTERVAL_MS = 60_000;

type AwaitingStatusWatermark = {
  watermark: string;
  reached: () => void;
};

/**
 * The BackfillManager initiates backfills for BackfillRequests from the
 * change-streamer (i.e. unfinished backfills from previous sessions)
 * or for new backfills signaled by `create-table` or `add-column` messages
 * from the change-source.
 *
 * The BackfillManager registers itself as a change stream listener in order
 * to track necessary backfills, and potentially invalidate the in-progress
 * backfill (e.g. due to a schema change) so that it can be retried at a
 * new snapshot.
 *
 * The manager also handles low priority streaming of the backfill messages
 * using the {@link ChangeStreamMultiplexer}, implementing a policy of always
 * releasing its reservation if another producer (i.e. the main change stream)
 * has messages to stream.
 */
export class BackfillManager implements Cancelable, Listener {
  readonly #lc: LogContext;

  /**
   * Tracks the metadata of required backfills based on schema changes
   * and initial backfill requests.
   */
  readonly #requiredBackfills = new CustomKeyMap<Identifier, RequiredBackfill>(
    tableKey,
  );
  readonly #changeStreamer: ChangeStreamMultiplexer;
  readonly #backfillStreamer: BackfillStreamer;
  readonly #jsonFormat: JSONFormat;

  /**
   * The current running backfill. The backfill request is always also in
   * `#requiredBackfills` (technically, it can be a subset of what's in
   * `#requiredBackfills`); the request is removed from `#requiredBackfills`
   * upon completion.
   */
  #runningBackfill: RunningBackfillState | null = null;

  /** The last seen watermark in the change stream. */
  #lastStatusWatermark: string | null = null;

  readonly #awaitingStatusWatermarks: AwaitingStatusWatermark[] = [];

  /** The watermark of the current transaction in the change stream. */
  #currentTxWatermark: string | null = null;

  readonly #commitThresholdBytes: number;

  /** Set when the change stream is canceled. No further backfills are run. */
  #canceled = false;

  constructor(
    lc: LogContext,
    changeStreamer: ChangeStreamMultiplexer,
    backfillStreamer: BackfillStreamer,
    jsonFormat: JSONFormat = JSON_STRINGIFIED,
    minBackoffMs = MIN_BACKOFF_INTERVAL_MS,
    maxBackoffMs = MAX_BACKOFF_INTERVAL_MS,
    commitThresholdBytes: number | undefined = COMMIT_THRESHOLD_BYTES,
  ) {
    this.#lc = lc.withContext('component', 'backfill-manager');
    this.#changeStreamer = changeStreamer;
    this.#backfillStreamer = backfillStreamer;
    this.#jsonFormat = jsonFormat;
    this.#minBackoffMs = minBackoffMs;
    this.#maxBackoffMs = maxBackoffMs;
    this.#retryDelayMs = minBackoffMs;
    this.#commitThresholdBytes = commitThresholdBytes ?? COMMIT_THRESHOLD_BYTES;
  }

  run(lastWatermark: string, initialRequests: BackfillRequest[]) {
    this.#lc.info?.(
      `starting backfill manager with ${initialRequests.length} initial requests`,
      {requests: initialRequests},
    );
    this.#lastStatusWatermark = lastWatermark;
    initialRequests.forEach(req =>
      this.#setRequiredBackfill('initial-request', req),
    );
    this.#checkAndStartBackfill();
  }

  #setLastStatusWatermark({watermark}: {watermark: string}) {
    // Only allow the watermark to move forward. This prevents a backfill
    // transaction (whose watermark is unrelated to change-stream state)
    // from moving the watermark backwards.
    if ((this.#lastStatusWatermark ?? '') < watermark) {
      this.#lastStatusWatermark = watermark;
      for (let i = this.#awaitingStatusWatermarks.length - 1; i >= 0; i--) {
        const awaiting = this.#awaitingStatusWatermarks[i];
        if (watermark >= awaiting.watermark) {
          awaiting.reached();
          this.#awaitingStatusWatermarks.splice(i, 1);
        }
      }
    }
  }

  #changeStreamReached(
    lc: LogContext,
    watermark: string,
  ): Promise<void> | null {
    if ((this.#lastStatusWatermark ?? '') < watermark) {
      const {promise, resolve: reached} = resolver();
      this.#awaitingStatusWatermarks.push({watermark, reached});
      lc.info?.(
        `waiting for change stream (at ${this.#lastStatusWatermark}) to reach ${watermark}`,
      );
      return promise;
    }
    return null;
  }

  readonly #minBackoffMs: number;
  readonly #maxBackoffMs: number;
  #retryDelayMs: number;
  #backfillRetryTimer: NodeJS.Timeout | undefined;

  #checkAndStartBackfill() {
    if (
      !this.#canceled &&
      !this.#backfillRetryTimer &&
      !this.#runningBackfill &&
      this.#requiredBackfills.size
    ) {
      // Pick a random backfill to avoid head-of-line blocking by a
      // problematic backfill (e.g. awaiting a primary key). This is
      // simpler that adding logic to classify (and declassify)
      // problematic backfills.
      const candidates = [...this.#requiredBackfills.values()];
      const entry = candidates[randInt(0, candidates.length - 1)];

      // The columns this manager owes run first, and without the columns
      // it runs again only for subscribers that declared them.
      const owed = Object.entries(entry.request.columns).filter(
        ([col]) => !entry.rerun.has(col),
      );
      const rerun = owed.length === 0;
      const request: BackfillRequest = {
        ...entry.request,
        columns: rerun ? entry.request.columns : Object.fromEntries(owed),
        resumeFrom: rerun ? null : entry.resumeFrom,
        resumeRunID: rerun ? null : (entry.resumes?.runID ?? null),
        resumeSeq: rerun ? null : (entry.resumes?.seq ?? null),
        minSnapshot: entry.minWatermark || null,
      };
      const state: RunningBackfillState = {
        request,
        minWatermark: entry.minWatermark,
        rerun,
      };
      const lc = this.#lc.withContext('table', request.table.name);

      this.#runningBackfill = state;
      void this.#runBackfill(lc, state)
        .then(() => {
          this.#stopRunningBackfill('backfill exited', state);
          this.#retryDelayMs = this.#minBackoffMs; // reset on success
        })
        // For unexpected errors (e.g. upstream replication slot
        // unavailability), retry with exponential backoff.
        .catch(e => {
          // The retry takes this run's place for the subscribers following it.
          this.#continueAfter(state);
          this.#stopRunningBackfill(String(e), state);
          this.#retryBackfillWithBackoff(e);
        });
    }
  }

  #retryBackfillWithBackoff(e: unknown) {
    if (this.#canceled) {
      this.#lc.debug?.(`not retrying backfill: change stream canceled`, e);
      return;
    }
    const log = this.#retryDelayMs === this.#maxBackoffMs ? 'error' : 'warn';
    this.#lc[log]?.(
      `Error running backfill. Retrying in ${this.#retryDelayMs} ms`,
      e,
    );
    this.#backfillRetryTimer = setTimeout(() => {
      this.#backfillRetryTimer = undefined;
      this.#checkAndStartBackfill();
    }, this.#retryDelayMs);

    this.#retryDelayMs = Math.min(this.#retryDelayMs * 2, this.#maxBackoffMs);
  }

  async #runBackfill(lc: LogContext, state: RunningBackfillState) {
    const changeStream = this.#changeStreamer; // Purely for readability

    // backfillTx is set if and only if a changeStreamer reservation has been
    // acquired and the backfill stream is inside a transaction.
    let backfillTx: string | null = null;
    let uncommittedBytes = 0;
    // The stream's watermark when the reservation was granted, which is where
    // it is released if the transaction is rolled back rather than committed.
    // (A property, since the assignment in `beginTxFor` is invisible to the
    // narrowing of a local where it is read.)
    const reservation: {watermark: string | null} = {watermark: null};

    /**
     * @returns the new tx watermark, or null if backfill was cancelled
     */
    const beginTxFor = async (
      msg: BackfillMessage['message'],
    ): Promise<string | null> => {
      assert(backfillTx === null, 'Expected no active backfill transaction');
      const lastWatermark = await changeStream.reserve('backfill');

      // After obtaining the changeStream reservation, check if the stream
      // had changes that resulted in invalidating / canceling this backfill.
      // A run announcement is checked like a batch of rows: it commits the
      // run's snapshot, so a snapshot that predates a row key change is
      // canceled before it is announced.
      const staleSnapshot =
        (msg.tag === 'backfill' || msg.tag === 'backfill-started') &&
        msg.watermark < state.minWatermark;
      // A resumed run is also voided by a key change its snapshot *includes*,
      // if the change came after the mark was taken: the moved row is at its
      // new key, below the mark, where the run never looks, and the update
      // that moved it may have omitted an unchanged TOASTed value. Such a
      // change can reach the stream after the run has started -- its snapshot
      // is taken at upstream's position, not the stream's -- so this is
      // checked at every message, the completion included: the completion
      // waits for the stream to reach the snapshot, which is when the last
      // such change arrives.
      const {resumeFrom = null, resumeFromWatermark = null} = state.request;
      const voidedMark =
        resumeFrom !== null && (resumeFromWatermark ?? '') < state.minWatermark;
      if (state.canceledReason || staleSnapshot || voidedMark) {
        if (state.canceledReason === undefined) {
          this.#stopRunningBackfill(
            staleSnapshot
              ? `row key change at ${state.minWatermark} ` +
                  `postdates backfill watermark at ${msg.watermark}`
              : `row key change at ${state.minWatermark} ` +
                  `postdates the mark resumed from, at ${resumeFromWatermark}`,
            state,
          );
        }
        changeStream.release(lastWatermark);
        return null;
      }

      reservation.watermark = lastWatermark;
      const {major, minor = 0n} = stateVersionFromString(lastWatermark);
      let tx = stateVersionToString({
        major,
        minor: BigInt(minor) + 1n,
      });

      if (msg.tag === 'backfill-completed' && tx < msg.watermark) {
        // At this point it must be the case that the #changeStreamReached() the
        // backfill watermark. Given that guarantee, ensure that the version of the
        // transaction containing the backfill-completed message is at least up
        // to the backfill watermark, so that the final database state version is
        // never earlier than the version of any backfilled rows.
        tx = msg.watermark;
      }

      void changeStream.push([
        'begin',
        // `backfill` tells the replicator that `tx` orders this manager's
        // stream only, so it commits the transaction at a replica-local version.
        {tag: 'begin', json: this.#jsonFormat, skipAck: true, backfill: true},
        {commitWatermark: tx},
      ]);
      return (backfillTx = tx);
    };

    const commitTx = () => {
      if (backfillTx) {
        void changeStream.push([
          'commit',
          {tag: 'commit'},
          {watermark: backfillTx},
        ]);
        changeStream.release(backfillTx);
      }
      backfillTx = null;
      uncommittedBytes = 0;
    };

    /**
     * Pushes a message into the backfill transaction, opening one if
     * necessary. Returns the transaction's watermark, or null if the backfill
     * was canceled. (The watermark is returned rather than assigned so that
     * `backfillTx` is only ever assigned in the loop below, where the
     * narrowing that the commit checks depend on can see it.)
     */
    const pushMessage = async (
      msg: BackfillMessage['message'],
      byteSize: number,
    ): Promise<string | null> => {
      const tx = backfillTx ?? (await beginTxFor(msg));
      if (tx === null) {
        return null;
      }
      // `await` to allow the change streamer to exert back pressure
      // on backfills.
      await changeStream.push([
        'data',
        state.rerun ? {...msg, rerun: true} : msg,
      ]);
      uncommittedBytes += byteSize;
      return tx;
    };

    try {
      for await (const {message: msg, byteSize} of this.#backfillStreamer(
        state.request,
      )) {
        if (this.#canceled) {
          // Exiting the loop finalizes the backfill stream (and the upstream
          // resources it holds). The reservation, if held, does not need to be
          // released since the change stream is gone.
          lc.info?.(`backfill stream canceled: change stream canceled`);
          return;
        }
        // Before sending `backfill-completed`, the main replication stream
        // may need to catch up, and/or the current transaction may need to be
        // committed to open a new transaction that's up to backfill watermark.
        const mustWaitBeforeFlush =
          msg.tag === 'backfill-completed' &&
          (this.#changeStreamReached(lc, msg.watermark) ||
            (backfillTx !== null && backfillTx < msg.watermark));

        // Commit (and later reopen) the transaction if the main stream is
        // waiting on the reservation, if we must catch up before completing, or
        // if the size of current transaction has reached the commit threshold.
        if (
          backfillTx &&
          (changeStream.waiterDelay() > 0 ||
            mustWaitBeforeFlush ||
            uncommittedBytes >= this.#commitThresholdBytes)
        ) {
          commitTx();
        }

        mustWaitBeforeFlush && (await mustWaitBeforeFlush);

        if (
          msg.tag === 'backfill' &&
          msg.rowValues.length > 0 &&
          msg.relation.rowKey.columns.length === 0
        ) {
          throw new MissingRowKeyError(state.request);
        }

        backfillTx = await pushMessage(msg, byteSize);
        if (backfillTx === null) {
          lc.info?.(
            `backfill stream canceled: ${state.canceledReason}`,
            state.request,
          );
          this.#checkAndStartBackfill(); // start the next backfill if present
          return; // this backfill is canceled
        }

        // Recorded after the push, so that a request answered from the
        // announcement is never answered ahead of what subscribers have been
        // sent.
        if (msg.tag === 'backfill-started') {
          state.announcement = msg;
          const start = state.rerun
            ? 'rerun'
            : msg.resumes === null
              ? 'zero'
              : 'resumed';
          const resumeFrom =
            msg.resumes === null ? null : (state.request.resumeFrom ?? null);
          runs.add(1, {start, table: msg.relation.name});
          // The one line that says which run is on the wire and where it picked
          // up. `backfillRun` is structured because everything downstream of it
          // -- the soak harness included -- wants the fields, not the prose.
          lc.info?.(
            `run ${msg.runID} of ${msg.relation.name} is streaming from ` +
              (msg.resumes === null
                ? 'the beginning'
                : `${JSON.stringify(resumeFrom)}, resuming run ` +
                  `${msg.resumes.runID} after batch ${msg.resumes.seq}`),
            {
              backfillRun: {
                runID: msg.runID,
                schema: msg.relation.schema,
                table: msg.relation.name,
                columns: msg.columns,
                start,
                resumes: msg.resumes,
                resumeFrom,
                rerun: state.rerun,
                snapshot: msg.watermark,
              },
            },
          );
        }
      }

      // Flush any final tx and release the stream.
      backfillTx && commitTx();
    } catch (e) {
      if (backfillTx !== null && !this.#canceled) {
        // A stream that fails mid-transaction (e.g. a lost COPY connection)
        // still holds the change stream. Roll its transaction back and release
        // the reservation, or no other producer -- the main replication stream
        // included -- can ever reserve it again.
        lc.warn?.(`rolling back backfill transaction ${backfillTx}`, e);
        void changeStream.push(['rollback', {tag: 'rollback'}]);
        changeStream.release(must(reservation.watermark));
        backfillTx = null;
      }
      throw e;
    }
    lc.debug?.(`backfill stream exited`, state.canceledReason ?? '');
  }

  /**
   * Handles a subscriber's declaration of an in-flight backfill, forwarded by
   * the change-streamer because it could not resolve it from its own change
   * log: the subscriber is not following the run the table has (it never saw
   * the run announced, or it follows a run of another replication-manager),
   * or it needs columns that this session has already finished.
   *
   * The answer is a run from the beginning, which every subscriber follows --
   * after the table's current run, if there is one, rather than in its place.
   * Nothing here compares a mark, because no subscriber declares one. And
   * nothing here cancels a run, so no subscriber following one is ever left
   * behind by another's declaration, and a wave of such declarations (a fleet
   * moving between replication-managers mid-run) costs one run.
   *
   * A run for columns this session has finished is a *rerun*, run apart from
   * the columns it still owes and flagged as such (see
   * {@link RequiredBackfill.rerun}), so that the subscribers that do not follow
   * runs -- which have completed those columns, and apply whatever they are
   * sent -- are never sent it.
   */
  onBackfillRequest(message: BackfillRequestMessage): void {
    const [, {table, columns, runID, runSeq, subscriberID}] = message;
    // On the context rather than at each site: every decision this method
    // logs is about one subscriber's declaration, and naming it is what makes
    // the run it costs attributable after the fact.
    const lc = this.#lc
      .withContext('table', table.name)
      .withContext('declaredBy', subscriberID ?? 'unknown');
    const entry = this.#requiredBackfills.get(table);

    if (entry === undefined) {
      // Scenario B: this session already finished the table. Every other
      // subscriber that follows runs ignores the resulting rows through the
      // column guard; no other is sent them.
      lc.info?.(
        `adding a rerun for a table this session already finished; ` +
          `it will run from the beginning`,
        {columns: Object.keys(columns)},
      );
      this.#setRequiredBackfill('backfill-request', {table, columns});
      for (const col of Object.keys(columns)) {
        must(this.#requiredBackfills.get(table)).rerun.add(col);
      }
      reruns.add(1, {table: table.name});
      this.#checkAndStartBackfill();
      return;
    }

    // The manager may have completed only some of the subscriber's columns.
    // Those are run again for it, after the columns the manager still owes.
    const finished = Object.keys(columns).filter(
      col => !(col in entry.request.columns),
    );
    if (finished.length) {
      entry.request = {
        ...entry.request,
        columns: {
          ...Object.fromEntries(finished.map(col => [col, columns[col]])),
          ...entry.request.columns,
        },
      };
      finished.forEach(col => entry.rerun.add(col));
      lc.info?.(
        `columns of ${table.name} this session already finished will run ` +
          `again from the beginning`,
        {columns: finished},
      );
      reruns.add(1, {table: table.name});
    }

    const running = this.#backfillRunningFor(table);
    if (running === null) {
      // The next run of the columns the manager owes must cover this
      // subscriber, if it declared any of them. It does if it starts from the
      // beginning, or if it resumes the run the subscriber is following from
      // a batch the subscriber has applied -- which is the common case, since
      // a restarted manager seeds the resume from its own replica, and its
      // backup replicator declares that very run and position. Anything else
      // starts from the beginning, which covers everyone.
      const owed = Object.keys(columns).some(col => !entry.rerun.has(col));
      if (
        owed &&
        entry.resumeFrom !== null &&
        !followsResumed(entry.resumes, runID, runSeq)
      ) {
        lc.info?.(
          `the next run of ${table.name} will start from the beginning`,
          {declared: {runID, runSeq}, wouldHaveResumed: entry.resumes},
        );
        entry.resumeFrom = null;
        entry.resumes = null;
      }
      this.#checkAndStartBackfill();
      return;
    }
    if (this.#willFollow(running, runID, runSeq)) {
      lc.debug?.(`subscriber follows the running run`, {runID, runSeq});
      return; // nothing to do: it has everything the run has sent
    }
    const again = Object.keys(columns).filter(
      col => col in running.request.columns && !entry.rerunAfter.has(col),
    );
    if (again.length) {
      again.forEach(col => entry.rerunAfter.add(col));
      lc.info?.(
        `the backfill of ${table.name} will run again from the beginning ` +
          `after the current run, for a subscriber that is not following it`,
        {declared: runID, columns: again},
      );
      reruns.add(1, {table: table.name});
    }
  }

  /**
   * Whether a subscriber following `runID` at batch `runSeq` follows the
   * running run: it is that run; or, while the run has not announced itself
   * yet -- so that the subscriber is certain to receive the announcement --
   * the run will start from the beginning or resume `runID` from a batch the
   * subscriber has applied.
   */
  #willFollow(
    running: RunningBackfillState,
    runID: string | null,
    runSeq: number | null,
  ): boolean {
    const {announcement, request} = running;
    if (announcement !== undefined) {
      return runID !== null && runID === announcement.runID;
    }
    const {resumeFrom = null, resumeRunID = null, resumeSeq = null} = request;
    return (
      resumeFrom === null ||
      resumeRunID === null ||
      resumeSeq === null ||
      followsResumed({runID: resumeRunID, seq: resumeSeq}, runID, runSeq)
    );
  }

  /**
   * Arranges for the next run of the table to take the place of `running`,
   * which is being canceled, for the subscribers following it: a run resumed
   * from the same mark that announces it resumes this one from its start
   * (batch 0, which every follower has applied), or, for a run from the
   * beginning, another from the beginning. Called before the entry is moved
   * or the run is stopped.
   *
   * A run that has not announced itself has no followers to keep, and the
   * next run announces exactly what this one would have.
   */
  #continueAfter(running: RunningBackfillState): void {
    const entry = this.#requiredBackfills.get(running.request.table);
    const announced = running.announcement;
    if (entry === undefined || announced === undefined) {
      return;
    }
    if (announced.resumes === null) {
      entry.resumeFrom = null;
      entry.resumes = null;
    } else if (entry.resumeFrom !== null) {
      // (A key change since has voided the mark otherwise, and with it the
      // run to resume: the next run starts from the beginning.)
      entry.resumes = {runID: announced.runID, seq: 0};
    }
  }

  /**
   * Cancels the table's running run, if any, so that the next run of the
   * table takes its place (see {@link #continueAfter}).
   */
  #replaceRunFor(table: Identifier, reason: string): void {
    const running = this.#backfillRunningFor(table);
    if (running) {
      this.#continueAfter(running);
      this.#stopRunningBackfill(reason, running);
    }
  }

  #backfillRunningFor(table: Identifier): RunningBackfillState | null {
    const state = this.#runningBackfill;
    return state?.request.table.schema === table.schema &&
      state.request.table.name === table.name
      ? state
      : null;
  }

  /**
   * Stops the running backfill for the specified `reason`. If `instance` is
   * specified, the running backfill is stopped only if it is that instance.
   * This allows the running backfill itself to clear backfill state without
   * accidentally stopping a different (e.g. subsequent) backfill.
   */
  #stopRunningBackfill(reason?: string, instance?: RunningBackfillState) {
    const backfill = this.#runningBackfill;
    if (backfill && backfill === (instance ?? backfill)) {
      backfill.canceledReason = reason;
      this.#runningBackfill = null;
      reason && this.#lc.info?.(`canceling backfill:`, reason);
    }
  }

  /**
   * Records `req` as required. `carry` is the entry it replaces when the
   * table's key has changed (a rename); otherwise the entry under `req`'s
   * table, if any.
   */
  #setRequiredBackfill(
    source: string,
    req: BackfillRequest,
    carry?: RequiredBackfill,
  ) {
    const existing = carry ?? this.#requiredBackfills.get(req.table);
    const action = existing ? 'updated' : 'added';
    this.#lc.info?.(`Backfill ${action}: ${source}`, {backfill: req});
    this.#requiredBackfills.set(req.table, {
      // The resume state of a table survives its request being updated (a
      // rename, a metadata change, a column dropped): none of those move any
      // row, so none of them invalidate a mark or forget a key change. (What
      // does -- a key change, a redefined key, a column added, a completion --
      // clears it explicitly.)
      minWatermark: existing?.minWatermark ?? req.minSnapshot ?? '',
      resumeFrom: existing?.resumeFrom ?? req.resumeFrom ?? null,
      resumes:
        existing?.resumes ??
        (req.resumeRunID !== null &&
        req.resumeRunID !== undefined &&
        req.resumeSeq !== null &&
        req.resumeSeq !== undefined
          ? {runID: req.resumeRunID, seq: req.resumeSeq}
          : null),
      rerun: new Set(),
      rerunAfter: new Set(),
      ...existing,
      request: req,
    });
  }

  #deleteRequiredBackfill(source: string, id: Identifier) {
    const entry = this.#requiredBackfills.get(id);
    if (entry) {
      const action = source === 'backfill-completed' ? 'completed' : 'dropped';
      this.#lc.info?.(`Backfill ${action}: ${source}`, {
        backfill: entry.request,
      });
      this.#requiredBackfills.delete(id);
    }
  }

  /**
   * Implements {@link Listener.onChange()}, invoked by the
   * {@link ChangeStreamMultiplexer}.
   */
  onChange(message: ChangeStreamMessage): void {
    if (message[0] === 'begin') {
      this.#currentTxWatermark = message[2].commitWatermark;
      return;
    }
    if (message[0] === 'commit') {
      this.#currentTxWatermark = null;
      this.#setLastStatusWatermark(message[2]);
      // Every commit is a candidate for starting the next backfill
      // (if one is not currently running).
      this.#checkAndStartBackfill();
      return;
    }
    if (message[0] === 'status') {
      this.#setLastStatusWatermark(message[2]);
      return;
    }
    if (message[0] !== 'data') {
      return;
    }
    const change = message[1];
    const {tag} = change;
    switch (tag) {
      case 'update-table-metadata': {
        const {table, new: metadata} = change;
        const entry = this.#requiredBackfills.get(table);
        if (entry) {
          if (isRowKeyRedefinition(change)) {
            // A mark is a position in the order of the old row key, which is
            // no position at all in the order of the new one: the next run
            // starts from the beginning, including the one that replaces a
            // run canceled here. (A run snapshotted before the change needs
            // no floor: the change source finds the old key in its snapshot
            // and rejects the run.)
            entry.resumeFrom = null;
            entry.resumes = null;
          }
          this.#replaceRunFor(table, `TableMetadata updated`);
          this.#setRequiredBackfill(tag, {
            ...entry.request,
            table: {...entry.request.table, metadata},
          });
        }
        break;
      }
      case 'create-table': {
        const {
          spec: {schema, name},
          metadata = null,
          backfill,
        } = change;

        if (backfill) {
          this.#setRequiredBackfill(tag, {
            table: {schema, name, metadata},
            columns: backfill,
          });
        }
        break;
      }
      case 'rename-table': {
        const {old, new: newTable} = change;
        const entry = this.#requiredBackfills.get(old);
        if (entry) {
          const {schema, name} = newTable;
          this.#replaceRunFor(old, `table renamed`);
          this.#deleteRequiredBackfill(tag, old);
          // Carried across the rename rather than rebuilt from the request:
          // the request's resume fields are as of the session's start, and
          // the entry's may since have been lowered by a key change.
          this.#setRequiredBackfill(
            tag,
            {
              ...entry.request,
              table: {...entry.request.table, schema, name},
            },
            entry,
          );
        }
        break;
      }
      case 'drop-table': {
        const {id} = change;
        const backfillRequest = this.#requiredBackfills.get(id)?.request;
        if (backfillRequest) {
          this.#deleteRequiredBackfill(tag, id);
          if (this.#backfillRunningFor(id)) {
            this.#stopRunningBackfill(`table dropped`);
          }
        }
        break;
      }
      case 'add-column': {
        const {
          table,
          tableMetadata: metadata = null,
          column,
          backfill,
        } = change;
        if (backfill) {
          const backfillRequest = this.#requiredBackfills.get(table)?.request;
          if (!backfillRequest) {
            this.#setRequiredBackfill(tag, {
              table: {...table, metadata},
              columns: {[column.name]: backfill},
            });
          } else {
            this.#setRequiredBackfill(tag, {
              ...backfillRequest,
              table: {...backfillRequest.table, metadata},
              columns: {
                ...backfillRequest.columns,
                [column.name]: backfill,
              },
            });
            // A mark is how far the replica got with the columns of the run
            // that produced it. The new column has no rows below it, so any
            // run that includes the column -- including one that replaces the
            // running run -- starts from the beginning.
            const entry = must(this.#requiredBackfills.get(table));
            entry.resumeFrom = null;
            entry.resumes = null;
            // Note: The running backfill need not be canceled if a
            //   new column is added. The new column will be backfilled
            //   by its own stream after the current backfill completes.
          }
        }
        break;
      }
      case 'update-column': {
        const {
          table,
          old: {name: oldName},
          new: {name: newName},
        } = change;
        if (oldName !== newName) {
          const backfillRequest = this.#requiredBackfills.get(table)?.request;
          if (backfillRequest && oldName in backfillRequest.columns) {
            const {[oldName]: colSpec, ...otherCols} = backfillRequest.columns;
            this.#setRequiredBackfill(tag, {
              ...backfillRequest,
              columns: {...otherCols, [newName]: colSpec},
            });
            const entry = must(this.#requiredBackfills.get(table));
            for (const cols of [entry.rerun, entry.rerunAfter]) {
              if (cols.delete(oldName)) {
                cols.add(newName);
              }
            }
            const backfill = this.#backfillRunningFor(table);
            if (backfill && oldName in backfill.request.columns) {
              this.#replaceRunFor(table, `column renamed`);
            }
          }
        }
        break;
      }
      case 'drop-column': {
        const {table, column} = change;
        const backfillRequest = this.#requiredBackfills.get(table)?.request;
        if (backfillRequest && column in backfillRequest.columns) {
          const {[column]: _excluded, ...remaining} = backfillRequest.columns;
          if (Object.keys(remaining).length === 0) {
            this.#deleteRequiredBackfill(tag, table);
          } else {
            this.#setRequiredBackfill(tag, {
              ...backfillRequest,
              columns: remaining,
            });
            const entry = must(this.#requiredBackfills.get(table));
            entry.rerun.delete(column);
            entry.rerunAfter.delete(column);
          }
          const backfill = this.#backfillRunningFor(table);
          if (backfill && column in backfill.request.columns) {
            this.#replaceRunFor(table, `column dropped`);
          }
        }
        break;
      }
      case 'update': {
        // A corner case that backfill is unable to correctly handle is when a
        // row's key changes; this is decomposed into a delete of the old key
        // and a set of the new key in the replica change log, at which point
        // the backfill algorithm assumes that the (old) row is deleted but
        // does not know to backfill the new row. The current backfill is
        // canceled and retried if its version precedes this update, and no
        // mark taken before it can be resumed from.
        const {relation} = change;
        const entry = this.#requiredBackfills.get(relation);
        const running = this.#backfillRunningFor(relation);
        // For a FULL identity table, the key its metadata names, which is
        // what a run orders by: pgoutput names every column a key column.
        const metadata = (entry ?? running)?.request.table.metadata;
        if (
          (!entry && !running) ||
          !isRowKeyChange(change, () => rowKeyColumnsOf(metadata))
        ) {
          break;
        }
        const txWatermark = must(this.#currentTxWatermark, `not in a tx`);
        // Recorded on the required entry rather than only on the running
        // state, so that a key change on a table whose run is not currently
        // active is not forgotten -- and so that a mark declared later, from
        // a snapshot older than this, is dropped rather than resumed from.
        if (entry) {
          entry.minWatermark = txWatermark;
          entry.resumeFrom = null;
          entry.resumes = null;
        }
        if (running) {
          running.minWatermark = txWatermark;
          this.#lc.info?.(
            `key for row has changed. ` +
              `backfill data must not predate ${txWatermark}`,
          );
          restarts.add(1, {reason: 'key-change', table: relation.name});
        }
        break;
      }
      case 'backfill-completed': {
        const {relation, columns} = change;
        const entry = this.#requiredBackfills.get(relation);
        assert(
          entry,
          () => `No BackfillRequest completed backfill ${stringify(change)}`,
        );
        const completed = new Set([...columns, ...relation.rowKey.columns]);
        // The columns a subscriber not following this run declared it needs:
        // they run again, from the beginning, now that the manager owes them
        // no longer.
        const again = new Set(
          [...entry.rerunAfter].filter(col => completed.has(col)),
        );
        const remaining = Object.entries(entry.request.columns).filter(
          ([col]) => !completed.has(col) || again.has(col),
        );
        if (remaining.length === 0) {
          this.#deleteRequiredBackfill(tag, relation);
        } else {
          if (again.size) {
            this.#lc.info?.(
              `Backfill completed; running ${relation.name} again from the ` +
                `beginning for subscribers that were not following it`,
              {backfill: entry.request, columns: [...again]},
            );
          }
          this.#setRequiredBackfill(tag, {
            ...entry.request,
            columns: Object.fromEntries(remaining),
          });
          const next = must(this.#requiredBackfills.get(relation));
          for (const col of completed) {
            next.rerun.delete(col);
          }
          again.forEach(col => next.rerun.add(col));
          next.rerunAfter.clear();
          // The resume point was where this run picked up, for this run's
          // columns. The columns left start from the beginning.
          next.resumeFrom = null;
          next.resumes = null;
        }
        // Technically the backfill is already stopping, but this method
        // cleans up the state that tracks it.
        this.#stopRunningBackfill();
        break;
      }
    }
  }

  cancel(): void {
    this.#canceled = true;
    this.#stopRunningBackfill(`change stream canceled`);
    clearTimeout(this.#backfillRetryTimer);
    this.#backfillRetryTimer = undefined;

    // Wake up a backfill that is waiting for the change stream to reach a
    // watermark. The change stream never will, and the backfill must be
    // allowed to unwind (and release its upstream resources) rather than
    // remain suspended forever.
    for (const {reached} of this.#awaitingStatusWatermarks.splice(0)) {
      reached();
    }
  }
}

/**
 * Whether a subscriber following `runID` at batch `runSeq` follows a run that
 * resumes `resumes.runID` after batch `resumes.seq`: it is following that
 * run, and has applied at least that many of its batches.
 */
function followsResumed(
  resumes: {runID: string; seq: number} | null,
  runID: string | null,
  runSeq: number | null,
): boolean {
  return (
    resumes !== null &&
    runID === resumes.runID &&
    runSeq !== null &&
    runSeq >= resumes.seq
  );
}

abstract class BackfillStreamError extends Error {
  constructor(bf: BackfillRequest, msg: string, cause?: unknown) {
    super(
      `Cannot backfill ${bf.table.schema}.${bf.table.name}` +
        `[${Object.keys(bf.columns).join(',')}]: ${msg}`,
      {cause},
    );
  }
}

/**
 * Background: The zero-cache supports replication of tables without a
 * PRIMARY KEY to facilitate the onboarding process. These rows can be
 * INSERT'ed, but postgres will rightfully prohibit UPDATEs and DELETEs
 * on such tables because the rows cannot be identified by a key. Supporting
 * this mode of replication allows the user to "fix" the setup by adding the
 * primary key, after which the table can be published downstream without
 * requiring a resync of the data.
 *
 * In terms of backfill, however, non-empty tables without a row key **cannot**
 * be backfilled, because backfill retries would result in writing duplicating
 * rows. (Empty tables, on the other hand, are fine because there is no data
 * to be deduped.)
 *
 * The MissingRowKeyError is used to signal that the table cannot be backfilled
 * in its current state. For simplicity, it is handled like runtime errors and
 * retried with backoff, with which it can eventually succeed if (1) a primary
 * key is added or (2) the table is emptied, e.g. via a TRUNCATE.
 */
class MissingRowKeyError extends BackfillStreamError {
  readonly name = 'MissingRowKeyError';

  constructor(bf: BackfillRequest, cause?: unknown) {
    super(bf, `"${bf.table.name}" is missing a PRIMARY KEY`, cause);
  }
}

/**
 * Error type for backfill stream implementations to throw indicating that
 * the backfill request failed due to a schema incompatibility error. This
 * type of error does not need exponential backoff, as the retry happens
 * naturally once the invalidating schema change is processed and committed.
 */
export class SchemaIncompatibilityError extends BackfillStreamError {
  readonly name = 'SchemaIncompatibilityError';

  constructor(bf: BackfillRequest, msg: string, cause?: unknown) {
    super(bf, msg, cause);
  }
}
