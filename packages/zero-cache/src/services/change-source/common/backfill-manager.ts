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
import {isRowKeyChange} from '../../replicator/change-log-cookies.ts';
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

type BackfillStreamer = (
  req: BackfillRequest,
) => AsyncGenerator<BackfillMessage>;

/**
 * Asks Postgres whether any row of the table has a key in `(from, to]`, under
 * the table's publication row filter. `from === null` means "from the
 * beginning".
 *
 * This is the only key comparison outside the backfill COPY itself, and it
 * answers the only question the manager has about a declaring subscriber:
 * does it need rows this run has already sent?
 */
export type RowsExist = (
  req: BackfillRequest,
  from: Mark | null,
  to: Mark,
) => Promise<boolean>;

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

  /** Where the next run of this table should start. */
  resumeFrom: Mark | null;
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

  /** Set from the run's own announcement, once it has been pushed. */
  announcement?: BackfillStarted | undefined;

  /**
   * The mark of the last `backfill` message pushed, i.e. how far the run has
   * got. Absent for an unordered run, which produces no marks.
   */
  lastMark?: Mark | undefined;

  /**
   * Re-announcements to push before the run's next message. Draining them
   * here rather than pushing them directly is what keeps them ordered with
   * the run's rows, which is the whole of what makes an announcement mean
   * anything.
   */
  readonly pendingAnnouncements: BackfillStarted[];
};

// The backfill run counters of the resumable-backfills rollout. A restart is
// the cost of a subscriber that needs rows a run has passed; a re-announcement
// is the same situation resolved for free.
const runs = getOrCreateCounter(
  'replication',
  'backfill_runs',
  'Backfill runs started, by whether they started from the beginning or ' +
    'resumed from a mark.',
);
const restarts = getOrCreateCounter(
  'replication',
  'backfill_restarts',
  'Backfill runs canceled and restarted, by what made them restart.',
);
const reannouncements = getOrCreateCounter(
  'replication',
  'backfill_reannouncements',
  'Run announcements re-emitted to cover a subscriber whose mark the run ' +
    'had already passed, which costs no rows.',
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
  readonly #rowsExist: RowsExist;
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
    rowsExist: RowsExist = () => Promise.resolve(true),
    jsonFormat: JSONFormat = JSON_STRINGIFIED,
    minBackoffMs = MIN_BACKOFF_INTERVAL_MS,
    maxBackoffMs = MAX_BACKOFF_INTERVAL_MS,
    commitThresholdBytes: number | undefined = COMMIT_THRESHOLD_BYTES,
  ) {
    this.#lc = lc.withContext('component', 'backfill-manager');
    this.#changeStreamer = changeStreamer;
    this.#backfillStreamer = backfillStreamer;
    this.#rowsExist = rowsExist;
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

      const request: BackfillRequest = {
        ...entry.request,
        resumeFrom: entry.resumeFrom,
        minSnapshot: entry.minWatermark || null,
      };
      const state: RunningBackfillState = {
        request,
        minWatermark: entry.minWatermark,
        pendingAnnouncements: [],
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
      if (state.canceledReason || staleSnapshot) {
        if (state.canceledReason === undefined) {
          this.#stopRunningBackfill(
            `row key change at ${state.minWatermark} ` +
              `postdates backfill watermark at ${msg.watermark}`,
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
      await changeStream.push(['data', msg]);
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

        // Re-announcements are pushed here, and not from
        // `onBackfillRequest()`, so that they are ordered with the run's rows:
        // "anyone at this mark is covered from here" is a statement about a
        // position in the stream, and means nothing out of order with it.
        for (const announcement of state.pendingAnnouncements.splice(0)) {
          backfillTx = await pushMessage(announcement, 0);
          if (backfillTx === null) {
            lc.info?.(
              `backfill stream canceled: ${state.canceledReason}`,
              state.request,
            );
            this.#checkAndStartBackfill();
            return;
          }
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

        // Recorded after the push, so that `lastMark` is never ahead of what
        // subscribers have been sent.
        if (msg.tag === 'backfill-started') {
          state.announcement = msg;
          const start = msg.resumeFrom === null ? 'zero' : 'resumed';
          runs.add(1, {start, table: msg.relation.name});
          // The one line that says which run is on the wire and where it picked
          // up. `backfillRun` is structured because everything downstream of it
          // -- the soak harness included -- wants the fields, not the prose.
          lc.info?.(
            `run ${msg.runID} of ${msg.relation.name} is streaming from ` +
              `${
                msg.resumeFrom === null
                  ? 'the beginning'
                  : JSON.stringify(msg.resumeFrom)
              }`,
            {
              backfillRun: {
                runID: msg.runID,
                schema: msg.relation.schema,
                table: msg.relation.name,
                columns: msg.columns,
                start,
                resumeFrom: msg.resumeFrom,
                snapshot: msg.watermark,
              },
            },
          );
        } else if (msg.tag === 'backfill' && msg.lastKey !== undefined) {
          state.lastMark = msg.lastKey;
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
   * Handles a subscriber's declared progress on a backfill, forwarded by the
   * change-streamer because it could not resolve it from its own change log:
   * either the subscriber needs rows a running run has already sent, or it
   * needs a table this session has already finished.
   *
   * Everything here is decided without ordering keys outside Postgres: a mark
   * is compared for equality, or handed to {@link RowsExist}.
   */
  async onBackfillRequest(message: BackfillRequestMessage): Promise<void> {
    const [
      ,
      {table, columns, mark: declared, markWatermark, runID, subscriberID},
    ] = message;
    // On the context rather than at each site: every decision this method
    // logs -- a restart above all -- is about one subscriber's declaration,
    // and naming it is what makes a restart attributable after the fact.
    const lc = this.#lc
      .withContext('table', table.name)
      .withContext('declaredBy', subscriberID ?? 'unknown');
    let entry = this.#requiredBackfills.get(table);

    // A mark recorded at a snapshot older than a row key change on the table
    // cannot be resumed from: a row whose key moved from above it to below it
    // was sent by neither the run that passed it nor a run resumed after it.
    let mark = declared;
    if (
      mark !== null &&
      entry !== undefined &&
      (markWatermark ?? '') < entry.minWatermark
    ) {
      lc.info?.(
        `dropping a mark taken at ${markWatermark}, which predates the row ` +
          `key change at ${entry.minWatermark}`,
      );
      mark = null;
    }

    if (entry === undefined) {
      // Scenario B: this session already finished the table, so it has no
      // `minSnapshot` for it -- the cookie row is gone -- and cannot tell
      // whether the declared mark is still safe. Rather than scan the log for
      // key changes on a table it has forgotten, it starts from the
      // beginning. Every other subscriber ignores the resulting rows through
      // the column guard.
      lc.info?.(
        `adding a backfill for a table this session already finished; ` +
          `it will run from the beginning`,
        {columns: Object.keys(columns)},
      );
      this.#setRequiredBackfill('backfill-request', {
        table,
        columns,
        resumeFrom: null,
      });
      entry = must(this.#requiredBackfills.get(table));
      mark = null;
    }

    const running = this.#backfillRunningFor(table);
    // The manager may have completed only some of the subscriber's columns.
    // Retain those obligations even when another column is still running.
    if (Object.keys(columns).some(col => !(col in entry.request.columns))) {
      entry.request = {
        ...entry.request,
        columns: {...columns, ...entry.request.columns},
      };
      entry.resumeFrom = null;
      mark = null;
      if (running) {
        this.#restartRun(lc, entry, running, null, 'declaration');
        return;
      }
    }
    if (running === null) {
      // No run to compare against, so the next one has to cover both this
      // subscriber and whatever the entry was already going to start from.
      // Equal marks cost nothing; different ones cannot be ordered here, so
      // the run starts from the beginning, which covers both.
      // The next run has to cover this subscriber as well as whatever it was
      // already going to start from. A mark equal to the pending start covers
      // both -- which is the common case, since a fleet restoring from one
      // backup declares one mark, and a restarted manager seeds the pending
      // start from its own replica's. Anything else cannot be ordered
      // outside Postgres, so the run starts from the beginning, which covers
      // everyone. (Starting from the declared mark instead would be a
      // regression in coverage for any subscriber that has no mark at all.)
      if (
        entry.resumeFrom !== null &&
        JSON.stringify(entry.resumeFrom) !== JSON.stringify(mark)
      ) {
        lc.info?.(
          `the next run of ${table.name} will start from the beginning`,
          {declared: mark, wouldHaveResumedFrom: entry.resumeFrom},
        );
        entry.resumeFrom = null;
      }
      this.#checkAndStartBackfill();
      return;
    }

    if (runID !== null && runID === running.announcement?.runID) {
      lc.debug?.(`subscriber is already following run ${runID}`);
      return; // nothing to do: it has everything the run has sent
    }

    const {lastMark} = running;
    if (lastMark === undefined) {
      // An unordered run produces no marks, so there is no way to say "anyone
      // at `mark` is covered from here". Restart from the beginning, which
      // covers everyone.
      this.#restartRun(lc, entry, running, null, 'declaration');
      return;
    }

    // Does this subscriber need rows the run has already passed?
    let needed: boolean;
    try {
      needed = await this.#rowsExist(running.request, mark, lastMark);
    } catch (e) {
      lc.warn?.(`error checking for rows the run has passed`, e);
      // Restarting is the safe answer: it re-sends rows the subscriber may
      // already have, which the column guard and the following rule make
      // harmless.
      needed = true;
    }
    if (this.#backfillRunningFor(table) !== running) {
      return; // the run ended or was canceled while the query was in flight
    }

    if (needed) {
      this.#restartRun(lc, entry, running, mark, 'declaration');
      return;
    }

    // The run has sent nothing this subscriber needs, so it can be told that
    // everything after its mark is covered from here on. Re-announcing costs
    // no rows and leaves existing followers alone: they are already
    // following this run, and the rule keeps them following it.
    const announcement = must(running.announcement);
    lc.info?.(`re-announcing run ${announcement.runID} from the mark`, {mark});
    running.pendingAnnouncements.push({...announcement, resumeFrom: mark});
    reannouncements.add(1, {table: table.name});
  }

  #restartRun(
    lc: LogContext,
    entry: RequiredBackfill,
    running: RunningBackfillState,
    resumeFrom: Mark | null,
    reason: 'declaration' | 'key-change',
  ) {
    entry.resumeFrom = resumeFrom;
    lc.info?.(
      `restarting the backfill of ${entry.request.table.name} from ` +
        `${resumeFrom === null ? 'the beginning' : JSON.stringify(resumeFrom)}`,
    );
    restarts.add(1, {reason, table: entry.request.table.name});
    this.#stopRunningBackfill(`restarting from a subscriber's mark`, running);
    this.#checkAndStartBackfill();
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

  #setRequiredBackfill(source: string, req: BackfillRequest) {
    const existing = this.#requiredBackfills.get(req.table);
    const action = existing ? 'updated' : 'added';
    this.#lc.info?.(`Backfill ${action}: ${source}`, {backfill: req});
    this.#requiredBackfills.set(req.table, {
      // The resume state of a table survives its request being updated (a
      // rename, a metadata change, a column added or dropped): none of those
      // move any row, so none of them invalidate a mark.
      minWatermark: existing?.minWatermark ?? req.minSnapshot ?? '',
      resumeFrom: existing?.resumeFrom ?? req.resumeFrom ?? null,
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
        const backfillRequest = this.#requiredBackfills.get(table)?.request;
        if (backfillRequest) {
          this.#setRequiredBackfill(tag, {
            ...backfillRequest,
            table: {...backfillRequest.table, metadata},
          });
          if (this.#backfillRunningFor(table)) {
            this.#stopRunningBackfill(`TableMetadata updated`);
          }
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
        const backfillRequest = this.#requiredBackfills.get(old)?.request;
        if (backfillRequest) {
          const {schema, name} = newTable;
          this.#deleteRequiredBackfill(tag, old);
          this.#setRequiredBackfill(tag, {
            ...backfillRequest,
            table: {...backfillRequest.table, schema, name},
          });
          if (this.#backfillRunningFor(old)) {
            this.#stopRunningBackfill(`table renamed`);
          }
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
            const backfill = this.#backfillRunningFor(table);
            if (backfill && oldName in backfill.request.columns) {
              this.#stopRunningBackfill(`column renamed`);
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
          }
          const backfill = this.#backfillRunningFor(table);
          if (backfill && column in backfill.request.columns) {
            this.#stopRunningBackfill(`column dropped`);
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
        if (!isRowKeyChange(change)) {
          break;
        }
        const {relation} = change;
        const txWatermark = must(this.#currentTxWatermark, `not in a tx`);
        // Recorded on the required entry rather than only on the running
        // state, so that a key change on a table whose run is not currently
        // active is not forgotten -- and so that a mark declared later, from
        // a snapshot older than this, is dropped rather than resumed from.
        const entry = this.#requiredBackfills.get(relation);
        if (entry) {
          entry.minWatermark = txWatermark;
          entry.resumeFrom = null;
        }
        const backfill = this.#backfillRunningFor(relation);
        if (backfill) {
          backfill.minWatermark = txWatermark;
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
        const backfillRequest = this.#requiredBackfills.get(relation)?.request;
        assert(
          backfillRequest,
          () => `No BackfillRequest completed backfill ${stringify(change)}`,
        );
        const remaining = Object.entries(backfillRequest.columns).filter(
          ([col]) =>
            !(columns.includes(col) || relation.rowKey.columns.includes(col)),
        );
        if (remaining.length === 0) {
          this.#deleteRequiredBackfill(tag, relation);
        } else {
          this.#setRequiredBackfill(tag, {
            ...backfillRequest,
            columns: Object.fromEntries(remaining),
          });
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
