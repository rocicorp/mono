import {LogContext} from '@rocicorp/logger';
import fc from 'fast-check';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {must} from '../../../../shared/src/must.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {ChangeStream} from '../../services/change-source/change-source.ts';
import {schemaVersionMigrationMap} from '../../services/change-source/common/replica-schema.ts';
import type {
  ChangeStreamData,
  ChangeStreamMessage,
} from '../../services/change-source/protocol/current/downstream.ts';
import {BackfillDeclarations} from '../../services/change-streamer/backfill-declarations.ts';
import {
  readReplicaInitializationParameters,
  withResumeMarks,
} from '../../services/change-streamer/change-log-initializer.ts';
import {ChangeProcessor} from '../../services/replicator/change-processor.ts';
import {
  BACKFILLING_TABLE,
  readBackfillDeclarations,
} from '../../services/replicator/schema/backfilling.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../../services/replicator/schema/constants.ts';
import {initReplicationState} from '../../services/replicator/schema/replication-state.ts';
import {liteTableName} from '../../types/names.ts';
import {id} from '../../types/sql.ts';
import {majorVersionOf} from '../../types/state-version.ts';
import {Census, setActiveCensus} from './census.ts';
import {SimClock} from './clock.ts';
import {SeededMathRandom, stepSeed} from './random.ts';
import {
  backfillingColumns,
  checkReplicaContent,
  columnDefaults,
  diffReplica,
  replicaStateVersion,
} from './replica.ts';
import {SimChangeSource} from './sim-change-source.ts';
import {SimPG, type BackfillFault} from './sim-pg.ts';
import {Trace} from './trace.ts';
import {
  applyWorkloadOp,
  backfillWorkloadTxArb,
  MAX_ROW_ID,
  type WorkloadOp,
} from './workload.ts';

const EPOCH_MS = Date.UTC(2026, 0, 1);
const SUBSCRIBER_ID = 'sim-replica';
const HEAL_ROUNDS = 40;
const HEAL_ROUND_MS = 5_000;
/** Heal rounds at the end with no commits, so the replica can reach the head. */
const HEAL_QUIET_ROUNDS = 8;

/** Everything about a backfill run that is not a step. */
export type BackfillRunConfig = {
  readonly seed: number;
  /** Rows per `backfill` message. */
  readonly batchRows: number;
  readonly commitThresholdBytes: number;
  /** Whether runs are ordered by row key, and so resumable. */
  readonly resume: boolean;
};

export const backfillRunConfigArb: fc.Arbitrary<BackfillRunConfig> = fc.record({
  seed: fc.integer({min: 0, max: 0x7fffffff}),
  batchRows: fc.integer({min: 1, max: 3}),
  commitThresholdBytes: fc.constantFrom(1, 64, 1 << 20),
  resume: fc.constantFrom(true, true, true, false),
});

export const PINNED_BACKFILL_CONFIG: BackfillRunConfig = {
  seed: 1,
  batchRows: 1,
  commitThresholdBytes: 1,
  resume: true,
};

export type BackfillStepAction =
  | {
      readonly kind: 'commit';
      readonly ops: readonly WorkloadOp[];
      readonly gap: number;
    }
  | {readonly kind: 'deliver'; readonly n: number}
  | {readonly kind: 'idle'}
  | {readonly kind: 'advance'; readonly ms: number}
  // The subscriber stops reading, which back-pressures every producer.
  | {readonly kind: 'pause'}
  | {readonly kind: 'pull'; readonly n: number}
  | {readonly kind: 'resume'}
  // The upstream connection drops. The change-streamer rolls back the
  // interrupted transaction, reconnects, and re-sends every declaration.
  | {readonly kind: 'sourceDisconnect'; readonly partial: number | undefined}
  // The replication-manager restarts: a new source session from its replica,
  // and a subscriber that reconnects with what its replica declares.
  | {readonly kind: 'managerRestart'}
  | {readonly kind: 'backfillFault'; readonly fault: BackfillFault}
  // A rollback to a v17 zero-cache, which replicates a TOASTed row key change
  // but maintains none of the resume columns, then the roll forward to v18.
  | {
      readonly kind: 'v17KeyChange';
      readonly table: number;
      readonly from: number;
      readonly to: number;
    }
  // Scenario A: a manager this subscriber moved away from re-delivers a run
  // that has already completed here, after upstream moved on.
  | {readonly kind: 'replayStaleRun'}
  // Scenario C: the subscriber's replica follows a run at another manager, and
  // its declaration reaches this manager's run mid-run.
  | {readonly kind: 'forgetRun'};

export type BackfillStep = BackfillStepAction & {readonly id: number};

const idArb = fc.integer({min: 1, max: MAX_ROW_ID});

const backfillActionArb: fc.Arbitrary<BackfillStepAction> = fc.oneof(
  {
    weight: 5,
    arbitrary: fc.record({
      kind: fc.constant('commit' as const),
      ops: backfillWorkloadTxArb,
      gap: fc.integer({min: 1, max: 40}),
    }),
  },
  {
    weight: 4,
    arbitrary: fc.record({
      kind: fc.constant('deliver' as const),
      n: fc.integer({min: 1, max: 4}),
    }),
  },
  {weight: 1, arbitrary: fc.constant<BackfillStepAction>({kind: 'idle'})},
  {
    weight: 2,
    arbitrary: fc.record({
      kind: fc.constant('advance' as const),
      ms: fc.oneof(
        fc.integer({min: 1, max: 1_000}),
        fc.integer({min: 1_000, max: 70_000}),
      ),
    }),
  },
  {weight: 2, arbitrary: fc.constant<BackfillStepAction>({kind: 'pause'})},
  {
    weight: 3,
    arbitrary: fc.record({
      kind: fc.constant('pull' as const),
      n: fc.integer({min: 1, max: 8}),
    }),
  },
  {weight: 1, arbitrary: fc.constant<BackfillStepAction>({kind: 'resume'})},
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant('sourceDisconnect' as const),
      partial: fc.option(fc.integer({min: 1, max: 4}), {nil: undefined}),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.constant<BackfillStepAction>({kind: 'managerRestart'}),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant('backfillFault' as const),
      fault: fc.oneof(
        fc.constant<BackfillFault>({at: 'snapshot'}),
        fc.record({
          at: fc.constant('copy' as const),
          afterBatches: fc.integer({min: 0, max: 3}),
        }),
      ),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant('v17KeyChange' as const),
      table: fc.nat({max: 7}),
      from: idArb,
      to: idArb,
    }),
  },
  {
    weight: 1,
    arbitrary: fc.constant<BackfillStepAction>({kind: 'replayStaleRun'}),
  },
  {
    weight: 1,
    arbitrary: fc.constant<BackfillStepAction>({kind: 'forgetRun'}),
  },
);

export function backfillStepsArb(
  maxLength: number,
): fc.Arbitrary<BackfillStep[]> {
  return fc
    .array(
      fc.record({
        action: backfillActionArb,
        id: fc.integer({min: 0, max: 0x7fffffff}),
      }),
      {maxLength, size: 'max'},
    )
    .map(steps => steps.map(({action, id}) => ({...action, id})));
}

/** A fixed step list, for a pinned scenario. */
export function pinnedBackfill(
  actions: readonly BackfillStepAction[],
): BackfillStep[] {
  return actions.map((action, i) => ({...action, id: i}));
}

export type BackfillRunResult = {
  readonly hash: string;
  readonly census: Census;
};

export async function runBackfillSimulation(
  config: BackfillRunConfig,
  steps: readonly BackfillStep[],
): Promise<BackfillRunResult> {
  return await new BackfillSimulation(config).run(steps);
}

/** A message as the trace records it: enough to follow a run, not its rows. */
function summarize(msg: ChangeStreamMessage): Record<string, unknown> {
  switch (msg[0]) {
    case 'begin':
      return {
        begin: msg[2].commitWatermark,
        ...(msg[1].backfill ? {backfill: true} : {}),
      };
    case 'commit':
      return {commit: msg[2].watermark};
    case 'rollback':
      return {rollback: true};
    case 'status':
      return {status: msg[2].watermark};
    case 'control':
      return {control: true};
    case 'data': {
      const change = msg[1];
      switch (change.tag) {
        case 'backfill':
          return {
            tag: change.tag,
            table: change.relation.name,
            columns: change.columns,
            runID: change.runID,
            keys: change.rowValues.map(row => row[0]),
            lastKey: change.lastKey,
          };
        case 'backfill-started':
          return {
            tag: change.tag,
            table: change.relation.name,
            columns: change.columns,
            runID: change.runID,
            resumeFrom: change.resumeFrom,
          };
        case 'backfill-completed':
          return {
            tag: change.tag,
            table: change.relation.name,
            columns: change.columns,
            runID: change.runID,
          };
        default:
          return {tag: change.tag};
      }
    }
  }
}

type Columns = Map<string, Set<string>>;
type Rows = Map<string, Map<string, Record<string, unknown>>>;

type OpenTransaction = {
  readonly watermark: string;
  readonly backfill: boolean;
  /** The replica's in-flight columns when the transaction began, by table. */
  readonly backfilling: Columns;
  /** For a backfill transaction, every table's rows when it began. */
  readonly rows: Rows | undefined;
};

/**
 * Backfills against one replica (D2b): the real `BackfillManager` and change
 * source glue over SimPG, into one replica through `ChangeProcessor`, with no
 * change-streamer. The simulation stands in for the change-streamer's part:
 * it acks what the replica commits, tracks the replica's declarations with
 * `BackfillDeclarations` and forwards its requests as `Subscriber` does, and
 * reconnects a source from the replica as the change-log initializer does.
 *
 * After every transaction it checks what is true upstream, never the rules:
 *
 * - 7: the replica's columns that are not being backfilled equal upstream's at
 *   the replica's major. A run's snapshot can be ahead of the replica, so a
 *   backfilling table may hold a phantom: a row from upstream's future whose
 *   other columns are still empty. Never a hole.
 * - 8: never stale: a backfilling column is empty or holds a value upstream
 *   has at the replica's major or later.
 * - 9: each column completes once. (Never half is 7, from the completion on.)
 * - 11: the column guard: a backfill transaction writes only the columns the
 *   replica has in flight, and bumps versions only of a table that completed.
 *
 * Heal stops the faults and must end with every backfill completed and the
 * replica equal to upstream.
 */
class BackfillSimulation {
  readonly census = new Census();
  readonly #config: BackfillRunConfig;
  readonly #trace: Trace;
  readonly #clock = new SimClock(EPOCH_MS);
  readonly #lc: LogContext;
  readonly #pg: SimPG;
  readonly #failures: string[] = [];
  readonly #completed = new Set<string>();
  readonly #applied: string[] = [];
  /** Every run's messages, as the replica applied them, by run ID. */
  readonly #runMessages = new Map<string, ChangeStreamData[]>();
  #lastCompletedRun: string | undefined;
  #replica: Database | undefined;
  #source: SimChangeSource | undefined;
  #processor: ChangeProcessor | undefined;
  #tracker: BackfillDeclarations | undefined;
  #stream: ChangeStream | undefined;
  #transaction: OpenTransaction | undefined;
  #lastRequests = '';
  #requestCovered = false;
  #random: SeededMathRandom | undefined;
  #paused = false;
  #credit = 0;
  readonly #waiters = new Set<() => void>();
  /** Set once the run has ended, after which nothing touches the replica. */
  #done = false;

  constructor(config: BackfillRunConfig) {
    this.#config = config;
    this.#trace = new Trace({
      now: () => this.#clock.elapsed(),
      runDirs: [],
      recent: 400,
      listener: event => this.census.observe(event),
    });
    this.#lc = new LogContext('debug', {}, this.#trace.sink('backfill', 0));
    this.#pg = new SimPG(this.#trace);
  }

  async run(steps: readonly BackfillStep[]): Promise<BackfillRunResult> {
    setActiveCensus(this.census);
    this.#clock.install();
    this.#random = new SeededMathRandom(this.#config.seed);
    const replica = new Database(this.#lc, ':memory:');
    this.#replica = replica;
    try {
      initReplicationState(replica, ['zero_data'], this.#pg.replicaVersion);
      this.#processor = this.#newProcessor();
      this.#tracker = BackfillDeclarations.forSubscriber(true, []);
      this.#source = new SimChangeSource(this.#lc, this.#pg, this.#config);
      await this.#connect(false);
      for (const [i, step] of steps.entries()) {
        this.#random.reseed(stepSeed(this.#config.seed, step.id));
        this.#trace.emit('backfill', 0, 'step', {i, step});
        await this.#execute(step);
        await this.#clock.advance(1);
        await this.#settle();
        this.#check(`step ${i} (${step.kind})`);
      }
      await this.#heal();
      return {hash: this.#trace.hash, census: this.census};
    } catch (e) {
      throw new Error(
        `${e instanceof Error ? e.message : String(e)}\n\n` +
          `the last trace events:\n${this.#trace.recent().join('\n')}`,
      );
    } finally {
      this.#done = true;
      this.#stream?.changes.cancel();
      this.#random[Symbol.dispose]();
      this.#clock.uninstall();
      setActiveCensus(undefined);
      replica.close();
    }
  }

  async #execute(step: BackfillStep): Promise<void> {
    if (!this.#stream && step.kind !== 'managerRestart') {
      await this.#connect(true);
    }
    const pg = this.#pg;
    switch (step.kind) {
      case 'commit': {
        const tx = pg.begin();
        for (const op of step.ops) {
          applyWorkloadOp(tx, op);
        }
        pg.commit(tx, step.gap);
        break;
      }
      case 'deliver':
        pg.deliver(step.n);
        break;
      case 'idle':
        pg.keepalive();
        break;
      case 'advance':
        await this.#clock.advance(step.ms);
        break;
      case 'pause':
        this.#paused = true;
        this.#credit = 0;
        break;
      case 'pull':
        this.#credit += step.n;
        this.#nudge();
        break;
      case 'resume':
        this.#paused = false;
        this.#nudge();
        break;
      case 'sourceDisconnect':
        pg.disconnect(step.partial);
        break;
      case 'managerRestart':
        await this.#managerRestart();
        break;
      case 'backfillFault':
        pg.failNextBackfill(step.fault);
        break;
      case 'v17KeyChange':
        await this.#v17KeyChange(step);
        break;
      case 'replayStaleRun':
        this.#replayStaleRun();
        break;
      case 'forgetRun':
        this.#forgetRun();
        break;
    }
  }

  /**
   * Starts a source session from the replica, as the change-log initializer
   * does from a replication-manager's replica. When `sourceRestarted` under a
   * connected subscriber, every declaration is re-sent at once, as the
   * change-streamer does. A subscriber that has just connected declares at its
   * next transaction instead.
   */
  async #connect(sourceRestarted: boolean): Promise<void> {
    const replica = must(this.#replica);
    const params = readReplicaInitializationParameters(replica);
    const requests = withResumeMarks(params.backfillRequests, params.marks);
    const watermark = majorVersionOf(params.lastWatermark);
    this.#trace.emit('backfill', 0, 'connect', {watermark, requests});
    if (requests.some(r => r.resumeFrom)) {
      this.census.note('connect:resume-mark');
    }
    const stream = await must(this.#source).startStream(watermark, requests);
    this.#stream = stream;
    void this.#consume(stream);
    if (sourceRestarted) {
      this.#requestBackfills(true);
    }
  }

  async #managerRestart(): Promise<void> {
    const stream = this.#stream;
    this.#stream = undefined;
    stream?.changes.cancel();
    if (this.#transaction) {
      // The subscriber's replicator aborts the transaction it was in.
      must(this.#processor).abort(this.#lc);
      this.#processor = this.#newProcessor();
      this.#transaction = undefined;
    }
    this.#tracker = BackfillDeclarations.forSubscriber(
      true,
      readBackfillDeclarations(must(this.#replica)),
    );
    this.#lastRequests = '';
    this.#requestCovered = false;
    this.#nudge();
    await this.#connect(false);
  }

  /**
   * A v17 zero-cache replicates a TOASTed row key change, and every resume
   * column the v18 rules would have changed keeps its old value. Then the roll
   * forward: migration 18's `migrateData` runs again, and the process restarts.
   */
  async #v17KeyChange(
    step: Extract<BackfillStep, {kind: 'v17KeyChange'}>,
  ): Promise<void> {
    const replica = must(this.#replica);
    const tx = this.#pg.begin();
    const moved = applyWorkloadOp(tx, {
      op: 'keyChange',
      table: step.table,
      id: step.from,
      to: step.to,
      value: step.from,
      toasted: true,
    });
    if (!moved || this.#paused) {
      return;
    }
    const kept = replica
      .prepare(/*sql*/ `SELECT "schema", "table", "column", "mark", "markWatermark",
          "runID", "minSnapshot" FROM "${BACKFILLING_TABLE}"`)
      .all<Record<string, string | null>>();
    this.#pg.commit(tx, 1);
    this.#pg.deliver(Number.MAX_SAFE_INTEGER);
    await this.#settle();
    const restore = replica.prepare(/*sql*/ `UPDATE "${BACKFILLING_TABLE}"
        SET "mark" = ?, "markWatermark" = ?, "runID" = ?, "minSnapshot" = ?
        WHERE "schema" = ? AND "table" = ? AND "column" = ?`);
    for (const row of kept) {
      restore.run(
        row['mark'],
        row['markWatermark'],
        row['runID'],
        row['minSnapshot'],
        row['schema'],
        row['table'],
        row['column'],
      );
    }
    // Optional, as the migration runner calls it.
    await schemaVersionMigrationMap[18].migrateData?.(this.#lc, replica);
    this.census.note('v17:rollback');
    await this.#managerRestart();
  }

  /**
   * Scenario A: re-applies the last run to complete here, as a manager this
   * subscriber moved away from would re-deliver it, in a backfill transaction
   * of its own. The column guard must skip its rows, now that upstream has
   * moved on, and its completion must be ignored.
   */
  #replayStaleRun(): void {
    const runID = this.#lastCompletedRun;
    const messages =
      runID === undefined ? undefined : this.#runMessages.get(runID);
    if (!messages || this.#transaction) {
      return;
    }
    // A manager cancels a run at the DDL that renames or drops its table, so
    // no stream carries the run's rows past it.
    const replica = must(this.#replica);
    const tableExists = replica.prepare(
      /*sql*/ `SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`,
    );
    if (
      messages.some(
        msg =>
          msg[0] === 'data' &&
          'relation' in msg[1] &&
          tableExists.get(liteTableName(msg[1].relation)) === undefined,
      )
    ) {
      return;
    }
    const watermark = majorVersionOf(replicaStateVersion(replica));
    this.census.note('backfill:stale-replay');
    try {
      this.#apply([
        'begin',
        {tag: 'begin', skipAck: true, backfill: true},
        {commitWatermark: watermark},
      ]);
      messages.forEach(message => this.#apply(message));
      this.#apply(['commit', {tag: 'commit'}, {watermark}]);
    } catch (e) {
      this.#failures.push(`replaying run ${runID} failed: ${String(e)}`);
    }
  }

  /**
   * Scenario C: the replica now follows a run at another manager, so it does
   * not follow the one this manager is sending. The subscriber reconnects, and
   * its declaration reaches the running run.
   */
  #forgetRun(): void {
    const replica = must(this.#replica);
    if (this.#transaction || !this.#stream) {
      return;
    }
    const {changes} = replica
      .prepare(
        /*sql*/ `UPDATE "${BACKFILLING_TABLE}" SET "runID" = 'other-manager'`,
      )
      .run();
    if (changes === 0) {
      return;
    }
    this.census.note('backfill:forgot-run');
    this.#tracker = BackfillDeclarations.forSubscriber(
      true,
      readBackfillDeclarations(replica),
    );
    this.#lastRequests = '';
    this.#requestBackfills(true);
  }

  async #consume(stream: ChangeStream): Promise<void> {
    const messages = stream.changes[Symbol.asyncIterator]();
    try {
      for (;;) {
        if (!(await this.#waitForCredit(stream))) {
          return;
        }
        const next = await messages.next();
        if (next.done) {
          return;
        }
        this.#receive(stream, next.value);
      }
    } catch (e) {
      const message = String(e);
      if (
        !message.includes('connection to the upstream was lost') &&
        !message.includes('terminating connection')
      ) {
        this.#failures.push(`the change stream failed: ${message}`);
      }
    } finally {
      if (this.#stream === stream) {
        this.#stream = undefined;
        if (this.#transaction && !this.#done) {
          // What the change-streamer forwards for an interrupted transaction.
          this.#apply(['rollback', {tag: 'rollback'}]);
        }
      }
    }
  }

  #receive(stream: ChangeStream, msg: ChangeStreamMessage): void {
    if (this.#done) {
      return;
    }
    this.#trace.emit('backfill', 0, 'received', summarize(msg));
    if (msg[0] === 'status') {
      if (msg[1].ack && this.#transaction === undefined) {
        stream.acks.push(['status', {tag: 'commit'}, msg[2]]);
      }
      return;
    }
    if (msg[0] === 'control') {
      return;
    }
    // Round-tripped through JSON, as the wire does.
    const data = BigIntJSON.parse(
      BigIntJSON.stringify(msg),
    ) as ChangeStreamData;
    try {
      this.#apply(data);
    } catch (e) {
      this.#failures.push(`applying ${data[0]} failed: ${String(e)}`);
      this.#stream = undefined;
      stream.changes.cancel();
      return;
    }
    if (data[0] === 'commit' && !data[2].watermark.includes('.')) {
      // What the replica has committed, the change-streamer acks.
      stream.acks.push([
        'status',
        {tag: 'commit'},
        {watermark: data[2].watermark},
      ]);
    }
  }

  #apply(data: ChangeStreamData): void {
    if (data[0] === 'begin') {
      const backfill = data[1].backfill === true;
      this.#transaction = {
        watermark: data[2].commitWatermark,
        backfill,
        backfilling: this.#backfillingColumns(),
        rows: backfill ? this.#replicaRows() : undefined,
      };
    }
    must(this.#processor).processMessage(this.#lc, data);
    this.#tracker?.apply(data);
    if (data[0] === 'data') {
      const change = data[1];
      if (
        (change.tag === 'backfill-started' ||
          change.tag === 'backfill' ||
          change.tag === 'backfill-completed') &&
        change.runID !== undefined
      ) {
        this.#runMessages.set(change.runID, [
          ...(this.#runMessages.get(change.runID) ?? []),
          data,
        ]);
        if (change.tag === 'backfill-completed') {
          this.#lastCompletedRun = change.runID;
        }
      }
    }
    if (data[0] === 'commit') {
      const tx = must(this.#transaction);
      this.#transaction = undefined;
      if (!tx.backfill) {
        this.#applied.push(data[2].watermark);
      }
      this.#checkTransaction(tx);
      this.#requestBackfills();
    } else if (data[0] === 'rollback') {
      this.#transaction = undefined;
      this.#requestBackfills();
    }
  }

  /** `Subscriber.requestBackfills`, for the one subscriber. */
  #requestBackfills(sourceRestarted = false): void {
    // A subscriber may have no tracker, as `Subscriber` allows.
    const tracker = this.#tracker;
    if (!tracker?.pending) {
      return;
    }
    this.#requestCovered ||= sourceRestarted;
    const stream = this.#stream;
    if (!stream || tracker.inTransaction) {
      return;
    }
    const requests = tracker.requests(SUBSCRIBER_ID, this.#requestCovered);
    const serialized = BigIntJSON.stringify(requests);
    const changed = serialized !== this.#lastRequests;
    this.#lastRequests = serialized;
    const force = this.#requestCovered;
    this.#requestCovered = false;
    if (requests.length && (changed || force)) {
      for (const request of requests) {
        stream.acks.push(request);
      }
      this.census.note('declaration:forwarded', requests.length);
    }
  }

  #checkTransaction(tx: OpenTransaction): void {
    const replica = must(this.#replica);
    const pg = this.#pg;
    const fail = (oracle: string, message: string) =>
      this.#failures.push(`oracle ${oracle} after ${tx.watermark}: ${message}`);

    // The replica's majors are exactly upstream's commits, in order.
    const expected = pg.commits
      .slice(1, this.#applied.length + 1)
      .map(c => c.watermark);
    if (this.#applied.join() !== expected.join()) {
      fail(
        '1 (contiguous)',
        `applied [${this.#applied}], upstream [${expected}]`,
      );
    }

    // 7 and 8.
    for (const {oracle, message} of checkReplicaContent(replica, pg, () =>
      this.census.note('backfill:phantom-row'),
    )) {
      fail(oracle, message);
    }

    if (!tx.backfill) {
      return;
    }

    const {state} = pg.commitAt(majorVersionOf(replicaStateVersion(replica)));
    const backfilling = this.#backfillingColumns();
    const isBackfilling = (table: string, column: string) =>
      backfilling.get(table)?.has(column) ?? false;

    // 9: each column completes once.
    const completedTables = new Set<string>();
    for (const [table, columns] of tx.backfilling) {
      for (const column of columns) {
        if (isBackfilling(table, column)) {
          continue;
        }
        completedTables.add(table);
        const attnum = state.tables
          .get(table)
          ?.columns.find(c => c.name === column)?.attnum;
        const key = `${state.tables.get(table)?.oid}.${attnum}`;
        this.census.note('completion:honored');
        if (this.#completed.has(key)) {
          fail('9 (completes once)', `${table}.${column} completed again`);
        }
        this.#completed.add(key);
      }
    }

    // 11: the column guard.
    const before = must(tx.rows);
    for (const [table, rows] of this.#replicaRows()) {
      const allowed = tx.backfilling.get(table) ?? new Set();
      const defaults = columnDefaults(replica, table);
      const previous = before.get(table);
      if (!previous) {
        fail('11 (column guard)', `${table} was created`);
        continue;
      }
      for (const [key, row] of rows) {
        const old = previous.get(key);
        for (const [col, value] of Object.entries(row)) {
          const was = old?.[col] ?? null;
          if (
            value === was ||
            allowed.has(col) ||
            (old === undefined &&
              (col === 'id' ||
                value === null ||
                value === defaults.get(col))) ||
            (col === ZERO_VERSION_COLUMN_NAME &&
              (completedTables.has(table) || old === undefined))
          ) {
            continue;
          }
          fail(
            '11 (column guard)',
            `${table} row ${key}: ${col} went from ${JSON.stringify(was)} ` +
              `to ${JSON.stringify(value)}, which is not being backfilled ` +
              `(in flight: [${[...allowed]}])`,
          );
        }
      }
      for (const key of previous.keys()) {
        if (!rows.has(key)) {
          fail('11 (column guard)', `${table} row ${key} was deleted`);
        }
      }
    }
  }

  /**
   * The spec's fairness for backfills: no new faults, the subscriber reads,
   * a connection comes back, commits keep arriving for a while and then stop,
   * and time moves past every backoff. Then every backfill must have
   * completed.
   */
  async #heal(): Promise<void> {
    this.#trace.emit('backfill', 0, 'heal');
    this.#paused = false;
    this.#nudge();
    for (let round = 0; round < HEAL_ROUNDS; round++) {
      must(this.#random).reseed(stepSeed(this.#config.seed, -1 - round));
      if (!this.#stream) {
        await this.#connect(true);
      }
      if (round < HEAL_ROUNDS - HEAL_QUIET_ROUNDS && round % 4 === 0) {
        this.#trickle(round);
      }
      this.#pg.deliver(Number.MAX_SAFE_INTEGER);
      await this.#clock.advance(HEAL_ROUND_MS);
      await this.#settle();
      this.#check(`heal round ${round}`);
      if (
        round >= HEAL_ROUNDS - HEAL_QUIET_ROUNDS &&
        !this.#unsettled().length
      ) {
        this.census.note('backfill:completes');
        return;
      }
    }
    throw new Error(
      `the backfills never completed:\n${this.#unsettled().join('\n')}`,
    );
  }

  #unsettled(): string[] {
    const replica = must(this.#replica);
    const problems: string[] = [];
    const version = replicaStateVersion(replica);
    const head = this.#pg.head.watermark;
    if (majorVersionOf(version) !== head) {
      problems.push(`the replica is at ${version}, upstream at ${head}`);
    }
    for (const [table, columns] of this.#backfillingColumns()) {
      problems.push(`${table} is still backfilling [${[...columns]}]`);
    }
    problems.push(...diffReplica(replica, this.#pg.head.state));
    return problems;
  }

  /** A commit on whatever table exists, so that runs start and requests flow. */
  #trickle(round: number): void {
    const tx = this.#pg.begin();
    if (
      !applyWorkloadOp(tx, {op: 'update', table: round, id: 1, value: round}) &&
      !applyWorkloadOp(tx, {op: 'insert', table: round, id: 1, value: round})
    ) {
      tx.createTable(['text']);
    }
    this.#pg.commit(tx, 1);
  }

  #check(at: string): void {
    const problems = [
      ...this.#failures.splice(0),
      ...this.census.takeAlarms().map(m => `alarm: ${m}`),
      ...this.#pg.takeViolations().map(m => `SimPG: ${m}`),
    ];
    if (problems.length) {
      throw new Error(`${at}:\n${problems.join('\n')}`);
    }
  }

  async #settle(): Promise<void> {
    await this.#clock.settle();
    await this.#clock.settle();
  }

  /**
   * Waits until `stream`'s consumer may read another message, or returns false
   * once `stream` is no longer the current one. {@link #nudge} wakes every
   * waiter, so that a consumer left behind by a reconnect can take neither a
   * wake-up nor a credit meant for the current one.
   */
  async #waitForCredit(stream: ChangeStream): Promise<boolean> {
    while (this.#stream === stream && this.#paused && this.#credit === 0) {
      await new Promise<void>(resolve => this.#waiters.add(resolve));
    }
    if (this.#stream !== stream) {
      return false;
    }
    if (this.#paused) {
      this.#credit--;
    }
    return true;
  }

  #nudge(): void {
    const waiters = [...this.#waiters];
    this.#waiters.clear();
    waiters.forEach(wake => wake());
  }

  #newProcessor(): ChangeProcessor {
    return new ChangeProcessor(
      new StatementRunner(must(this.#replica)),
      'serving',
      (_lc, err) => {
        throw err;
      },
    );
  }

  #backfillingColumns(): Columns {
    return backfillingColumns(must(this.#replica));
  }

  #replicaRows(): Rows {
    const replica = must(this.#replica);
    const rows: Rows = new Map();
    for (const {name} of replica
      .prepare(/*sql*/ `SELECT name FROM sqlite_master
          WHERE type = 'table'
            AND substr(name, 1, 6) <> '_zero.'
            AND substr(name, 1, 7) <> 'sqlite_'`)
      .all<{name: string}>()) {
      rows.set(
        name,
        new Map(
          replica
            .prepare(`SELECT * FROM ${id(name)}`)
            .all<Record<string, unknown>>()
            .map(row => [JSON.stringify([row['id'] ?? null]), row]),
        ),
      );
    }
    return rows;
  }
}
