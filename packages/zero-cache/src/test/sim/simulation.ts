import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  realpathSync,
  rmSync,
} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {getDefaultHighWaterMark, setDefaultHighWaterMark} from 'node:stream';
import {LogContext} from '@rocicorp/logger';
import type {Enum} from '../../../../shared/src/enum.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {errorTypeToReadableName} from '../../services/change-streamer/change-streamer.ts';
import type * as ErrorType from '../../services/change-streamer/error-type-enum.ts';
import {changeLogFileName} from '../../services/replicator/change-log-db.ts';
import {Census, setActiveCensus} from './census.ts';
import {SimClock} from './clock.ts';
import type {SimContext} from './context.ts';
import {ActiveResourceCheck, DeterminismGuard} from './guard.ts';
import {currentIncarnation, Incarnation} from './incarnation.ts';
import {Oracles} from './oracles.ts';
import {Probes} from './probes.ts';
import {SeededMathRandom, stepSeed} from './random.ts';
import {backfillingColumns, diffReplica} from './replica.ts';
import {RMNode} from './rm-node.ts';
import {SimBackup} from './sim-backup.ts';
import {SimPG} from './sim-pg.ts';
import {groupOf, type RunConfig, type Step} from './steps.ts';
import {Trace, type TraceEvent} from './trace.ts';
import {SimNetwork, type Connection, type SubscribeOrder} from './transport.ts';
import {VSNode} from './vs-node.ts';
import {applyWorkloadOp} from './workload.ts';

type ErrorType = Enum<typeof ErrorType>;

/** The virtual epoch every run starts at. */
export const SIM_EPOCH_MS = Date.UTC(2026, 0, 1);

/** `CLEANUP_DELAY_MS` in `change-streamer-service.ts`. */
const CLEANUP_DELAY_MS = 30_000;

const HEAL_ROUND_MS = 5_000;
/**
 * Heal rounds in which commits keep arriving: long enough for a restore
 * interrupted before heal to be repeated, not for one that loops.
 */
const HEAL_BUSY_ROUNDS = 30;

/** `reserveAndGetSnapshotStatus` listens for these until its stream ends. */
const RESERVATION_SIGNALS = ['SIGINT', 'SIGTERM'] as const;

/** `SnapshotReservations.#expire`'s warning. */
const RESERVATION_EXPIRED = /^releasing the snapshot reservation for (\S+),/;

export type RunOptions = {
  /** Runs the async-hooks determinism guard, which costs promise hooks. */
  readonly guard?: boolean | undefined;
  /** Writes every trace event here, as JSON lines. */
  readonly traceFile?: string | undefined;
};

export type RunResult = {
  readonly hash: string;
  readonly census: Census;
  readonly events: number;
};

/** A run that broke an oracle or a harness rule, with the trace behind it. */
export class SimFailure extends Error {
  override readonly name = 'SimFailure';

  constructor(message: string, recent: readonly string[]) {
    super(
      `${message}\n\nthe last ${recent.length} trace events:\n${recent.join('\n')}`,
    );
  }
}

export async function runSimulation(
  config: RunConfig,
  steps: readonly Step[],
  opts: RunOptions = {},
): Promise<RunResult> {
  return await new Simulation(config, opts).run(steps);
}

class Simulation implements SimContext {
  readonly config: RunConfig;
  readonly runDir: string;
  readonly lc: LogContext;
  readonly trace: Trace;
  readonly clock: SimClock;
  readonly census = new Census();
  readonly pg: SimPG;
  readonly backup: SimBackup;
  readonly network: SimNetwork;
  readonly oracles: Oracles;
  readonly rm: RMNode;
  readonly viewSyncers: VSNode[];

  readonly #opts: RunOptions;
  readonly #scope = new Incarnation('sim', 0);
  readonly #guard: DeterminismGuard | undefined;
  readonly #failures: string[] = [];
  readonly #parkedYields: (() => void)[] = [];
  readonly #probes = new Probes(this.census);
  #gatedPurge: boolean;
  #random: SeededMathRandom | undefined;
  #resources: ActiveResourceCheck | undefined;

  constructor(config: RunConfig, opts: RunOptions) {
    this.config = config;
    this.#opts = opts;
    this.#gatedPurge = config.gatedPurge;
    this.runDir = mkdtempSync(join(tmpdir(), 'zero-sim-'));
    this.trace = new Trace({
      now: () => this.clock.elapsed(),
      runDirs: [this.runDir, realpathSync(this.runDir)],
      keepAll: opts.traceFile !== undefined,
      listener: event => {
        this.census.observe(event);
        this.#observeTakeBacks(event);
        if (event.kind === 'pg.takeover') {
          this.census.note('slot:takeover');
        }
      },
    });
    this.lc = new LogContext('debug', {}, this.trace.sink('sim', 0));
    this.#guard = opts.guard
      ? new DeterminismGuard(() => currentIncarnation() !== undefined)
      : undefined;
    this.clock = new SimClock(SIM_EPOCH_MS, {
      yielding: () => this.#guard?.expectImmediate(),
    });
    this.pg = new SimPG(this.trace);
    const backupDir = join(this.runDir, 'backups');
    mkdirSync(backupDir);
    this.backup = new SimBackup(backupDir, this.trace);
    this.oracles = new Oracles(this.pg, this.lc);
    this.network = new SimNetwork(
      this.trace,
      {
        opened: conn => this.#nodeOf(conn)?.opened(conn),
        reserved: (conn, msg) => this.#nodeOf(conn)?.reserved(conn, msg),
        received: (conn, downstream) => {
          this.oracles.network.received?.(conn, downstream);
          const {data} = downstream;
          if (data[0] === 'error') {
            const type = errorTypeToReadableName(data[1].type as ErrorType);
            this.census.note(`subscriber:error/${type}`);
            const vs = this.#nodeOf(conn);
            const reserved = vs?.protectedBy(conn);
            if (type === 'WatermarkTooOld' && reserved !== undefined) {
              this.oracles.report(
                '5 (promise kept)',
                `${conn.client.name} restored under a reservation at ` +
                  `${reserved} that was never taken back, and its ` +
                  `subscription at ${conn.ctx?.watermark} was answered ` +
                  `WatermarkTooOld: ${data[1].message}`,
              );
            }
            vs?.noteError(type);
          }
        },
      },
      () => this.clock.elapsed(),
    );
    this.rm = new RMNode(this);
    this.viewSyncers = Array.from(
      {length: config.viewSyncers},
      (_, i) => new VSNode(this, i),
    );
  }

  subscribeOrder(): SubscribeOrder {
    return Math.random() < 0.5 ? 'socket' : 'registered';
  }

  purgeYield(): Promise<void> {
    if (!this.#gatedPurge) {
      return Promise.resolve();
    }
    return new Promise(resolve => this.#parkedYields.push(resolve));
  }

  fail(message: string): void {
    this.#failures.push(message);
    this.trace.emit('sim', 0, 'failure', {message});
  }

  reservationsLost(server: Incarnation): void {
    for (const vs of this.viewSyncers) {
      vs.noteServerLost(server);
    }
  }

  rmReplicaFile(server: Incarnation): string | undefined {
    return this.rm.replicaFileOf(server);
  }

  /**
   * A replication-manager takes a reservation back without telling its
   * follower, whose `/snapshot` stream just ends. Only its log line says why.
   */
  #observeTakeBacks(event: TraceEvent): void {
    if (event.node !== 'rm' || event.kind !== 'log.warn') {
      return;
    }
    const [message, detail] = (event.data as {args: unknown[]}).args;
    if (typeof message !== 'string') {
      return;
    }
    const expired = RESERVATION_EXPIRED.exec(message);
    const taskIDs = expired
      ? [expired[1]]
      : message.includes('snapshot reservation(s): the change log was reseeded')
        ? (detail as {taskIDs: string[]}).taskIDs
        : [];
    const reason = expired ? 'expired' : 'invalidated';
    for (const taskID of taskIDs) {
      for (const vs of this.viewSyncers) {
        vs.noteReservationTakenBack(event.inc, taskID, reason);
      }
    }
  }

  watch(
    incarnation: Incarnation,
    name: string,
    running: Promise<unknown>,
    onStop?: () => void,
  ): void {
    running.then(
      () => {
        if (!incarnation.fenced) {
          if (onStop) {
            onStop();
          } else {
            this.fail(`${incarnation.name}'s ${name} stopped`);
          }
        }
      },
      (e: unknown) => {
        if (!incarnation.fenced) {
          this.fail(`${incarnation.name}'s ${name} failed: ${String(e)}`);
        }
      },
    );
  }

  async run(steps: readonly Step[]): Promise<RunResult> {
    const highWaterMark = getDefaultHighWaterMark(false);
    setDefaultHighWaterMark(false, this.config.highWaterMark);
    setActiveCensus(this.census);
    const uninstallProbes = this.#probes.install();
    this.clock.install();
    this.#random = new SeededMathRandom(this.config.seed);
    this.#guard?.enable();
    this.#resources = new ActiveResourceCheck();
    const signalListeners = RESERVATION_SIGNALS.map(
      signal => new Set(process.listeners(signal)),
    );
    try {
      await this.#scope.run(async () => {
        this.rm.startInitial();
        // Initial sync ends with the replica's first backup, which is what
        // view-syncers restore from.
        this.rm.takeBackup();
        for (const vs of this.viewSyncers) {
          vs.start();
        }
        await this.#check('start');
        for (const [i, step] of steps.entries()) {
          await this.#step(i, step);
        }
        await this.#heal();
      });
      return {
        hash: this.trace.hash,
        census: this.census,
        events: this.trace.events,
      };
    } catch (e) {
      throw new SimFailure(
        e instanceof Error ? e.message : String(e),
        this.trace.recent(),
      );
    } finally {
      this.rm.dispose();
      this.viewSyncers.forEach(vs => vs.dispose());
      // A fenced view-syncer's reservation loop never reaches its `finally`.
      for (const [i, signal] of RESERVATION_SIGNALS.entries()) {
        for (const listener of process.listeners(signal)) {
          if (!signalListeners[i].has(listener)) {
            process.off(signal, listener);
          }
        }
      }
      this.#guard?.disable();
      this.#random[Symbol.dispose]();
      this.clock.uninstall();
      uninstallProbes();
      setActiveCensus(undefined);
      setDefaultHighWaterMark(false, highWaterMark);
      if (this.#opts.traceFile) {
        this.trace.writeJSONLines(this.#opts.traceFile);
      }
      rmSync(this.runDir, {recursive: true, force: true});
    }
  }

  async #step(i: number, step: Step): Promise<void> {
    this.#random?.reseed(stepSeed(this.config.seed, step.id));
    const skipped = this.config.disabledGroups.includes(groupOf(step));
    this.trace.emit('sim', 0, 'step', {i, skipped, step});
    if (!skipped) {
      this.#execute(step);
    }
    const ms = step.kind === 'advance' && !skipped ? step.ms : step.dt;
    await this.clock.advance(Math.max(1, ms));
    this.rm.endOverlap(this.clock.elapsed());
    this.rm.replaceLost();
    await this.#check(`step ${i} (${step.kind})`);
  }

  #execute(step: Step): void {
    switch (step.kind) {
      case 'commit': {
        const tx = this.pg.begin();
        for (const op of step.ops) {
          applyWorkloadOp(tx, op);
        }
        this.pg.commit(tx, step.gap);
        break;
      }
      case 'idle':
        this.pg.keepalive();
        break;
      case 'deliver':
        this.pg.deliver(step.n);
        break;
      case 'sourceDisconnect':
        this.pg.disconnect(step.partial);
        break;
      case 'backfillFault':
        this.pg.failNextBackfill(step.fault);
        break;
      case 'slotTakeover': {
        const latest = this.backup.latest;
        if (latest) {
          this.rm.takeOver(latest, step.overlap);
        }
        break;
      }
      case 'rmCrash':
        this.rm.crash(step.shm);
        break;
      case 'rmCrashAt': {
        const armed = this.rm.live?.incarnation;
        if (armed) {
          this.#probes.arm(armed, step.point, step.after, () => {
            if (this.rm.live?.incarnation === armed) {
              this.rm.crash(true);
            }
          });
        }
        break;
      }
      case 'rmRestart':
        if (!this.rm.isDown) {
          this.rm.crash(true);
        }
        this.rm.restart();
        break;
      case 'rmReplace': {
        const latest = this.backup.latest;
        if (latest) {
          this.rm.replace(latest);
        }
        break;
      }
      case 'deleteChangeLog':
        this.rm.deleteChangeLog();
        break;
      case 'backupTake':
        this.rm.takeBackup();
        break;
      case 'backupStall':
        this.backup.stall();
        break;
      case 'backupResume':
        this.backup.resume();
        break;
      case 'vsPull':
        this.#vs(step.vs)?.pull(step.n);
        break;
      case 'vsPause':
        this.#vs(step.vs)?.pause();
        break;
      case 'vsResume':
        this.#vs(step.vs)?.resume();
        break;
      case 'vsDisconnect':
        this.#vs(step.vs)?.disconnect(
          step.error ? new Error('simulated disconnect') : undefined,
        );
        break;
      case 'vsRestart':
        this.#vs(step.vs)?.restart();
        break;
      case 'vsWipe':
        this.#vs(step.vs)?.wipe();
        break;
      case 'vsHoldReservation':
        this.#vs(step.vs)?.holdReservation();
        break;
      case 'advance':
        break;
      case 'pumpPurge':
        this.#pumpPurge(step.batches);
        break;
    }
  }

  /**
   * The spec's fairness: no new faults, crashed nodes restart, paused ones
   * pull, commits keep arriving and backups are taken, and time moves in
   * bounded rounds. Wedged restores stay wedged: they are what the lease is
   * for. View-syncers that stopped restart themselves.
   *
   * Liveness is checked twice. While commits still arrive, every view-syncer
   * that isn't wedged must be at the head, which a restore that cannot land
   * while the log moves (one that outlasts its lease, §5 row 4) never is. Once
   * quiet, the log must also have drained and no reservation outlived its cap.
   */
  async #heal(): Promise<void> {
    this.trace.emit('sim', 0, 'heal');
    this.#probes.disarmAll();
    this.rm.endOverlap();
    this.backup.resume();
    this.#gatedPurge = false;
    this.#pumpPurge(this.#parkedYields.length);
    this.viewSyncers.forEach(vs => vs.resume());
    for (const vs of this.viewSyncers) {
      if (vs.wedged) {
        this.census.note('restore:wedged-through-heal');
      }
    }

    let round = 0;
    for (; round < HEAL_BUSY_ROUNDS; round++) {
      await this.#healRound(round, round % 3 === 0);
    }
    this.#checkLiveness('while commits arrive', false);

    // Long enough for a reservation taken just before heal to expire, and
    // for the log it held to drain after that.
    const quietRounds = Math.ceil(
      (Math.max(this.config.retentionMs, this.config.reservationMaxAgeMs) +
        3 * CLEANUP_DELAY_MS) /
        HEAL_ROUND_MS,
    );
    for (const end = round + quietRounds; round < end; round++) {
      await this.#healRound(round, false);
    }
    this.#checkLiveness('after heal', true);
  }

  async #healRound(round: number, commit: boolean): Promise<void> {
    this.#random?.reseed(stepSeed(this.config.seed, -1 - round));
    // Also a crash already in progress when heal began, which can land after.
    if (this.rm.isDown) {
      this.rm.restart();
    }
    this.rm.replaceLost();
    if (commit) {
      this.#trickle(round);
    }
    this.pg.deliver(Number.MAX_SAFE_INTEGER);
    if (round % 2 === 1) {
      this.rm.takeBackup();
    }
    await this.clock.advance(HEAL_ROUND_MS);
    await this.#check(`heal round ${round}`);
  }

  #checkLiveness(when: string, quiet: boolean): void {
    const head = this.pg.head.watermark;
    const problems: string[] = [];
    const rmVersion = this.rm.stateVersion();
    if (rmVersion !== head) {
      problems.push(`the backup replicator is at ${rmVersion}, not ${head}`);
    }
    for (const vs of this.viewSyncers) {
      if (vs.wedged) {
        continue;
      }
      // Prop_RestoreCompletes. A lease taken back is no exemption.
      const version = vs.stateVersion();
      if (version === undefined) {
        problems.push(
          `${vs.name} has not finished restoring` +
            (vs.stoppedBy ? ` (stopped by ${vs.stoppedBy})` : ''),
        );
      } else if (version !== head) {
        problems.push(`${vs.name} is at ${version}, not ${head}`);
      }
    }
    if (quiet) {
      // Prop_NoPermanentPin, wedged restores included.
      const now = this.clock.elapsed();
      const cap = this.config.reservationMaxAgeMs;
      for (const conn of this.network.connections()) {
        if (
          conn.kind === 'snapshot' &&
          now - conn.openedAt > cap + HEAL_ROUND_MS
        ) {
          problems.push(
            `${conn.client.name}'s reservation has been open for ` +
              `${now - conn.openedAt} ms, past its ${cap} ms cap`,
          );
        }
      }
      const bounds = this.#logBounds();
      if (bounds && (bounds.min !== head || bounds.head !== head)) {
        problems.push(
          `the change log spans ${bounds.min}..${bounds.head}; ` +
            `it should have drained to ${head}`,
        );
      }
      // Every backfill completed, and every replica equals upstream.
      for (const [name, replicaFile] of this.#replicas()) {
        problems.push(...this.#unsettled(name, replicaFile));
      }
    }
    if (problems.length) {
      throw new Error(`liveness ${when}:\n${problems.join('\n')}`);
    }
  }

  /**
   * The replicas that must settle: the backup replicator's, and each one of a
   * view-syncer that is replicating and not wedged.
   */
  #replicas(): [name: string, replicaFile: string][] {
    const replicas: [string, string][] = [];
    const rm = this.rm.live;
    if (rm) {
      replicas.push([rm.incarnation.name, rm.files.replicaFile]);
    }
    for (const vs of this.viewSyncers) {
      const life = vs.live;
      if (life?.phase === 'replicating' && !life.wedged) {
        replicas.push([life.incarnation.name, life.replicaFile]);
      }
    }
    return replicas;
  }

  #unsettled(name: string, replicaFile: string): string[] {
    const db = new Database(this.lc, replicaFile, {readonly: true});
    try {
      return [
        ...Array.from(
          backfillingColumns(db),
          ([table, columns]) =>
            `${name}: ${table} is still backfilling [${[...columns]}]`,
        ),
        ...diffReplica(db, this.pg.head.state).map(diff => `${name}: ${diff}`),
      ];
    } finally {
      db.close();
    }
  }

  /** A low rate of commits during heal, on whatever table exists. */
  #trickle(round: number): void {
    const tx = this.pg.begin();
    const table = tx.tableAt(round) ?? tx.createTable(['text']);
    const row = Object.fromEntries(
      table.columns.map(c => [c.name, c.name === 'id' ? 1 : null]),
    );
    if (!tx.update(table.name, {id: 1}, row)) {
      tx.insert(table.name, row);
    }
    this.pg.commit(tx, 1);
  }

  async #check(at: string): Promise<void> {
    await this.clock.settle();
    await this.clock.settle();
    this.rm.checkOracles();
    for (const vs of this.viewSyncers) {
      vs.checkOracles();
    }
    const problems = [
      ...this.#failures.splice(0),
      ...this.census.takeAlarms().map(m => `alarm: ${m}`),
      ...this.pg.takeViolations().map(m => `SimPG: ${m}`),
      ...this.oracles.take().map(v => `oracle ${v.oracle}: ${v.message}`),
      ...(this.#guard
        ?.takeViolations()
        .map(v => `guard: ${v.type} created at\n${v.stack}`) ?? []),
    ];
    const excess = this.#resources?.excess() ?? [];
    if (excess.length) {
      problems.push(`real resources are active: ${excess.join(', ')}`);
    }
    if (problems.length) {
      throw new Error(`${at}:\n${problems.join('\n')}`);
    }
  }

  #pumpPurge(batches: number): void {
    for (const resume of this.#parkedYields.splice(0, batches)) {
      resume();
    }
  }

  #vs(index: number): VSNode | undefined {
    return this.viewSyncers[index % this.viewSyncers.length];
  }

  #nodeOf(conn: Connection): VSNode | undefined {
    return this.viewSyncers.find(vs => vs.live?.incarnation === conn.client);
  }

  #logBounds(): {min: string; head: string} | undefined {
    const replicaFile = this.rm.live?.files.replicaFile;
    if (!replicaFile || !existsSync(changeLogFileName(replicaFile))) {
      return undefined;
    }
    const db = new Database(this.lc, changeLogFileName(replicaFile), {
      readonly: true,
    });
    try {
      return db
        .prepare(/*sql*/ `SELECT min("watermark") AS "min", max("watermark") AS "head"
            FROM "_zero.changeLogStream"`)
        .get<{min: string; head: string}>();
    } finally {
      db.close();
    }
  }
}
