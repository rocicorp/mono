import {existsSync, mkdirSync} from 'node:fs';
import {join} from 'node:path';
import {LogContext} from '@rocicorp/logger';
import {sleep} from '../../../../shared/src/sleep.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import {deleteLiteDB} from '../../db/delete-lite-db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {
  reserveAndGetSnapshotStatus,
  restoreUnderReservation,
  type SnapshotMessage,
  type SnapshotStatus,
} from '../../services/change-streamer/snapshot.ts';
import {
  replicaStateIsValid,
  type RestoreResult,
} from '../../services/litestream/commands.ts';
import {deleteChangeLogDB} from '../../services/replicator/change-log-db.ts';
import {ReplicatorService} from '../../services/replicator/replicator.ts';
import {getSubscriptionState} from '../../services/replicator/schema/replication-state.ts';
import {getPragmaConfig} from '../../workers/replicator.ts';
import type {SimContext} from './context.ts';
import {inThreadWriteWorker} from './in-thread-worker.ts';
import {Incarnation} from './incarnation.ts';
import {replicaStateVersion} from './replica.ts';
import {copySQLiteFiles, SIM_LOG_CONFIG} from './rm-node.ts';
import type {Connection, SimChangeStreamerClient} from './transport.ts';

/** `reserveAndGetSnapshotStatus` hands its config only to `reserve`. */
const NO_CONFIG = {} as NormalizedZeroConfig;

type Reservation = {
  /** Where it was confirmed: the `minWatermark` of its status. */
  readonly watermark: string;
  /** The replication-manager incarnation that confirmed it. */
  readonly server: Incarnation;
  /** Why it was taken back, if it was. */
  takenBack: string | undefined;
};

type VSLife = {
  readonly incarnation: Incarnation;
  readonly lc: LogContext;
  readonly replicaFile: string;
  readonly taskID: string;
  phase: 'restoring' | 'replicating';
  /** A restore held at its download, which never finishes. */
  wedged: boolean;
  /** The last reservation confirmed to this life, until another is opened. */
  reservation: Reservation | undefined;
  /** The subscription that consumed it, which is the one it protects. */
  consumedBy: Connection | undefined;
};

/**
 * A view-syncer, down to its replica.
 *
 * Every incarnation starts the way `runWorker` does: the real restore loop
 * (`restoreUnderReservation`) reserves a snapshot through the simulated
 * network, waits out a download in virtual time, copies SimBackup's newest
 * backup unless the replica survived, and runs the real validity check. Then a
 * serving replicator subscribes. IVM and the sync protocol are not in the loop.
 *
 * A replicator that stops on a terminal error is restarted after a delay, as an
 * orchestrator would restart the task, and the restart restores again.
 */
export class VSNode {
  readonly index: number;
  readonly name: string;
  readonly #sim: SimContext;
  #life: VSLife | undefined;
  #incarnations = 0;
  #tasks = 0;
  #paused = false;
  #holdNextRestore = false;
  #stoppedBy: string | undefined;

  constructor(sim: SimContext, index: number) {
    this.#sim = sim;
    this.index = index;
    this.name = `vs-${index}`;
  }

  get live(): VSLife | undefined {
    return this.#life;
  }

  /** The terminal error the replicator stopped with, while it awaits restart. */
  get stoppedBy(): string | undefined {
    return this.#stoppedBy;
  }

  get wedged(): boolean {
    return this.#life?.wedged ?? false;
  }

  get paused(): boolean {
    return this.#paused;
  }

  /** A new task on an empty volume. */
  start(): void {
    this.#start(this.#newReplicaFile(), this.#newTaskID());
  }

  /**
   * The process restarts in the same task (C1): its volume, `-shm` included,
   * survives.
   */
  restart(): void {
    const life = this.#life;
    const replicaFile = this.#newReplicaFile();
    if (life) {
      copySQLiteFiles(life.replicaFile, replicaFile, true);
      this.#sim.trace.emit(this.name, life.incarnation.number, 'vs.restart');
      life.incarnation.fence();
    }
    this.#start(replicaFile, life?.taskID ?? this.#newTaskID());
  }

  /** The task is replaced by a new one on an empty volume (C3). */
  wipe(): void {
    const life = this.#life;
    if (life) {
      this.#sim.trace.emit(this.name, life.incarnation.number, 'vs.wipe');
      life.incarnation.fence();
    }
    this.#start(this.#newReplicaFile(), this.#newTaskID());
  }

  /**
   * Wedges a restore at its download: it holds its reservation and its
   * socket, and never finishes. This is what the reservation lease exists for.
   * A view-syncer past its restore is restarted into one.
   */
  holdReservation(): void {
    this.#holdNextRestore = true;
    if (this.#life?.phase !== 'restoring') {
      this.restart();
    }
  }

  /** The subscription ended with a terminal error, which stops the replicator. */
  noteError(type: string): void {
    this.#stoppedBy ??= type;
  }

  /** Replication-manager incarnation `server` took back `taskID`'s reservation. */
  noteReservationTakenBack(
    server: number,
    taskID: string,
    reason: string,
  ): void {
    const life = this.#life;
    const reservation = life?.reservation;
    if (
      life?.taskID !== taskID ||
      !reservation ||
      reservation.takenBack ||
      reservation.server.number !== server
    ) {
      return;
    }
    this.#takeBack(life, reservation, reason);
  }

  /** Every reservation confirmed by `server` is gone with it. */
  noteServerLost(server: Incarnation): void {
    const life = this.#life;
    const reservation = life?.reservation;
    if (life && reservation?.server === server && !reservation.takenBack) {
      this.#takeBack(life, reservation, 'rm-crash');
    }
  }

  /**
   * If `conn` is the subscription of a replica restored under a reservation
   * that nobody took back, which oracle 5 protects, that reservation's
   * watermark.
   */
  protectedBy(conn: Connection): string | undefined {
    const life = this.#life;
    const reservation = life?.reservation;
    return life?.consumedBy === conn &&
      reservation?.takenBack === undefined &&
      reservation?.server === conn.server.incarnation
      ? reservation.watermark
      : undefined;
  }

  /**
   * Oracles 7 and 8 on a replica that is replicating, and oracle 4 for a
   * reservation that is confirmed and not yet consumed.
   */
  checkOracles(): void {
    const life = this.#life;
    if (!life) {
      return;
    }
    const {census, oracles} = this.#sim;
    if (life.phase === 'replicating') {
      oracles.checkReplica(life.incarnation.name, life.replicaFile, () =>
        census.note('backfill:phantom-row'),
      );
    }
    const {reservation} = life;
    if (!reservation || reservation.takenBack || life.consumedBy) {
      return;
    }
    // Against the log of the incarnation that confirmed it, while it runs.
    const rmReplicaFile = this.#sim.rmReplicaFile(reservation.server);
    if (rmReplicaFile) {
      oracles.checkReservation(
        rmReplicaFile,
        life.incarnation.name,
        reservation.watermark,
      );
    }
  }

  connections(kind: Connection['kind'] = 'changes'): Connection[] {
    const life = this.#life;
    return life
      ? this.#sim.network
          .connectionsOf(life.incarnation)
          .filter(c => c.kind === kind)
      : [];
  }

  /** Called for each connection this view-syncer opens. */
  opened(conn: Connection): void {
    const life = this.#life;
    if (life?.incarnation !== conn.client) {
      return;
    }
    if (conn.kind === 'snapshot') {
      // A reservation for the same task supersedes the last one.
      life.reservation = undefined;
      return;
    }
    if (life.phase === 'replicating' && life.consumedBy === undefined) {
      // The first subscription closes the reservation, whatever its route.
      life.consumedBy = conn;
    }
    if (this.#paused) {
      conn.pause();
    }
  }

  /** A reservation's status reached this view-syncer over `conn`. */
  reserved(conn: Connection, [, status]: SnapshotMessage): void {
    const life = this.#life;
    if (life?.incarnation !== conn.client) {
      return;
    }
    life.reservation = {
      watermark: status.minWatermark,
      server: conn.server.incarnation,
      takenBack: undefined,
    };
    this.#sim.census.note('reservation:confirmed');
    this.#sim.trace.emit(this.name, life.incarnation.number, 'vs.reserved', {
      watermark: status.minWatermark,
      server: conn.server.incarnation.name,
    });
  }

  pause(): void {
    this.#paused = true;
    this.connections().forEach(c => c.pause());
  }

  pull(n: number): void {
    if (this.#paused) {
      this.connections().forEach(c => c.pull(n));
    }
  }

  resume(): void {
    this.#paused = false;
    this.connections().forEach(c => c.resume());
  }

  disconnect(err: Error | undefined): void {
    this.connections().forEach(c => c.disconnect(err));
  }

  /** The replica's state version, once a restore has materialized it. */
  stateVersion(): string | undefined {
    const life = this.#life;
    if (
      !life ||
      life.phase !== 'replicating' ||
      !existsSync(life.replicaFile)
    ) {
      return undefined;
    }
    const db = new Database(this.#sim.lc, life.replicaFile, {readonly: true});
    try {
      return replicaStateVersion(db);
    } finally {
      db.close();
    }
  }

  dispose(): void {
    this.#life?.incarnation.fence();
    this.#life = undefined;
  }

  #start(replicaFile: string, taskID: string): void {
    const {trace, network} = this.#sim;
    const n = ++this.#incarnations;
    const incarnation = new Incarnation(this.name, n);
    const lc = new LogContext('debug', {}, trace.sink(this.name, n));
    const life: VSLife = {
      incarnation,
      lc,
      replicaFile,
      taskID,
      phase: 'restoring',
      wedged: false,
      reservation: undefined,
      consumedBy: undefined,
    };
    this.#life = life;
    this.#stoppedBy = undefined;
    trace.emit(this.name, n, 'vs.start', {
      taskID,
      replica: existsSync(replicaFile) ? 'kept' : 'empty',
    });

    incarnation.run(() => {
      const client = network.clientFor(incarnation, () =>
        this.#sim.subscribeOrder(),
      );
      this.#sim.watch(
        incarnation,
        'view-syncer',
        (async () => {
          await this.#restore(life, client);
          await this.#replicate(life, client);
        })(),
        () => this.#stopped(life),
      );
    });
  }

  #restore(
    life: VSLife,
    client: SimChangeStreamerClient,
  ): Promise<RestoreResult> {
    const {lc, replicaFile, taskID, incarnation} = life;
    const {backup, census, config, trace} = this.#sim;
    return restoreUnderReservation<RestoreResult>(
      lc,
      () =>
        reserveAndGetSnapshotStatus(lc, NO_CONFIG, () =>
          client.reserveSnapshot(taskID),
        ),
      async (status: SnapshotStatus) => {
        if (this.#holdNextRestore) {
          this.#holdNextRestore = false;
          life.wedged = true;
          census.note('restore:wedged');
          trace.emit(this.name, incarnation.number, 'vs.wedged');
          await new Promise<never>(() => {});
        }
        // `litestream restore -if-db-not-exists`: a surviving replica is kept.
        // Otherwise the download is of the newest backup when it starts.
        const existed = existsSync(replicaFile);
        const download = existed ? undefined : backup.latest;
        if (!existed && !download) {
          census.note('restore:no-backup');
          return {restored: false, result: 'no_backup'};
        }
        if (download) {
          await sleep(config.restoreDurationMs);
          backup.restore(download, replicaFile);
          if (download.backfilling > 0) {
            census.note('restore:mid-run');
          }
        }
        if (!replicaStateIsValid(lc, readState(lc, replicaFile), status)) {
          census.note('restore:invalid');
          deleteLiteDB(replicaFile);
          deleteChangeLogDB(replicaFile);
          return {restored: false, result: 'invalid_replica'};
        }
        if (!existed) {
          deleteChangeLogDB(replicaFile);
        }
        census.note(existed ? 'restore:kept' : 'restore:downloaded');
        return {restored: true, result: 'success'};
      },
    );
  }

  async #replicate(
    life: VSLife,
    client: SimChangeStreamerClient,
  ): Promise<void> {
    const {lc, replicaFile, taskID} = life;
    life.phase = 'replicating';
    const db = new Database(lc, replicaFile);
    try {
      db.pragma('journal_mode = wal2');
    } finally {
      db.close();
    }
    const worker = inThreadWriteWorker({
      createLogContext: () => lc,
      createLitestreamClient: () => {
        throw new Error('a serving replicator has no checkpointer');
      },
    });
    await worker.init(
      replicaFile,
      'serving',
      getPragmaConfig('serving'),
      SIM_LOG_CONFIG,
      null,
    );
    await new ReplicatorService(
      lc,
      taskID,
      `${this.name}-replicator`,
      'serving',
      client,
      worker,
      null,
    ).run();
  }

  #stopped(life: VSLife): void {
    if (this.#stoppedBy === undefined) {
      this.#sim.fail(
        `${life.incarnation.name}'s replicator stopped without an error`,
      );
      return;
    }
    this.#sim.census.note(`vs:stopped/${this.#stoppedBy}`);
    setTimeout(() => {
      if (this.#life === life) {
        this.restart();
      }
    }, this.#sim.config.vsRestartDelayMs);
  }

  #takeBack(life: VSLife, reservation: Reservation, reason: string): void {
    reservation.takenBack = reason;
    this.#sim.census.note(`reservation:taken-back/${reason}`);
    if (life.phase === 'restoring' && !life.wedged) {
      this.#sim.census.note('reservation:taken-back-mid-restore');
    }
    this.#sim.trace.emit(
      this.name,
      life.incarnation.number,
      'vs.reservation-taken-back',
      {reason},
    );
  }

  #newTaskID(): string {
    return `${this.name}-task-${++this.#tasks}`;
  }

  #newReplicaFile(): string {
    const dir = join(
      this.#sim.runDir,
      `${this.name}-${this.#incarnations + 1}`,
    );
    mkdirSync(dir, {recursive: true});
    return join(dir, 'replica.db');
  }
}

function readState(lc: LogContext, replicaFile: string) {
  try {
    const db = new Database(lc, replicaFile, {readonly: true});
    try {
      return getSubscriptionState(new StatementRunner(db));
    } finally {
      db.close();
    }
  } catch {
    // An unreadable replica is invalid, as `replicaIsValid` finds it.
    return {replicaVersion: '', watermark: '', publications: []};
  }
}
