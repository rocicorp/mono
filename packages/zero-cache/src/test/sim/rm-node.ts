import {copyFileSync, existsSync, mkdirSync} from 'node:fs';
import {join} from 'node:path';
import {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../zqlite/src/db.ts';
import {
  createChangeStreamer,
  type ChangeStreamerRegistry,
} from '../../services/change-streamer/change-streamer-service.ts';
import type {ChangeStreamerService} from '../../services/change-streamer/change-streamer.ts';
import {
  changeLogFileName,
  deleteChangeLogDB,
} from '../../services/replicator/change-log-db.ts';
import {ReplicationStatusPublisher} from '../../services/replicator/replication-status.ts';
import {ReplicatorService} from '../../services/replicator/replicator.ts';
import {initReplicationState} from '../../services/replicator/schema/replication-state.ts';
import {getPragmaConfig} from '../../workers/replicator.ts';
import type {SimContext} from './context.ts';
import {inThreadWriteWorker} from './in-thread-worker.ts';
import {Incarnation} from './incarnation.ts';
import {replicaStateVersion} from './replica.ts';
import type {SimBackupRecord} from './sim-backup.ts';
import {SimChangeSource} from './sim-change-source.ts';

export const SIM_SHARD = {appID: 'sim', shardNum: 0};
export const SIM_REPLICA_ID = 'sim-replica';
export const SIM_BACKUP_URL = 'sim://backup';
export const SIM_LOG_CONFIG = {level: 'debug', format: 'text'} as const;

const RM_TASK_ID = 'rm-task';

type RMFiles = {
  readonly dir: string;
  readonly replicaFile: string;
};

export type RMLife = {
  readonly incarnation: Incarnation;
  readonly lc: LogContext;
  readonly files: RMFiles;
  readonly source: SimChangeSource;
  readonly service: ChangeStreamerService;
  /** The latest watermark given to `trackBackupWatermark`. */
  backupWatermark: string | undefined;
};

/** A replaced incarnation that still runs beside the live one. */
type Overlapped = {
  readonly life: RMLife;
  /**
   * When its orchestrator stops it: at a virtual time, or as the live
   * incarnation takes the slot.
   */
  readonly until: number | 'slot';
};

/**
 * The replication-manager: a change-streamer with the PG change log off, fed
 * by SimPG through the real backfill manager and multiplexer, and its backup
 * replicator, whose replica SimBackup backs up.
 *
 * Each incarnation runs on its own directory. A crash copies the replica and
 * the change log, sidecars included, into the next directory before fencing
 * the incarnation, so nothing the dying code does (a rollback, a fail-soft
 * delete) can reach the files the next one opens.
 *
 * A slot takeover is the one replacement that is not fenced at once: the
 * replaced incarnation keeps running beside the new one until it reads that it
 * lost the slot, which shuts its process down, or its orchestrator stops it.
 */
export class RMNode {
  readonly #sim: SimContext;
  #life: RMLife | undefined;
  #overlapped: Overlapped | undefined;
  #down: RMFiles | undefined;
  /** The live incarnation lost its slot and exited, and awaits replacement. */
  #lost = false;
  #incarnations = 0;
  #dirs = 0;

  constructor(sim: SimContext) {
    this.#sim = sim;
  }

  get live(): RMLife | undefined {
    return this.#life;
  }

  get isDown(): boolean {
    return this.#down !== undefined;
  }

  /** The first incarnation, on a replica fresh from initial sync. */
  startInitial(): void {
    const files = this.#newFiles();
    const db = new Database(this.#sim.lc, files.replicaFile);
    try {
      db.pragma('journal_mode = wal');
      initReplicationState(db, ['zero_data'], this.#sim.pg.replicaVersion);
    } finally {
      db.close();
    }
    this.#start(files);
  }

  /**
   * Kills the live incarnation, leaving its files for {@link restart}. A
   * process crash leaves `-shm` on the volume; a lost volume does not.
   */
  crash(shm: boolean): boolean {
    const life = this.#life;
    if (!life) {
      return false;
    }
    const holder = this.#sim.pg.holder;
    if (holder?.incarnation === life.incarnation && holder.queued > 0) {
      // C8: killed with a burst of upstream transactions still arriving.
      this.#sim.census.note('rm:crash/mid-burst');
    }
    const next = this.#newFiles();
    copySQLiteFiles(life.files.replicaFile, next.replicaFile, shm);
    copySQLiteFiles(
      changeLogFileName(life.files.replicaFile),
      changeLogFileName(next.replicaFile),
      shm,
    );
    this.#sim.trace.emit('rm', life.incarnation.number, 'rm.crash', {shm});
    life.incarnation.fence();
    this.#life = undefined;
    this.#down = next;
    return true;
  }

  /** Starts the next incarnation on what the last crash left. */
  restart(): boolean {
    const files = this.#down;
    if (!files) {
      return false;
    }
    this.#start(files);
    return true;
  }

  /**
   * Replaces the replication-manager with a new task restored from `record`,
   * which has no change log (C14).
   */
  replace(record: SimBackupRecord): void {
    const life = this.#life;
    if (life) {
      this.#sim.trace.emit('rm', life.incarnation.number, 'rm.replaced');
      life.incarnation.fence();
      this.#life = undefined;
    }
    this.#startFromBackup(record);
  }

  /**
   * A new task restored from `record` takes over from the live one, which is
   * not stopped first. Unless `overlap`, the old task dies as the new one takes
   * the slot. Otherwise it keeps serving the subscribers it has until it reads
   * that it lost the slot, which shuts it down (see {@link #streamerStopped}),
   * or until its orchestrator stops it `overlapMs` later. If it was not reading
   * when the slot was taken, its next attempt takes the slot back, and the new
   * task is the one that exits.
   */
  takeOver(record: SimBackupRecord, overlap: boolean): void {
    const life = this.#life;
    if (!life) {
      this.replace(record);
      return;
    }
    this.endOverlap();
    this.#sim.census.note(`rm:takeover/${overlap ? 'overlap' : 'fenced'}`);
    this.#sim.trace.emit('rm', life.incarnation.number, 'rm.taken-over', {
      overlap,
    });
    this.#overlapped = {
      life,
      until: overlap
        ? this.#sim.clock.elapsed() + this.#sim.config.overlapMs
        : 'slot',
    };
    this.#life = undefined;
    this.#startFromBackup(record);
  }

  /**
   * Stops a replaced incarnation whose overlap has run its course by `now`,
   * or any, without `now`.
   */
  endOverlap(now = Number.POSITIVE_INFINITY): void {
    const overlapped = this.#overlapped;
    if (
      !overlapped ||
      (overlapped.until !== 'slot' && overlapped.until > now)
    ) {
      return;
    }
    this.#overlapped = undefined;
    this.#sim.trace.emit(
      'rm',
      overlapped.life.incarnation.number,
      'rm.overlap-ended',
    );
    overlapped.life.incarnation.fence();
  }

  /**
   * The orchestrator replaces a live task that exited because another took its
   * slot, from the latest backup.
   */
  replaceLost(): void {
    const latest = this.#sim.backup.latest;
    if (this.#lost && !this.#life && latest) {
      this.#startFromBackup(latest);
    }
  }

  /**
   * C6: the change log is deleted and the replication-manager restarts, which
   * reseeds it. A log deleted under a live writer would stay deleted until a
   * restart anyway: the writer keeps its handle to the unlinked file.
   */
  deleteChangeLog(): boolean {
    if (this.#life) {
      this.crash(true);
    }
    const files = this.#down;
    if (!files) {
      return false;
    }
    deleteChangeLogDB(files.replicaFile);
    this.#sim.trace.emit('rm', this.#incarnations, 'rm.log-deleted');
    return this.restart();
  }

  /** Backs up the live replica and confirms the backup's watermark. */
  takeBackup(): SimBackupRecord | undefined {
    const life = this.#life;
    if (!life) {
      return undefined;
    }
    const record = this.#sim.backup.take(this.#sim.lc, life.files.replicaFile);
    this.confirmBackup(record.watermark);
    return record;
  }

  /**
   * What the backup monitor does with a watermark it polls: ignores one that
   * is not newer, and gives the rest to `trackBackupWatermark`.
   */
  confirmBackup(watermark: string): void {
    const life = this.#life;
    if (
      !life ||
      (life.backupWatermark !== undefined && watermark <= life.backupWatermark)
    ) {
      return;
    }
    life.backupWatermark = watermark;
    this.#sim.trace.emit('rm', life.incarnation.number, 'rm.backup', {
      watermark,
    });
    life.incarnation.run(() => life.service.trackBackupWatermark(watermark));
  }

  /**
   * Oracles 2, 3, and 6 on the change log of each running incarnation, and 7
   * and 8 on its replica.
   */
  checkOracles(): void {
    const {census, oracles} = this.#sim;
    for (const life of [this.#life, this.#overlapped?.life]) {
      if (life) {
        oracles.checkChangeLog(
          life.files.replicaFile,
          SIM_REPLICA_ID,
          life.backupWatermark,
        );
        oracles.checkReplica(
          life.incarnation.name,
          life.files.replicaFile,
          () => census.note('backfill:phantom-row'),
        );
      }
    }
  }

  /** The replica file of `server`, while that incarnation runs. */
  replicaFileOf(server: Incarnation): string | undefined {
    for (const life of [this.#life, this.#overlapped?.life]) {
      if (life?.incarnation === server && !server.fenced) {
        return life.files.replicaFile;
      }
    }
    return undefined;
  }

  /** The live replica's state version. */
  stateVersion(): string | undefined {
    const life = this.#life;
    if (!life) {
      return undefined;
    }
    const db = new Database(this.#sim.lc, life.files.replicaFile, {
      readonly: true,
    });
    try {
      return replicaStateVersion(db);
    } finally {
      db.close();
    }
  }

  dispose(): void {
    this.#overlapped?.life.incarnation.fence();
    this.#overlapped = undefined;
    this.#life?.incarnation.fence();
    this.#life = undefined;
  }

  #startFromBackup(record: SimBackupRecord): void {
    if (record.backfilling > 0) {
      this.#sim.census.note('rm:replaced-mid-run');
    }
    const files = this.#newFiles();
    this.#sim.backup.restore(record, files.replicaFile);
    const db = new Database(this.#sim.lc, files.replicaFile);
    try {
      db.pragma('journal_mode = wal');
    } finally {
      db.close();
    }
    this.#start(files);
  }

  /**
   * A change-streamer stops for good only on a shutdown signal, which losing
   * its slot to a takeover sends, and then its process exits.
   */
  #streamerStopped(life: RMLife): void {
    const {incarnation} = life;
    if (!life.source.takenOver) {
      this.#sim.fail(`${incarnation.name}'s change-streamer stopped`);
      return;
    }
    this.#sim.census.note('rm:exited/slot-taken');
    this.#sim.trace.emit('rm', incarnation.number, 'rm.exited');
    if (this.#overlapped?.life === life) {
      this.#overlapped = undefined;
    } else if (this.#life === life) {
      this.#life = undefined;
      this.#lost = true;
    }
    incarnation.fence();
  }

  /** A fenced takeover's old incarnation dies as the new one takes the slot. */
  #streamStarted(incarnation: Incarnation): void {
    if (
      this.#overlapped?.until === 'slot' &&
      this.#life?.incarnation === incarnation
    ) {
      this.endOverlap();
    }
  }

  #start(files: RMFiles): void {
    const {config, trace, pg, backup, network} = this.#sim;
    const n = ++this.#incarnations;
    const incarnation = new Incarnation('rm', n);
    const lc = new LogContext('debug', {}, trace.sink('rm', n));
    const registry: ChangeStreamerRegistry = {
      assumeOwnership: () => {
        trace.emit('rm', n, 'rm.owner');
        return Promise.resolve();
      },
      markResetRequired: () => {
        this.#sim.fail(`rm#${n} was told that a reset is required`);
        return Promise.resolve();
      },
    };
    const source = new SimChangeSource(lc, pg, {
      batchRows: config.batchRows,
      commitThresholdBytes: config.commitThresholdBytes,
      resume: config.resume,
      onStreamStart: () => this.#streamStarted(incarnation),
    });
    const service = incarnation.run(() =>
      createChangeStreamer(
        lc,
        registry,
        SIM_SHARD,
        pg.replicaVersion,
        source,
        silentStatusPublisher(),
        {backupURL: SIM_BACKUP_URL, litestreamVersion: 'v5'},
        false,
        {
          pgChangeLogEnabled: false,
          flowControlConsensusTimeoutProportion:
            config.flowControlConsensusTimeoutProportion,
          snapshotReservationMaxAgeMs: config.reservationMaxAgeMs,
          sqliteCatchup: {
            changeLogFile: changeLogFileName(files.replicaFile),
            readBatchRows: config.readBatchRows,
            barrierTimeoutMs: config.barrierTimeoutMs,
          },
          sqliteChangeLogWriter: {
            replicaFile: files.replicaFile,
            identity: {
              epoch: null,
              generation: pg.replicaVersion,
              replicaID: SIM_REPLICA_ID,
            },
          },
          sqliteChangeLogPurge: {
            retentionMs: config.retentionMs,
            batchRows: config.purgeBatchRows,
            yieldFn: () => this.#sim.purgeYield(),
          },
          sqliteChangeLogServe: {
            readPercent: 100,
            coldReadPercent: 100,
            retentionMs: config.retentionMs,
          },
        },
      ),
    );
    const life: RMLife = {
      incarnation,
      lc,
      files,
      source,
      service,
      backupWatermark: undefined,
    };
    this.#life = life;
    this.#down = undefined;
    this.#lost = false;
    // Its snapshot reservations die with the process.
    incarnation.onFence(() => this.#sim.reservationsLost(incarnation));
    const server = {incarnation, service};
    network.route(server);
    trace.emit('rm', n, 'rm.start', {dir: files.dir});

    incarnation.run(() => {
      this.#sim.watch(incarnation, 'change-streamer', service.run(), () =>
        this.#streamerStopped(life),
      );
      const worker = inThreadWriteWorker({
        createLogContext: () => lc,
        createLitestreamClient: (workerLC, replicaFile) =>
          backup.client(workerLC, replicaFile),
      });
      const replicator = new ReplicatorService(
        lc,
        RM_TASK_ID,
        'backup-replicator',
        'backup',
        network.clientFor(
          incarnation,
          () => this.#sim.subscribeOrder(),
          server,
        ),
        worker,
        null,
      );
      this.#sim.watch(
        incarnation,
        'backup-replicator',
        (async () => {
          await worker.init(
            files.replicaFile,
            'backup',
            getPragmaConfig('backup'),
            SIM_LOG_CONFIG,
            {
              checkpointThresholdPages: config.checkpointThresholdPages,
              maxWalPages: config.maxWalPages,
            },
          );
          await replicator.run();
        })(),
      );
    });

    // A new task's backup monitor finds the latest backup already there.
    const latest = backup.latest;
    if (latest) {
      this.confirmBackup(latest.watermark);
    }
  }

  #newFiles(): RMFiles {
    const dir = join(this.#sim.runDir, `rm-${++this.#dirs}`);
    mkdirSync(dir);
    return {dir, replicaFile: join(dir, 'replica.db')};
  }
}

const SQLITE_FILE_SUFFIXES = ['', '-wal', '-wal2'];

/** Copies a database and its sidecars, `-shm` only if asked. */
export function copySQLiteFiles(from: string, to: string, shm: boolean): void {
  for (const suffix of shm
    ? [...SQLITE_FILE_SUFFIXES, '-shm']
    : SQLITE_FILE_SUFFIXES) {
    if (existsSync(from + suffix)) {
      copyFileSync(from + suffix, to + suffix);
    }
  }
}

function silentStatusPublisher(): ReplicationStatusPublisher {
  return new ReplicationStatusPublisher(
    (lc, fn) => {
      const db = new Database(lc, ':memory:');
      try {
        return fn(db);
      } finally {
        db.close();
      }
    },
    () => Promise.resolve(),
  );
}
