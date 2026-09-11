import {existsSync} from 'node:fs';
import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../zqlite/src/db.ts';
import type {Downstream} from '../../services/change-streamer/change-streamer.ts';
import {
  seedCatchupStart,
  SQLiteChangeLogReader,
} from '../../services/change-streamer/sqlite-change-log-reader.ts';
import {
  CHANGE_LOG_META_TABLE,
  CHANGE_LOG_STREAM_TABLE,
  changeLogFileName,
  readChangeLogMeta,
} from '../../services/replicator/change-log-db.ts';
import {majorVersionOf} from '../../types/state-version.ts';
import {checkReplicaContent} from './replica.ts';
import type {SimPG} from './sim-pg.ts';
import type {Connection, NetworkObserver} from './transport.ts';

export type Violation = {
  readonly oracle: string;
  readonly message: string;
};

/**
 * The safety oracles that are checked after every step. None re-implements a
 * rule of the code under test: each knows only what is true upstream (SimPG)
 * and what the protocol promises.
 */
export class Oracles {
  readonly #pg: SimPG;
  readonly #lc: LogContext;
  readonly #violations: Violation[] = [];
  readonly #subscriptions = new WeakMap<Connection, SubscriptionOracle>();

  constructor(pg: SimPG, lc: LogContext) {
    this.#pg = pg;
    this.#lc = lc;
  }

  take(): Violation[] {
    return this.#violations.splice(0);
  }

  /** Records a violation that an oracle outside this class observed. */
  report(oracle: string, message: string): void {
    this.#violation(oracle, message);
  }

  #violation(oracle: string, message: string): void {
    this.#violations.push({oracle, message});
  }

  /**
   * Oracle 4: a confirmed reservation at `watermark` can still catch up, the
   * way its follower will ask. A follower restored to a minor `M.j`
   * subscribes at `M`, where the log's seed may stand in (`seedCatchupStart`),
   * so the question is asked at the major. Asking at `M.j` would pass a log
   * that purged `M` but kept `M.j`, or one reseeded at `M.j`.
   */
  checkReservation(
    replicaFile: string,
    holder: string,
    watermark: string,
  ): void {
    const file = changeLogFileName(replicaFile);
    if (!existsSync(file)) {
      this.#violation(
        '4 (reservation covered)',
        `${holder}'s reservation at ${watermark} is held by a replication-manager with no change log`,
      );
      return;
    }
    let reader: SQLiteChangeLogReader;
    try {
      reader = new SQLiteChangeLogReader(this.#lc, file);
    } catch {
      return;
    }
    try {
      const major = majorVersionOf(watermark);
      let plan = reader.plan(major);
      if (plan.kind === 'too-old') {
        const seed = seedCatchupStart(major, true, plan);
        if (seed !== undefined) {
          plan = reader.plan(seed);
        }
      }
      if (plan.kind === 'too-old') {
        this.#violation(
          '4 (reservation covered)',
          `${holder}'s reservation at ${watermark} can no longer catch up ` +
            `at ${major}: the log spans ${plan.minWatermark}..${plan.headWatermark} ` +
            `(seed ${plan.seedWatermark})`,
        );
      }
    } finally {
      reader.close();
    }
  }

  /**
   * Oracles 7 and 8 on the replica at `replicaFile`, read through its own
   * read-only connection.
   */
  checkReplica(
    holder: string,
    replicaFile: string,
    onPhantom?: () => void,
  ): void {
    if (!existsSync(replicaFile)) {
      return;
    }
    let db: Database;
    try {
      db = new Database(this.#lc, replicaFile, {readonly: true});
    } catch {
      // A replica whose `-shm` a crash left behind cannot be opened read-only
      // until its writer opens it.
      return;
    }
    try {
      for (const {oracle, message} of checkReplicaContent(
        db,
        this.#pg,
        onPhantom,
      )) {
        this.#violation(oracle, `${holder}'s replica: ${message}`);
      }
    } finally {
      db.close();
    }
  }

  /** Taps every subscription for oracle 1. */
  readonly network: NetworkObserver = {
    received: (conn, {data}) => {
      if (!conn.ctx) {
        return;
      }
      let oracle = this.#subscriptions.get(conn);
      if (!oracle) {
        oracle = new SubscriptionOracle(this.#pg, conn.ctx.watermark, message =>
          this.#violation(
            '1 (subscriber stream)',
            `${conn.client.name} via connection ${conn.id}: ${message}`,
          ),
        );
        this.#subscriptions.set(conn, oracle);
      }
      oracle.received(data);
    },
  };

  /**
   * Oracles 2, 3, and 6 on the change log beside `replicaFile`, read through
   * its own read-only connection.
   *
   * @param backupWatermark the latest watermark given to this incarnation's
   *     `trackBackupWatermark`, if any.
   */
  checkChangeLog(
    replicaFile: string,
    replicaID: string,
    backupWatermark: string | undefined,
  ): void {
    const file = changeLogFileName(replicaFile);
    if (!existsSync(file)) {
      return;
    }
    let db: Database;
    try {
      db = new Database(this.#lc, file, {readonly: true});
    } catch {
      // A log whose `-shm` a crash left behind cannot be opened read-only until
      // its writer opens it.
      return;
    }
    try {
      if (
        !tableExists(db, CHANGE_LOG_STREAM_TABLE) ||
        !tableExists(db, CHANGE_LOG_META_TABLE)
      ) {
        return;
      }
      const meta = readChangeLogMeta(db);
      const txs = readTransactions(db);
      this.#checkWhole(txs, meta.seedWatermark);
      this.#checkBackupFloor(txs, meta.seedWatermark, backupWatermark);
      this.#checkIdentity(replicaFile, replicaID, meta);
    } finally {
      db.close();
    }
  }

  // Oracle 2: above its minimum the log is whole. At most the minimum
  // transaction is torn, and it keeps its commit row. The head is retained.
  #checkWhole(txs: LogTransaction[], seed: string): void {
    const fail = (message: string) => this.#violation('2 (log whole)', message);
    if (txs.length === 0) {
      fail('the log is empty');
      return;
    }
    for (const [i, tx] of txs.entries()) {
      const {watermark, rows} = tx;
      if (rows.some((row, j) => j > 0 && row.pos !== rows[j - 1].pos + 1)) {
        fail(`${watermark} has non-contiguous positions`);
      }
      const last = rows.at(-1);
      if (last?.tag !== 'commit' || last.precommit !== watermark) {
        fail(`${watermark} does not end in its commit row`);
      }
      if (rows.slice(0, -1).some(row => row.precommit !== null)) {
        fail(`${watermark} has a precommit before its commit row`);
      }
      const whole = rows[0].pos === 0 && rows[0].tag === 'begin';
      if (!whole && i > 0) {
        fail(`${watermark}, above the minimum, is torn`);
      }
      if (!whole && i === txs.length - 1) {
        fail(`the head ${watermark} is torn`);
      }
    }
    const watermarks = txs.map(tx => tx.watermark);
    for (const watermark of watermarks) {
      if (
        watermark !== seed &&
        !watermark.includes('.') &&
        !this.#pg.isCommit(watermark)
      ) {
        fail(`${watermark} is not an upstream commit`);
      }
    }
    const min = watermarks[0];
    const head = watermarks.at(-1) ?? min;
    const expected = this.#pg
      .commitsAfter(min)
      .filter(c => c.watermark <= head)
      .map(c => c.watermark);
    const actual = watermarks.slice(1).filter(w => !w.includes('.'));
    if (expected.join() !== actual.join()) {
      fail(
        `between ${min} and ${head} the log holds [${actual}], ` +
          `upstream committed [${expected}]`,
      );
    }
  }

  // Oracle 3: nothing at or above the confirmed backup watermark is purged.
  #checkBackupFloor(
    txs: LogTransaction[],
    seed: string,
    backupWatermark: string | undefined,
  ): void {
    const min = txs[0]?.watermark;
    if (min === undefined) {
      return;
    }
    if (backupWatermark === undefined) {
      if (min !== seed) {
        this.#violation(
          '3 (purge below backup)',
          `purged to ${min}, past the seed ${seed}, with no backup confirmed`,
        );
      }
      return;
    }
    const floor = backupWatermark > seed ? backupWatermark : seed;
    if (min > floor) {
      this.#violation(
        '3 (purge below backup)',
        `purged to ${min}, past the confirmed backup ${backupWatermark} ` +
          `(seed ${seed})`,
      );
    }
  }

  // Oracle 6: the log belongs to the replica it is beside.
  #checkIdentity(
    replicaFile: string,
    replicaID: string,
    meta: {generation: string; replicaID: string | null},
  ): void {
    let replica: Database;
    try {
      replica = new Database(this.#lc, replicaFile, {readonly: true});
    } catch {
      return;
    }
    try {
      const {replicaVersion} = replica
        .prepare(
          /*sql*/ `SELECT "replicaVersion" FROM "_zero.replicationConfig"`,
        )
        .get<{replicaVersion: string}>();
      if (meta.generation !== replicaVersion || meta.replicaID !== replicaID) {
        this.#violation(
          '6 (log identity)',
          `the log is for ${meta.generation}/${meta.replicaID}, ` +
            `its replica is ${replicaVersion}/${replicaID}`,
        );
      }
    } finally {
      replica.close();
    }
  }
}

/**
 * Oracle 1 for one subscription: after its start watermark, the majors it is
 * sent are exactly upstream's commits, in order, with upstream's changes. A
 * transaction that ends in `rollback` was not delivered. Backfill minors are
 * left to oracles 8, 9, and 11.
 */
class SubscriptionOracle {
  readonly #pg: SimPG;
  readonly #fail: (message: string) => void;
  #last: string;
  #tx: {watermark: string; changes: string[]} | undefined;

  constructor(pg: SimPG, start: string, fail: (message: string) => void) {
    this.#pg = pg;
    this.#last = start;
    this.#fail = fail;
  }

  received(data: Downstream): void {
    switch (data[0]) {
      case 'status':
      case 'error':
        return;
      case 'begin':
        if (this.#tx) {
          this.#fail(
            `begin ${data[2].commitWatermark} inside ${this.#tx.watermark}`,
          );
        }
        this.#tx = {watermark: data[2].commitWatermark, changes: []};
        return;
      case 'data':
        if (!this.#tx) {
          this.#fail(`${data[1].tag} outside a transaction`);
        } else {
          this.#tx.changes.push(canonicalJSON(data[1]));
        }
        return;
      case 'rollback':
        this.#tx = undefined;
        return;
      case 'commit': {
        const tx = this.#tx;
        this.#tx = undefined;
        const {watermark} = data[2];
        if (tx?.watermark !== watermark) {
          this.#fail(
            `commit ${watermark} does not close begin ${tx?.watermark}`,
          );
          return;
        }
        if (watermark.includes('.')) {
          return;
        }
        const expected = this.#pg.commitsAfter(this.#last)[0];
        if (!expected) {
          this.#fail(`commit ${watermark} is beyond upstream's head`);
        } else if (expected.watermark !== watermark) {
          this.#fail(
            `after ${this.#last} upstream committed ${expected.watermark}, ` +
              `but ${watermark} was sent (a gap, duplicate, or reordering)`,
          );
        } else {
          const want = expected.changes.map(canonicalJSON);
          if (want.join('\n') !== tx.changes.join('\n')) {
            this.#fail(
              `${watermark} carries [${tx.changes}], upstream committed [${want}]`,
            );
          }
        }
        this.#last = watermark;
        return;
      }
    }
  }
}

type LogRow = {
  watermark: string;
  pos: number;
  tag: string;
  precommit: string | null;
};

type LogTransaction = {watermark: string; rows: LogRow[]};

function readTransactions(db: Database): LogTransaction[] {
  const txs: LogTransaction[] = [];
  for (const row of db
    .prepare(/*sql*/ `SELECT "watermark", "pos", "tag", "precommit"
        FROM "${CHANGE_LOG_STREAM_TABLE}" ORDER BY "watermark", "pos"`)
    .iterate<LogRow>()) {
    const last = txs.at(-1);
    if (last?.watermark === row.watermark) {
      last.rows.push(row);
    } else {
      txs.push({watermark: row.watermark, rows: [row]});
    }
  }
  return txs;
}

function tableExists(db: Database, table: string): boolean {
  return (
    db
      .prepare(
        /*sql*/ `SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`,
      )
      .get(table) !== undefined
  );
}

/** JSON with object keys sorted, so that key order is not a difference. */
export function canonicalJSON(value: unknown): string {
  return JSON.stringify(value, (_key, v: unknown) =>
    v && typeof v === 'object' && !Array.isArray(v)
      ? Object.fromEntries(
          Object.entries(v as Record<string, unknown>).sort(([a], [b]) =>
            a < b ? -1 : a > b ? 1 : 0,
          ),
        )
      : typeof v === 'bigint'
        ? `${v}n`
        : v,
  );
}
