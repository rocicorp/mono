import {execFileSync, spawn, type ChildProcess} from 'node:child_process';
import {createWriteStream, type WriteStream} from 'node:fs';
import {rm} from 'node:fs/promises';
import {basename, dirname, join} from 'node:path';
import {createInterface} from 'node:readline';
import {APP_ROOT, ZERO_CACHE_MAIN} from './config.ts';
import type {SoakLog} from './logs.ts';

/**
 * Every pid in the process tree rooted at `root`, deepest first.
 *
 * A signal to the process group is not enough: `childWorker` forks each
 * zero-cache worker with `detached: true` (so that SIGINT is not propagated
 * automatically and graceful shutdown happens as intended), which puts every
 * worker in its own process group. Signalling the dispatcher's group reaches
 * only the dispatcher and the runner, and a dispatcher that dies without
 * draining leaves its change-streamer, backup-replicator, litestream and
 * vfs-query processes running -- still serving view-syncers, and still
 * holding the replica's locks, which is what makes the next start fail with
 * `SQLITE_BUSY: journal_mode = delete`.
 */
function processTree(root: number): number[] {
  let listing: string;
  try {
    listing = execFileSync('ps', ['-Ao', 'pid=,ppid='], {
      encoding: 'utf8',
    });
  } catch {
    return [root];
  }
  const childrenOf = new Map<number, number[]>();
  for (const line of listing.split('\n')) {
    const match = PS_LINE.exec(line);
    if (!match) {
      continue;
    }
    const pid = Number(match[1]);
    const ppid = Number(match[2]);
    const siblings = childrenOf.get(ppid);
    if (siblings) {
      siblings.push(pid);
    } else {
      childrenOf.set(ppid, [pid]);
    }
  }
  const order: number[] = [];
  const visit = (pid: number) => {
    for (const child of childrenOf.get(pid) ?? []) {
      visit(child);
    }
    order.push(pid);
  };
  visit(root);
  return order;
}

const PS_LINE = /^\s*(\d+)\s+(\d+)\s*$/;

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function signalAll(pids: readonly number[], signal: NodeJS.Signals): void {
  for (const pid of pids) {
    try {
      process.kill(pid, signal);
    } catch {
      // Already gone.
    }
  }
}

export type NodeOptions = {
  readonly name: string;
  readonly env: NodeJS.ProcessEnv;
  readonly logsDir: string;
  readonly log: SoakLog;
  readonly replicaFile: string;
};

/**
 * One zero-cache process tree: the replication-manager, or one view-syncer.
 *
 * Started with cwd = apps/zbugs so that `shared/src/dotenv.ts` finds the same
 * `.env` the ordinary dev flow uses. Every variable this harness cares about
 * is set explicitly in {@link NodeOptions.env}, and dotenvx does not override
 * a variable that is already present -- but it does inject one that is
 * absent, which is why the view-syncer environment says
 * `SQLITE_CHANGE_LOG_MODE=off` rather than omitting it.
 */
export class SoakNode {
  readonly name: string;
  readonly replicaFile: string;
  /**
   * Mutable: the change-log mode is a startup option, so the rollback drills
   * (C10-C12) rewrite it between restarts.
   */
  env: NodeJS.ProcessEnv;
  readonly #opts: NodeOptions;
  #child: ChildProcess | undefined;
  #logStream: WriteStream | undefined;
  #exited: Promise<{code: number | null; signal: NodeJS.Signals | null}> =
    Promise.resolve({code: null, signal: null});
  #unexpectedExit: Promise<never> = new Promise(() => {});
  #startCount = 0;
  #stopping = false;
  /**
   * Every pid this incarnation has ever been seen to own.
   *
   * Accumulated rather than sampled once, because the tree has to be readable
   * *while* the node is alive: once the dispatcher dies its workers are
   * reparented to init and `ps` can no longer relate them back to this node.
   */
  readonly #tree = new Set<number>();

  constructor(opts: NodeOptions) {
    this.#opts = opts;
    this.name = opts.name;
    this.replicaFile = opts.replicaFile;
    this.env = opts.env;
  }

  get running(): boolean {
    const child = this.#child;
    return (
      child !== undefined &&
      child.exitCode === null &&
      child.signalCode === null
    );
  }

  get pid(): number | undefined {
    return this.#child?.pid;
  }

  get startCount(): number {
    return this.#startCount;
  }

  get unexpectedExit(): Promise<never> {
    return this.#unexpectedExit;
  }

  /**
   * Spawns the process and resolves when the dispatcher reports every worker
   * ready. Rejects if it exits first.
   */
  async start(readyTimeoutMs = 300_000): Promise<void> {
    if (this.running) {
      throw new Error(`${this.name} is already running`);
    }
    this.#startCount++;
    const logPath = join(
      this.#opts.logsDir,
      `${this.name}.${this.#startCount}.log`,
    );
    this.#logStream = createWriteStream(logPath, {flags: 'a'});

    const child = spawn(
      process.execPath,
      ['--trace-warnings', ZERO_CACHE_MAIN],
      {
        cwd: APP_ROOT,
        env: this.env,
        stdio: ['ignore', 'pipe', 'pipe'],
        // Its own process group, so that SIGSTOP/SIGCONT (C7) and SIGKILL
        // (C8) reach the workers and not just the dispatcher.
        detached: true,
      },
    );
    this.#child = child;
    this.#stopping = false;
    this.#tree.clear();

    for (const stream of [child.stdout, child.stderr]) {
      if (!stream) {
        continue;
      }
      const lines = createInterface({input: stream, crlfDelay: Infinity});
      lines.on('line', line => {
        this.#logStream?.write(`${line}\n`);
        this.#opts.log.push(this.name, line);
      });
    }

    this.#exited = new Promise(resolve => {
      child.once('exit', (code, signal) => {
        this.#logStream?.end();
        this.#logStream = undefined;
        resolve({code, signal});
      });
    });
    this.#unexpectedExit = this.#exited.then(({code, signal}) => {
      if (!this.#stopping) {
        throw new Error(
          `${this.name} exited unexpectedly (code=${code} signal=${signal})`,
        );
      }
      return new Promise<never>(() => {});
    });
    // A node can exit between awaited harness operations. Mark the rejection
    // handled here; `SoakCluster.guard()` still observes the original promise
    // and fails the operation currently in flight.
    this.#unexpectedExit.catch(() => undefined);

    // Sample the tree while the node is coming up, so that a start which
    // fails partway through still leaves a reapable set behind.
    const tracker = setInterval(() => this.#trackTree(), 500);
    this.#trackTree();

    const ready = this.#opts.log.waitForRecord(
      `${this.name} to report all workers ready`,
      record =>
        record.node === this.name &&
        record.message.startsWith('all workers ready'),
      readyTimeoutMs,
    );
    try {
      await Promise.race([ready, this.#unexpectedExit]);
    } catch (e) {
      // A failed start still leaves workers behind; reap them so the next
      // attempt is not blocked by the replica locks they hold.
      await this.#reap();
      throw e;
    } finally {
      clearInterval(tracker);
      this.#trackTree();
    }
  }

  #trackTree(): void {
    const pid = this.#child?.pid;
    if (pid === undefined) {
      return;
    }
    for (const child of processTree(pid)) {
      this.#tree.add(child);
    }
  }

  /** Sends `signal` and waits for exit. SIGKILLs after `graceMs`. */
  async stop(
    signal: NodeJS.Signals = 'SIGTERM',
    graceMs = 30_000,
  ): Promise<void> {
    const child = this.#child;
    if (!child || !this.running) {
      return;
    }
    this.#stopping = true;
    const exited = this.#exited;
    // Refresh the tree before signalling; afterwards the parents are gone and
    // `ps` can no longer relate the orphans back to this node.
    this.#trackTree();
    const tree = [...this.#tree];
    if (signal === 'SIGKILL') {
      // A hard crash takes the workers with it, the way losing the container
      // does in production.
      signalAll(tree, 'SIGKILL');
    } else {
      child.kill(signal);
    }
    let killer: NodeJS.Timeout | undefined;
    if (signal !== 'SIGKILL') {
      killer = setTimeout(() => {
        if (this.running) {
          signalAll(tree, 'SIGKILL');
        }
      }, graceMs);
    }
    try {
      await exited;
    } finally {
      clearTimeout(killer);
    }
    this.#child = undefined;
    await this.#reap();
  }

  /**
   * Waits briefly for the workers to finish their own shutdown, then kills
   * whatever is left. The dispatcher's exit does not imply its workers have
   * exited, and a surviving worker holds the replica against the next start.
   */
  async #reap(): Promise<void> {
    const deadline = Date.now() + 10_000;
    let survivors = [...this.#tree].filter(alive);
    while (survivors.length > 0 && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 200));
      survivors = survivors.filter(alive);
    }
    if (survivors.length > 0) {
      signalAll(survivors, 'SIGKILL');
    }
  }

  /**
   * Sends a signal to the whole process tree without waiting; for
   * SIGSTOP / SIGCONT (chaos C7), where pausing only the dispatcher would
   * leave the change-streamer serving and pause nothing that matters.
   */
  signal(signal: NodeJS.Signals): void {
    const pid = this.#child?.pid;
    if (!this.running || pid === undefined) {
      throw new Error(`${this.name} is not running`);
    }
    this.#trackTree();
    signalAll(processTree(pid), signal);
  }

  /**
   * Deletes the replica, its sidecars and litestream's local state, but not
   * the change log.
   *
   * The litestream directory goes too: in production a view-syncer that loses
   * its replica comes back on a fresh volume with nothing beside it, so
   * leaving local backup state behind would be a different scenario from the
   * one C3 means to run.
   */
  async deleteReplica(): Promise<void> {
    if (this.running) {
      throw new Error(`refusing to delete ${this.name}'s live replica`);
    }
    await rm(
      join(
        dirname(this.replicaFile),
        `.${basename(this.replicaFile)}-litestream`,
      ),
      {force: true, recursive: true},
    );
    await Promise.all(
      [
        this.replicaFile,
        `${this.replicaFile}-shm`,
        `${this.replicaFile}-wal`,
        `${this.replicaFile}-wal2`,
        `${this.replicaFile}-serving-copy`,
        `${this.replicaFile}-serving-copy-shm`,
        `${this.replicaFile}-serving-copy-wal`,
        `${this.replicaFile}-serving-copy-wal2`,
      ].map(file => rm(file, {force: true})),
    );
  }

  /** Deletes only the change log, leaving the replica and backups valid. */
  async deleteChangeLog(): Promise<void> {
    if (this.running) {
      throw new Error(`refusing to delete ${this.name}'s live change log`);
    }
    const changeLog = `${this.replicaFile}-change-log`;
    await Promise.all(
      [
        changeLog,
        `${changeLog}-shm`,
        `${changeLog}-wal`,
        `${changeLog}-wal2`,
      ].map(file => rm(file, {force: true})),
    );
  }
}
