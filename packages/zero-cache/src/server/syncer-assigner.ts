import {h32} from '../../../shared/src/hash.ts';

type Assignment = {
  readonly worker: number;
  confirmed: boolean;
  readonly assignedAt: number;
};

export class SyncerAssigner {
  readonly #taskID: string;
  readonly #numWorkers: number;
  readonly #pendingTimeoutMs: number;
  readonly #assignments = new Map<string, Assignment>();
  readonly #workerLoads: number[];
  readonly #sweepTimer: NodeJS.Timeout | undefined;

  constructor(
    taskID: string,
    numWorkers: number,
    pendingTimeoutMs = 30_000,
    enableSweep = true,
  ) {
    this.#taskID = taskID;
    this.#numWorkers = numWorkers;
    this.#pendingTimeoutMs = pendingTimeoutMs;
    this.#workerLoads = new Array<number>(numWorkers).fill(0);

    if (enableSweep && numWorkers > 0) {
      this.#sweepTimer = setInterval(
        () => this.sweepExpired(),
        pendingTimeoutMs,
      );
      this.#sweepTimer.unref?.();
    }
  }

  assign(clientGroupID: string, now = Date.now()): number {
    if (this.#numWorkers <= 0) {
      return 0;
    }

    const existing = this.#assignments.get(clientGroupID);
    if (existing) {
      if (
        existing.confirmed ||
        now - existing.assignedAt < this.#pendingTimeoutMs
      ) {
        return existing.worker;
      }
      // Tentative assignment expired without confirmation; release it
      this.#decrementLoad(existing.worker);
      this.#assignments.delete(clientGroupID);
    }

    // Find worker(s) with minimum active load
    let minLoad = Infinity;
    const tied: number[] = [];
    for (let i = 0; i < this.#numWorkers; i++) {
      const load = this.#workerLoads[i];
      if (load < minLoad) {
        minLoad = load;
        tied.length = 0;
        tied.push(i);
      } else if (load === minLoad) {
        tied.push(i);
      }
    }

    const chosen =
      tied.length === 1
        ? tied[0]
        : tied[h32(this.#taskID + '/' + clientGroupID) % tied.length];

    this.#workerLoads[chosen]++;
    this.#assignments.set(clientGroupID, {
      worker: chosen,
      confirmed: false,
      assignedAt: now,
    });
    return chosen;
  }

  confirm(clientGroupID: string, workerIndex: number): void {
    const existing = this.#assignments.get(clientGroupID);
    if (existing && existing.worker === workerIndex) {
      existing.confirmed = true;
    }
  }

  release(clientGroupID: string, workerIndex: number): void {
    const existing = this.#assignments.get(clientGroupID);
    if (existing && existing.worker === workerIndex) {
      this.#decrementLoad(workerIndex);
      this.#assignments.delete(clientGroupID);
    }
  }

  workerCrashed(workerIndex: number): void {
    for (const [id, entry] of this.#assignments) {
      if (entry.worker === workerIndex) {
        this.#assignments.delete(id);
      }
    }
    this.#workerLoads[workerIndex] = 0;
  }

  sweepExpired(now = Date.now()): void {
    for (const [id, entry] of this.#assignments) {
      if (
        !entry.confirmed &&
        now - entry.assignedAt >= this.#pendingTimeoutMs
      ) {
        this.#decrementLoad(entry.worker);
        this.#assignments.delete(id);
      }
    }
  }

  getWorkerLoad(workerIndex: number): number {
    return this.#workerLoads[workerIndex] ?? 0;
  }

  #decrementLoad(worker: number): void {
    if (this.#workerLoads[worker] > 0) {
      this.#workerLoads[worker]--;
    }
  }

  destroy(): void {
    if (this.#sweepTimer) {
      clearInterval(this.#sweepTimer);
    }
  }
}
