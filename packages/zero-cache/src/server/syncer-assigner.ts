import {h32} from '../../../shared/src/hash.ts';

export type Assignment = {
  readonly worker: number;
  readonly generation: number;
  /** The affinity key whose groups this one shares its worker with, if any. */
  readonly affinityKey?: string | undefined;
};

/**
 * How many more groups than the least loaded worker a worker can hold and still
 * take a group for affinity. It grows with load so that it stays small next to
 * what a worker is already doing.
 */
function maxAffinitySkew(minLoad: number): number {
  return Math.max(4, Math.ceil(minLoad / 4));
}

export class SyncerAssigner {
  readonly #taskID: string;
  readonly #numWorkers: number;
  readonly #assignments = new Map<string, Assignment>();
  /** For each affinity key, the workers with groups for it, and how many. */
  readonly #keyWorkers = new Map<string, Map<number, number>>();
  readonly #workerLoads: number[];
  #nextGeneration = 0;

  constructor(taskID: string, numWorkers: number) {
    this.#taskID = taskID;
    this.#numWorkers = numWorkers;
    this.#workerLoads = new Array<number>(numWorkers).fill(0);
  }

  /**
   * Places a client group on a worker. Groups with the same `affinityKey` share
   * a worker where load allows, because only groups on one worker can reuse each
   * other's work. Affinity never overrides balance by more than
   * {@link maxAffinitySkew}. Past that, a group goes to the least loaded of the
   * other workers that have the key, and only when none can take it to a new
   * one, so that the key spreads over as few workers as possible.
   */
  assign(clientGroupID: string, affinityKey?: string): Assignment {
    if (this.#numWorkers <= 0) {
      return {worker: 0, generation: 0};
    }

    const generation = ++this.#nextGeneration;
    const existing = this.#assignments.get(clientGroupID);
    if (existing !== undefined) {
      const updated: Assignment = {...existing, generation};
      this.#assignments.set(clientGroupID, updated);
      return updated;
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

    const leastLoaded =
      tied.length === 1
        ? tied[0]
        : tied[h32(this.#taskID + '/' + clientGroupID) % tied.length];

    let chosen = leastLoaded;
    if (affinityKey !== undefined) {
      let workers = this.#keyWorkers.get(affinityKey);
      const maxLoad = minLoad + maxAffinitySkew(minLoad);
      let best: number | undefined;
      for (const worker of workers?.keys() ?? []) {
        const load = this.#workerLoads[worker];
        if (
          load <= maxLoad &&
          (best === undefined || load < this.#workerLoads[best])
        ) {
          best = worker;
        }
      }
      chosen = best ?? leastLoaded;
      if (!workers) {
        workers = new Map();
        this.#keyWorkers.set(affinityKey, workers);
      }
      workers.set(chosen, (workers.get(chosen) ?? 0) + 1);
    }

    this.#workerLoads[chosen]++;
    const assignment: Assignment = {worker: chosen, generation, affinityKey};
    this.#assignments.set(clientGroupID, assignment);
    return assignment;
  }

  release(
    clientGroupID: string,
    workerIndex: number,
    generation?: number,
  ): void {
    const existing = this.#assignments.get(clientGroupID);
    if (existing === undefined || existing.worker !== workerIndex) {
      return;
    }
    if (generation !== undefined && existing.generation !== generation) {
      return;
    }
    this.#decrementLoad(workerIndex);
    this.#assignments.delete(clientGroupID);
    if (existing.affinityKey !== undefined) {
      const workers = this.#keyWorkers.get(existing.affinityKey);
      const groups = (workers?.get(workerIndex) ?? 0) - 1;
      if (groups > 0) {
        workers?.set(workerIndex, groups);
      } else {
        workers?.delete(workerIndex);
        if (workers?.size === 0) {
          this.#keyWorkers.delete(existing.affinityKey);
        }
      }
    }
  }

  getWorkerLoad(workerIndex: number): number {
    return this.#workerLoads[workerIndex] ?? 0;
  }

  getAssignment(clientGroupID: string): number | undefined {
    return this.#assignments.get(clientGroupID)?.worker;
  }

  #decrementLoad(worker: number): void {
    if (this.#workerLoads[worker] > 0) {
      this.#workerLoads[worker]--;
    }
  }

  destroy(): void {
    this.#assignments.clear();
    this.#keyWorkers.clear();
    this.#workerLoads.fill(0);
  }
}
