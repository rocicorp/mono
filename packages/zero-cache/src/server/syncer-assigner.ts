import {h32} from '../../../shared/src/hash.ts';

export type Assignment = {
  readonly worker: number;
  readonly generation: number;
  /** The affinity key this group holds a worker in place for, if any. */
  readonly anchoredKey?: string | undefined;
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
  readonly #anchors = new Map<string, {worker: number; groups: number}>();
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
   * {@link maxAffinitySkew}.
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

    const anchor =
      affinityKey === undefined ? undefined : this.#anchors.get(affinityKey);
    const chosen =
      anchor &&
      this.#workerLoads[anchor.worker] <= minLoad + maxAffinitySkew(minLoad)
        ? anchor.worker
        : leastLoaded;

    // Only groups on the anchor's worker hold it in place: an overflow group
    // sits elsewhere, so it says nothing about where the key's work lives.
    let anchored = false;
    if (affinityKey !== undefined) {
      if (!anchor) {
        this.#anchors.set(affinityKey, {worker: chosen, groups: 1});
        anchored = true;
      } else if (anchor.worker === chosen) {
        anchor.groups++;
        anchored = true;
      }
    }

    this.#workerLoads[chosen]++;
    const assignment: Assignment = {
      worker: chosen,
      generation,
      anchoredKey: anchored ? affinityKey : undefined,
    };
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
    if (existing.anchoredKey !== undefined) {
      const anchor = this.#anchors.get(existing.anchoredKey);
      if (anchor && --anchor.groups === 0) {
        this.#anchors.delete(existing.anchoredKey);
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
    this.#anchors.clear();
    this.#workerLoads.fill(0);
  }
}
