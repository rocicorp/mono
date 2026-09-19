import {h32} from '../../../shared/src/hash.ts';

export type Assignment = {
  readonly worker: number;
  readonly generation: number;
};

export class SyncerAssigner {
  readonly #taskID: string;
  readonly #numWorkers: number;
  readonly #assignments = new Map<string, Assignment>();
  readonly #workerLoads: number[];
  #nextGeneration = 0;

  constructor(taskID: string, numWorkers: number) {
    this.#taskID = taskID;
    this.#numWorkers = numWorkers;
    this.#workerLoads = new Array<number>(numWorkers).fill(0);
  }

  assign(clientGroupID: string): Assignment {
    if (this.#numWorkers <= 0) {
      return {worker: 0, generation: 0};
    }

    const generation = ++this.#nextGeneration;
    const existing = this.#assignments.get(clientGroupID);
    if (existing !== undefined) {
      const updated: Assignment = {worker: existing.worker, generation};
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

    const chosen =
      tied.length === 1
        ? tied[0]
        : tied[h32(this.#taskID + '/' + clientGroupID) % tied.length];

    this.#workerLoads[chosen]++;
    const assignment: Assignment = {worker: chosen, generation};
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
    this.#workerLoads.fill(0);
  }
}
