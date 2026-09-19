import {h32} from '../../../shared/src/hash.ts';

export class SyncerAssigner {
  readonly #taskID: string;
  readonly #numWorkers: number;
  readonly #assignments = new Map<string, number>();
  readonly #workerLoads: number[];

  constructor(taskID: string, numWorkers: number) {
    this.#taskID = taskID;
    this.#numWorkers = numWorkers;
    this.#workerLoads = new Array<number>(numWorkers).fill(0);
  }

  assign(clientGroupID: string): number {
    if (this.#numWorkers <= 0) {
      return 0;
    }

    const existing = this.#assignments.get(clientGroupID);
    if (existing !== undefined) {
      return existing;
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
    this.#assignments.set(clientGroupID, chosen);
    return chosen;
  }

  release(clientGroupID: string, workerIndex: number): void {
    const existing = this.#assignments.get(clientGroupID);
    if (existing === workerIndex) {
      this.#decrementLoad(workerIndex);
      this.#assignments.delete(clientGroupID);
    }
  }

  getWorkerLoad(workerIndex: number): number {
    return this.#workerLoads[workerIndex] ?? 0;
  }

  getAssignment(clientGroupID: string): number | undefined {
    return this.#assignments.get(clientGroupID);
  }

  #decrementLoad(worker: number): void {
    if (this.#workerLoads[worker] > 0) {
      this.#workerLoads[worker]--;
    }
  }

  destroy(): void {
    this.#assignments.clear();
  }
}
