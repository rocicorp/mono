import {readFile} from 'node:fs/promises';
import {performance} from 'node:perf_hooks';
import type postgres from 'postgres';
import {sleep} from '../util.ts';
import type {Recorder} from './recorder.ts';

/**
 * Stands in for an application's mutators by writing to Postgres directly.
 * Each write is one transaction running one statement, with `$1` bound to
 * the user of a randomly chosen open session.
 */
export type AppWrite = {
  readonly name: string;
  readonly weight: number;
  readonly sql: string;
};

export type AppWritesSpec = {
  readonly writes: readonly AppWrite[];
};

export async function loadAppWritesSpec(path: string): Promise<AppWritesSpec> {
  const spec = JSON.parse(await readFile(path, 'utf8')) as AppWritesSpec;
  if (!Array.isArray(spec.writes) || spec.writes.length === 0) {
    throw new Error(`${path}: an app writes spec needs at least one write`);
  }
  for (const w of spec.writes) {
    if (!w.name || !w.sql || !(w.weight > 0)) {
      throw new Error(`${path}: each write needs a name, sql and weight > 0`);
    }
  }
  return spec;
}

export type AppWriteDriverOptions = {
  readonly sql: postgres.Sql;
  readonly spec: AppWritesSpec;
  readonly writesPerSecond: number;
  readonly maxInFlight: number;
  readonly activeUserIDs: () => readonly string[];
  readonly random: () => number;
  readonly recorder: Recorder;
};

export class AppWriteDriver {
  readonly #o: AppWriteDriverOptions;
  readonly #totalWeight: number;
  #stopped = false;
  #inFlight = 0;

  constructor(options: AppWriteDriverOptions) {
    this.#o = options;
    this.#totalWeight = options.spec.writes.reduce((s, w) => s + w.weight, 0);
  }

  stop(): void {
    this.#stopped = true;
  }

  /** Issues writes at the target rate until stopped. */
  async run(): Promise<void> {
    const o = this.#o;
    const intervalMs = 1000 / o.writesPerSecond;
    let next = performance.now();
    while (!this.#stopped) {
      const now = performance.now();
      if (now < next) {
        await sleep(Math.min(100, next - now));
        continue;
      }
      next += intervalMs;
      const users = o.activeUserIDs();
      if (users.length === 0 || this.#inFlight >= o.maxInFlight) {
        continue;
      }
      const userID = users[Math.floor(o.random() * users.length)];
      void this.#write(this.#pick(), userID);
    }
    while (this.#inFlight > 0) {
      await sleep(20);
    }
  }

  #pick(): AppWrite {
    let target = this.#o.random() * this.#totalWeight;
    for (const w of this.#o.spec.writes) {
      target -= w.weight;
      if (target < 0) {
        return w;
      }
    }
    return this.#o.spec.writes.at(-1) as AppWrite;
  }

  async #write(write: AppWrite, userID: string): Promise<void> {
    this.#inFlight++;
    const start = performance.now();
    try {
      await this.#o.sql.unsafe(write.sql, [userID]);
      this.#o.recorder.appWrite(performance.now() - start, undefined);
    } catch (e) {
      this.#o.recorder.appWrite(0, `${write.name}: ${String(e)}`);
    } finally {
      this.#inFlight--;
    }
  }
}
