import {appendFileSync} from 'node:fs';
import type {Context, LogLevel, LogSink} from '@rocicorp/logger';

/** One thing that happened in a run. */
export type TraceEvent = {
  readonly seq: number;
  /** Virtual milliseconds since the run started. */
  readonly t: number;
  /** `pg`, `rm`, `vs-1`, ..., or `sim` for the simulator itself. */
  readonly node: string;
  /** The node's incarnation, or 0 for an event that belongs to none. */
  readonly inc: number;
  readonly kind: string;
  readonly data?: unknown;
};

export type TraceOptions = {
  /** The virtual clock, in milliseconds since the run started. */
  readonly now: () => number;
  /** Paths below these are written as `$RUN`, since they vary by process. */
  readonly runDirs: readonly string[];
  /** Keeps every event for {@link Trace.writeJSONLines}. */
  readonly keepAll?: boolean | undefined;
  /** How many events a failure report shows. */
  readonly recent?: number | undefined;
  /** Sees every event as it is emitted, e.g. for the census. */
  readonly listener?: ((event: TraceEvent) => void) | undefined;
};

const DEFAULT_RECENT = 200;

/**
 * Everything a run did, in order, and a hash of it.
 *
 * The hash is the replay check: a seed and a step list must hash the same in
 * every process and after any other run in the same process. Each event is
 * hashed as the JSON line it is reported as, so what is hashed is exactly what
 * a failure shows. Log lines are events too, which is how purge batches and
 * route decisions reach the hash.
 */
export class Trace {
  readonly #now: () => number;
  readonly #runDirs: readonly string[];
  readonly #recentLimit: number;
  readonly #recent: string[] = [];
  readonly #all: string[] | undefined;
  readonly #listener: ((event: TraceEvent) => void) | undefined;
  #seq = 0;
  #h1 = 0x811c9dc5;
  #h2 = 0x9747b28c;

  constructor({now, runDirs, keepAll, recent, listener}: TraceOptions) {
    this.#now = now;
    this.#listener = listener;
    // Longest first, so that a directory is never rewritten by its parent.
    this.#runDirs = runDirs.toSorted((a, b) => b.length - a.length);
    this.#recentLimit = recent ?? DEFAULT_RECENT;
    this.#all = keepAll ? [] : undefined;
  }

  emit(node: string, inc: number, kind: string, data?: unknown): void {
    const event: TraceEvent = {
      seq: this.#seq++,
      t: this.#now(),
      node,
      inc,
      kind,
      ...(data === undefined ? {} : {data}),
    };
    const line = this.#normalize(stringifyEvent(event));
    this.#mix(line);
    this.#recent.push(line);
    if (this.#recent.length > this.#recentLimit) {
      this.#recent.shift();
    }
    this.#all?.push(line);
    this.#listener?.(event);
  }

  get events(): number {
    return this.#seq;
  }

  get hash(): string {
    return (
      (this.#h1 >>> 0).toString(16).padStart(8, '0') +
      (this.#h2 >>> 0).toString(16).padStart(8, '0')
    );
  }

  /** The most recent events, one JSON line each. */
  recent(): readonly string[] {
    return this.#recent;
  }

  /** Appends every event to `file`. Requires `keepAll`. */
  writeJSONLines(file: string): void {
    if (this.#all) {
      appendFileSync(file, this.#all.map(line => `${line}\n`).join(''));
    }
  }

  /** A LogSink that records each log line as an event of `node`. */
  sink(node: string, inc: number): LogSink {
    return {
      log: (
        level: LogLevel,
        context: Context | undefined,
        ...args: unknown[]
      ) => this.emit(node, inc, `log.${level}`, {context, args}),
    };
  }

  #normalize(line: string): string {
    for (const dir of this.#runDirs) {
      line = line.replaceAll(dir, '$RUN');
    }
    return line;
  }

  // Two 32-bit FNV-1a variants. Collisions only matter for the replay check,
  // which compares hashes of the same run.
  #mix(line: string): void {
    let h1 = this.#h1;
    let h2 = this.#h2;
    for (let i = 0; i < line.length; i++) {
      const c = line.charCodeAt(i);
      h1 = Math.imul(h1 ^ c, 0x01000193);
      h2 = Math.imul(h2 ^ c, 0x5bd1e995);
    }
    this.#h1 = Math.imul(h1 ^ 0x0a, 0x01000193);
    this.#h2 = Math.imul(h2 ^ 0x0a, 0x5bd1e995);
  }
}

function stringifyEvent(event: TraceEvent): string {
  const seen = new WeakSet<object>();
  return JSON.stringify(event, (_key, value: unknown) => {
    if (typeof value === 'bigint') {
      return `${value}n`;
    }
    if (typeof value === 'function' || typeof value === 'symbol') {
      return undefined;
    }
    if (value instanceof Error) {
      // Stacks name source files and line numbers, which vary with the code
      // rather than with the run.
      const {code} = value as {code?: unknown};
      return {
        name: value.name,
        message: value.message,
        ...(code === undefined ? {} : {code}),
        ...(value.cause === undefined ? {} : {cause: value.cause}),
      };
    }
    if (typeof value === 'object' && value !== null) {
      if (seen.has(value)) {
        return '[seen]';
      }
      seen.add(value);
    }
    return value;
  });
}
