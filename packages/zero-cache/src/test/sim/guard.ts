import {createHook, type AsyncHook} from 'node:async_hooks';

/**
 * Async resources that cannot make a run nondeterministic by themselves:
 * promises, microtasks and ticks (which run before the event loop moves on,
 * in the order they were queued), the clock's bound timer callbacks, and the
 * signal watcher a first `process.on('SIGINT')` starts (a view-syncer's
 * reservation listens for shutdown), which fires only on a real signal.
 */
const ALWAYS_ALLOWED = new Set([
  'PROMISE',
  'Microtask',
  'TickObject',
  'SimTimer',
  'SIGNALWRAP',
]);

// vitest's `advanceTimersByTimeAsync` yields between timers with the real
// `setImmediate` it captured when the fakes were installed. A callback fired
// by the advance runs deeper in that same stack, so what decides is who called
// `setImmediate`: vitest's bundle, or anything else.
const REAL_SET_IMMEDIATE = /\bsetImmediate \(node:timers/;
const VITEST_BUNDLE = /[\\/]vitest[\\/]dist[\\/]/;

function isFakeTimersYield(stack: string): boolean {
  const lines = stack.split('\n');
  const at = lines.findIndex(line => REAL_SET_IMMEDIATE.test(line));
  return at >= 0 && VITEST_BUNDLE.test(lines[at + 1] ?? '');
}

export type GuardViolation = {
  readonly type: string;
  readonly stack: string;
};

/**
 * Catches a run creating an async resource that the simulator does not order:
 * file or socket I/O, a real timer, a child process, a message port.
 *
 * It sees only resources created in scope, which is the run's async context,
 * so vitest's own RPC is not reported. The real immediates that
 * `SimClock.settle()` and vitest's async advance yield with are allowed; every
 * other resource is recorded with its creation stack.
 *
 * What it cannot see: a real `clearInterval` given a fake handle (the static
 * `timers` check covers that), and a synchronous wait inside native code, such
 * as SQLite's busy handler.
 */
export class DeterminismGuard {
  readonly #inScope: () => boolean;
  readonly #violations: GuardViolation[] = [];
  #hook: AsyncHook | undefined;
  #expectedImmediates = 0;

  constructor(inScope: () => boolean) {
    this.#inScope = inScope;
  }

  enable(): this {
    this.#hook ??= createHook({init: this.#init}).enable();
    return this;
  }

  disable(): void {
    this.#hook?.disable();
    this.#hook = undefined;
  }

  /** The clock is about to yield with one real `setImmediate`. */
  expectImmediate(): void {
    this.#expectedImmediates++;
  }

  /** Returns and clears what was recorded since the last call. */
  takeViolations(): GuardViolation[] {
    return this.#violations.splice(0);
  }

  readonly #init = (_asyncId: number, type: string) => {
    if (ALWAYS_ALLOWED.has(type) || !this.#inScope()) {
      return;
    }
    if (type === 'Immediate') {
      if (this.#expectedImmediates > 0) {
        this.#expectedImmediates--;
        return;
      }
      const stack = captureStack();
      if (!isFakeTimersYield(stack)) {
        this.#violations.push({type, stack});
      }
      return;
    }
    this.#violations.push({type, stack: captureStack()});
  };
}

/**
 * Compares the process's active handles and requests with a baseline taken
 * when the run started. It needs no hooks, so it stays cheap enough to run
 * after every step of a long sweep.
 */
export class ActiveResourceCheck {
  readonly #baseline: Map<string, number>;

  constructor() {
    this.#baseline = activeResourceCounts();
  }

  /** Resource types that are active now beyond the baseline, with counts. */
  excess(): string[] {
    const excess: string[] = [];
    for (const [type, count] of activeResourceCounts()) {
      const extra = count - (this.#baseline.get(type) ?? 0);
      if (extra > 0) {
        excess.push(`${type} (+${extra})`);
      }
    }
    return excess;
  }
}

function activeResourceCounts(): Map<string, number> {
  const counts = new Map<string, number>();
  for (const type of process.getActiveResourcesInfo()) {
    counts.set(type, (counts.get(type) ?? 0) + 1);
  }
  return counts;
}

function captureStack(): string {
  const limit = Error.stackTraceLimit;
  Error.stackTraceLimit = 40;
  try {
    const holder: {stack?: string} = {};
    Error.captureStackTrace(holder, captureStack);
    return holder.stack ?? '';
  } finally {
    Error.stackTraceLimit = limit;
  }
}
