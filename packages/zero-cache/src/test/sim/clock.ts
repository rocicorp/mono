import {AsyncResource} from 'node:async_hooks';
import {vi} from 'vitest';
import {currentIncarnation, type Incarnation} from './incarnation.ts';

// Captured at import, before any run fakes the global.
const realSetImmediate = globalThis.setImmediate;

/** Every global a run fakes. Microtasks and `nextTick` stay real. */
const FAKED = [
  'setTimeout',
  'clearTimeout',
  'setInterval',
  'clearInterval',
  'setImmediate',
  'clearImmediate',
  'Date',
  'performance',
] as const;

type Clear = (handle: unknown) => void;

export type ClockHooks = {
  /** A timer is about to run its callback. */
  readonly fired?:
    | ((kind: string, owner: Incarnation | undefined) => void)
    | undefined;
  /** `settle()` is about to yield with a real `setImmediate`. */
  readonly yielding?: (() => void) | undefined;
};

/**
 * Virtual time for a run, shared by every incarnation.
 *
 * Installing fakes the globals in {@link FAKED} and wraps the faked timer
 * functions once. The wrapper binds each callback to the async context it was
 * scheduled in, which fake timers lose (they call `func.apply(null, args)`),
 * and files the timer under the scheduling incarnation, so that fencing it
 * cancels exactly its timers. A fenced incarnation's later timers never fire.
 *
 * Clocks are captured at construction (`opts.now ?? Date.now`,
 * `setTimeoutFn = setTimeout`), so a run installs before building anything and
 * never reinstalls. `node:timers` imports are never faked; the static check in
 * `timers.sim.test.ts` keeps them off the simulated path.
 */
export class SimClock {
  readonly #epochMs: number;
  readonly #hooks: ClockHooks;
  readonly #timersOf = new Map<Incarnation, Map<unknown, Clear>>();
  readonly #ownerOf = new Map<unknown, Incarnation>();
  #installed = false;

  constructor(epochMs: number, hooks: ClockHooks = {}) {
    this.#epochMs = epochMs;
    this.#hooks = hooks;
  }

  install(): void {
    if (this.#installed) {
      throw new Error('the simulated clock is already installed');
    }
    vi.useFakeTimers({toFake: [...FAKED], now: this.#epochMs});
    this.#installed = true;

    const g = globalThis;
    const setTimeoutFake = g.setTimeout;
    const clearTimeoutFake = g.clearTimeout;
    const setIntervalFake = g.setInterval;
    const clearIntervalFake = g.clearInterval;
    const setImmediateFake = g.setImmediate;
    const clearImmediateFake = g.clearImmediate;

    const clearTimeoutHandle: Clear = h =>
      clearTimeoutFake(h as NodeJS.Timeout);
    const clearIntervalHandle: Clear = h =>
      clearIntervalFake(h as NodeJS.Timeout);
    const clearImmediateHandle: Clear = h =>
      clearImmediateFake(h as NodeJS.Immediate);

    g.setTimeout = ((
      callback: (...args: unknown[]) => void,
      ms?: number,
      ...args: unknown[]
    ) =>
      this.#schedule(
        'timeout',
        true,
        clearTimeoutHandle,
        fire => setTimeoutFake(fire, ms),
        () => callback(...args),
      )) as unknown as typeof setTimeout;
    g.setInterval = ((
      callback: (...args: unknown[]) => void,
      ms?: number,
      ...args: unknown[]
    ) =>
      this.#schedule(
        'interval',
        false,
        clearIntervalHandle,
        fire => setIntervalFake(fire, ms),
        () => callback(...args),
      )) as unknown as typeof setInterval;
    g.setImmediate = ((
      callback: (...args: unknown[]) => void,
      ...args: unknown[]
    ) =>
      this.#schedule(
        'immediate',
        true,
        clearImmediateHandle,
        fire => setImmediateFake(fire),
        () => callback(...args),
      )) as unknown as typeof setImmediate;
    g.clearTimeout = ((handle: unknown) => {
      this.#forget(handle);
      clearTimeoutHandle(handle);
    }) as typeof clearTimeout;
    g.clearInterval = ((handle: unknown) => {
      this.#forget(handle);
      clearIntervalHandle(handle);
    }) as typeof clearInterval;
    g.clearImmediate = ((handle: unknown) => {
      this.#forget(handle);
      clearImmediateHandle(handle);
    }) as typeof clearImmediate;
  }

  /** Restores the real globals, discarding every pending timer. */
  uninstall(): void {
    if (this.#installed) {
      vi.clearAllTimers();
      vi.useRealTimers();
      this.#installed = false;
      this.#timersOf.clear();
      this.#ownerOf.clear();
    }
  }

  /** Virtual milliseconds since the clock was installed. */
  elapsed(): number {
    return Date.now() - this.#epochMs;
  }

  /**
   * Moves virtual time forward, firing due timers in order. Between timers it
   * drains microtasks, as Node does; the synchronous `advanceTimersByTime`
   * would not.
   */
  async advance(ms: number): Promise<void> {
    await vi.advanceTimersByTimeAsync(ms);
  }

  /**
   * Drains every promise chain that does not wait on a timer. With no real I/O
   * outstanding, one macrotask turn does it: the event loop reaches the check
   * phase only once the microtask queue is empty. Fires no timers, including a
   * zero-delay one, which runs at the next {@link advance}.
   */
  settle(): Promise<void> {
    return new Promise(resolve => {
      this.#hooks.yielding?.();
      realSetImmediate(resolve);
    });
  }

  /** Timers pending for `owner`, for the trace and for tests of the clock. */
  pendingTimers(owner: Incarnation): number {
    return this.#timersOf.get(owner)?.size ?? 0;
  }

  #schedule(
    kind: string,
    once: boolean,
    clear: Clear,
    schedule: (fire: () => void) => unknown,
    run: () => void,
  ): unknown {
    const owner = currentIncarnation();
    if (owner?.fenced) {
      // Dead code keeps running until it blocks. Whatever it schedules is a
      // real handle, since callers unref and clear them, but it never fires.
      const handle = schedule(() => {});
      clear(handle);
      return handle;
    }
    const timers = owner && this.#timersFor(owner);
    let handle: unknown;
    const fire = AsyncResource.bind(() => {
      if (once) {
        this.#forget(handle);
      }
      this.#hooks.fired?.(kind, owner);
      run();
    }, 'SimTimer');
    handle = schedule(fire);
    if (owner && timers) {
      timers.set(handle, clear);
      this.#ownerOf.set(handle, owner);
    }
    return handle;
  }

  #timersFor(owner: Incarnation): Map<unknown, Clear> {
    let timers = this.#timersOf.get(owner);
    if (!timers) {
      const created = new Map<unknown, Clear>();
      timers = created;
      this.#timersOf.set(owner, created);
      owner.onFence(() => {
        for (const [handle, clear] of created) {
          clear(handle);
          this.#ownerOf.delete(handle);
        }
        created.clear();
        this.#timersOf.delete(owner);
      });
    }
    return timers;
  }

  #forget(handle: unknown): void {
    const owner = this.#ownerOf.get(handle);
    if (owner) {
      this.#ownerOf.delete(handle);
      this.#timersOf.get(owner)?.delete(handle);
    }
  }
}
