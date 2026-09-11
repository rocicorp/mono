import {AsyncLocalStorage} from 'node:async_hooks';

const current = new AsyncLocalStorage<Incarnation>();

/**
 * One life of a simulated node, from its start until it is fenced.
 *
 * The simulator runs each incarnation's code inside {@link run}, and the async
 * context carries the incarnation from there: through promise continuations,
 * and through timer callbacks, which the clock binds (fake timers do not). So
 * {@link currentIncarnation} names whose code is running wherever the harness
 * needs to know: to cancel its timers, close its databases, and drop its calls.
 *
 * A crash is a fence, not a stop. A fenced incarnation's code keeps running
 * until it blocks, and nothing it does from then on can reach anything the
 * next incarnation uses.
 */
export class Incarnation {
  readonly node: string;
  readonly number: number;
  readonly #fenceHooks: (() => void)[] = [];
  #fenced = false;

  constructor(node: string, number: number) {
    this.node = node;
    this.number = number;
  }

  get name(): string {
    return `${this.node}#${this.number}`;
  }

  get fenced(): boolean {
    return this.#fenced;
  }

  run<T>(fn: () => T): T {
    return current.run(this, fn);
  }

  /**
   * Registers cleanup for when this incarnation is fenced, or runs it now if
   * it already has been.
   */
  onFence(hook: () => void): void {
    if (this.#fenced) {
      hook();
    } else {
      this.#fenceHooks.push(hook);
    }
  }

  /** Marks the incarnation dead and runs its fence hooks, newest first. */
  fence(): void {
    if (this.#fenced) {
      return;
    }
    this.#fenced = true;
    for (const hook of this.#fenceHooks.splice(0).reverse()) {
      hook();
    }
  }
}

/** The incarnation whose code is running, if any. */
export function currentIncarnation(): Incarnation | undefined {
  return current.getStore();
}

/** Whether the running code belongs to a fenced incarnation. */
export function inFencedIncarnation(): boolean {
  return current.getStore()?.fenced ?? false;
}
