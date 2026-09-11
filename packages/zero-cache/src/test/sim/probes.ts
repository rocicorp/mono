import {vi} from 'vitest';
import type {WatermarkedChange} from '../../services/change-streamer/change-streamer.ts';
import {Forwarder} from '../../services/change-streamer/forwarder.ts';
import {SQLiteChangeLogWriter} from '../../services/change-streamer/sqlite-change-log-writer.ts';
import type {Subscriber} from '../../services/change-streamer/subscriber.ts';
import {ChangeProcessor} from '../../services/replicator/change-processor.ts';
import type {Census} from './census.ts';
import {currentIncarnation, type Incarnation} from './incarnation.ts';
import type {CrashPoint} from './steps.ts';

/** Unwinds the code of an incarnation that crashed at a {@link CrashPoint}. */
export class SimulatedCrash extends Error {
  override readonly name = 'SimulatedCrash';
}

type Armed = {
  readonly point: CrashPoint;
  remaining: number;
  readonly crash: () => void;
};

/**
 * Instruments the code under test, for the length of one run.
 *
 * - **Crash points.** An incarnation can be armed to crash at the nth time its
 *   code reaches a named point: around the change log's commit of a
 *   transaction, after the transaction's `commit` is forwarded, or when a
 *   replicator is about to apply a `commit`. These are the crash points of
 *   `change-log-crash-recovery.test.ts`, end to end. The crash (copy the
 *   files, fence) runs at the point, and a {@link SimulatedCrash} then unwinds
 *   the dying code.
 * - **Taps** for census outcomes that nothing else reports: a subscriber
 *   registered while the stream loop waits in flow control.
 *
 * Each wrapper calls through synchronously and returns what it was given, so
 * it adds no await to the code it wraps.
 */
export class Probes {
  readonly #census: Census;
  readonly #armed = new Map<Incarnation, Armed>();

  constructor(census: Census) {
    this.#census = census;
  }

  arm(
    incarnation: Incarnation,
    point: CrashPoint,
    after: number,
    crash: () => void,
  ): void {
    this.#armed.set(incarnation, {point, remaining: after, crash});
  }

  /** Disarms every crash point, as the heal phase injects no new faults. */
  disarmAll(): void {
    this.#armed.clear();
  }

  install(): () => void {
    const census = this.#census;
    const reach = (point: CrashPoint) => this.#reach(point);
    const flowControlWaits = new WeakMap<Forwarder, number>();

    const write = SQLiteChangeLogWriter.prototype.write;
    const forward = Forwarder.prototype.forward;
    const forwardWithFlowControl = Forwarder.prototype.forwardWithFlowControl;
    const add = Forwarder.prototype.add;
    const processMessage = ChangeProcessor.prototype.processMessage;

    const spies = [
      vi
        .spyOn(SQLiteChangeLogWriter.prototype, 'write')
        .mockImplementation(function (
          this: SQLiteChangeLogWriter,
          ...args: Parameters<SQLiteChangeLogWriter['write']>
        ) {
          const commit = args[0][0] === 'commit';
          if (commit) {
            reach('before-log-commit');
          }
          write.apply(this, args);
          if (commit) {
            reach('after-log-commit');
          }
        }),
      vi.spyOn(Forwarder.prototype, 'forward').mockImplementation(function (
        this: Forwarder,
        entry: WatermarkedChange,
      ) {
        forward.call(this, entry);
        if (entry[1] === 'commit') {
          reach('after-forward');
        }
      }),
      vi
        .spyOn(Forwarder.prototype, 'forwardWithFlowControl')
        .mockImplementation(function (
          this: Forwarder,
          entry: WatermarkedChange,
        ) {
          const forwarded = forwardWithFlowControl.call(this, entry);
          flowControlWaits.set(this, (flowControlWaits.get(this) ?? 0) + 1);
          void forwarded
            .finally(() =>
              flowControlWaits.set(this, (flowControlWaits.get(this) ?? 1) - 1),
            )
            .catch(() => {});
          if (entry[1] === 'commit') {
            reach('after-forward');
          }
          return forwarded;
        }),
      vi.spyOn(Forwarder.prototype, 'add').mockImplementation(function (
        this: Forwarder,
        sub: Subscriber,
      ) {
        if ((flowControlWaits.get(this) ?? 0) > 0) {
          census.note('flow-control:registered-during-wait');
        }
        add.call(this, sub);
      }),
      vi
        .spyOn(ChangeProcessor.prototype, 'processMessage')
        .mockImplementation(function (
          this: ChangeProcessor,
          ...args: Parameters<ChangeProcessor['processMessage']>
        ) {
          if (args[1][0] === 'commit') {
            reach('mid-flush');
          }
          return processMessage.apply(this, args);
        }),
    ];
    return () => {
      spies.forEach(spy => spy.mockRestore());
      this.#armed.clear();
    };
  }

  #reach(point: CrashPoint): void {
    const incarnation = currentIncarnation();
    const armed = incarnation && this.#armed.get(incarnation);
    if (!incarnation || !armed || armed.point !== point) {
      return;
    }
    if (--armed.remaining > 0) {
      return;
    }
    this.#armed.delete(incarnation);
    this.#census.note(`crash:${point}`);
    armed.crash();
    throw new SimulatedCrash(`${incarnation.name} crashed ${point}`);
  }
}
