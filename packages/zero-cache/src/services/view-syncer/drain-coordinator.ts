import {resolver} from '@rocicorp/resolver';
import {assert} from '../../../../shared/src/asserts.js';

/**
 * There are two types of drains:
 * 1. Elective drains happen when a view-syncer is about to process
 *    a replication event and drains instead because {@link shouldDrain()}
 *    returned true. In this case, it exits its processing loop and
 *    calls {@link drainNextIn()} immediately.
 * 2. Force drains are performed by the Syncer, which picks a random
 *    view-syncer and calls {@link stop()}.
 *
 * In the case of a forced drain, the view-syncer may be queued up
 * behind a large amount of CPU-consuming events, and thus it may take
 * a long time before it actually stops. Elective drains are preferred because
 * they are less subject to that variance. However, elective drains only happen
 * if there is a task to be processed, so forced drains are still necessary
 * for draining servers with no work / tasks.
 *
 * The Syncer kicks off the drain process by calling {@link drainNextIn drainNextIn(0)},
 * which sets off a short {@link forceDrainTimeout} but starts returning `true` for
 * {@link shouldDrain()} for elective drains. In the latter case, the drained
 * view-syncer immediately exiting its processing loop and calls
 * {@link drainNextIn drainNextIn(myHydrationTime)} to reset the timeout
 * for the next elective or forced drain.
 */
export class DrainCoordinator {
  #nextDrainTime = 0;
  #timeout = resolver();
  #timeoutID: NodeJS.Timeout | undefined;

  shouldDrain() {
    return this.#nextDrainTime && this.#nextDrainTime <= Date.now();
  }

  drainNextIn(interval: number) {
    const now = Date.now();
    assert(
      this.#nextDrainTime <= now,
      `drainNextIn() should only be called if shouldDrain()`,
    );
    this.#nextDrainTime = now + interval;

    // Push the forceDrainTimeout forward.
    clearTimeout(this.#timeoutID);
    this.#timeoutID = setTimeout(() => {
      this.#timeout.resolve();
      this.#timeout = resolver();
    }, interval + FORCE_DRAIN_PADDING);
  }

  get forceDrainTimeout() {
    return this.#timeout.promise;
  }

  // Exposed for testing.
  get nextDrainTime() {
    return this.#nextDrainTime;
  }
}

const FORCE_DRAIN_PADDING = 2;
