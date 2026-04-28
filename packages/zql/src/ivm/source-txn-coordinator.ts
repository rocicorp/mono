import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {ChangeType} from './change-type.ts';
import type {Source, SourceTxnListener} from './source.ts';
import type {Stream} from './stream.ts';

/**
 * Drives a `MultiSourceUnionFanIn`'s accumulation window without a
 * `UnionFanOut` directly above the union.
 *
 * The coordinator subscribes to one or more `Source`s as a `SourceTxnListener`.
 * Whenever a subscribed source begins/ends a logical push, the coordinator
 * forwards the begin/end signals to the registered fan-in so it can
 * accumulate pushes from independent branch source connections and emit a
 * single deduped change at the end.
 */
export interface CoordinatedFanIn {
  fanOutStartedPushing(): void;
  fanOutDonePushing(changeType: ChangeType): Stream<'yield'>;
}

export class SourceTxnCoordinator implements SourceTxnListener {
  #fanIn: CoordinatedFanIn | undefined;
  readonly #unsubscribers: (() => void)[] = [];

  setFanIn(fanIn: CoordinatedFanIn): void {
    assert(this.#fanIn === undefined, 'FanIn already set on coordinator');
    this.#fanIn = fanIn;
  }

  attachSource(source: Source): void {
    this.#unsubscribers.push(source.addTxnListener(this));
  }

  beginPush(): void {
    must(this.#fanIn).fanOutStartedPushing();
  }

  *endPush(changeType: ChangeType): Stream<'yield'> {
    yield* must(this.#fanIn).fanOutDonePushing(changeType);
  }

  destroy(): void {
    for (const unsubscribe of this.#unsubscribers) {
      unsubscribe();
    }
    this.#unsubscribers.length = 0;
  }
}
