import {DifferenceStream, Listener} from '../graph/difference-stream.js';
import {Materialite} from '../materialite.js';
import {Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {View} from './view.js';

export abstract class AbstractView<T extends object, CT> implements View<CT> {
  readonly #stream;
  protected readonly _materialite: Materialite;
  protected readonly _listener: Listener<T>;
  readonly #listeners: Set<(s: CT, v: Version) => void> = new Set();
  readonly name;
  #hydrated = false;

  // We keep track of the last version we saw so we can keep track of whether we
  // had any changes in the last commit.
  #lastSeenVersion = -1;
  #didVersionChange = false;

  abstract get value(): CT;

  /**
   * @param stream The stream of differences that should be materialized into this sink
   * @param comparator How to sort results
   */
  constructor(
    materialite: Materialite,
    stream: DifferenceStream<T>,
    name: string = '',
  ) {
    this.name = name;
    this._materialite = materialite;
    this.#stream = stream;
    this._listener = {
      newDifference: (version: Version, data: Multiset<T>) => {
        if (version > this.#lastSeenVersion) {
          this.#lastSeenVersion = version;
          this.#didVersionChange = false;
        }
        const changed = this._newDifference(data);
        if (changed) {
          this.#didVersionChange = true;
        }
      },
      commit: (v: Version) => {
        this.#hydrated = true;
        this._notifyCommitted(this.value, v);
      },
    };
    this.#stream.addDownstream(this._listener);
  }

  get stream() {
    return this.#stream;
  }

  get hydrated() {
    return this.#hydrated;
  }

  abstract pullHistoricalData(): void;

  protected _notifyCommitted(d: CT, version: Version) {
    if (!this.#didVersionChange) {
      return;
    }
    for (const listener of this.#listeners) {
      listener(d, version);
    }
  }

  on(listener: (s: CT, v: Version) => void) {
    this.#listeners.add(listener);
    return () => {
      this.off(listener);
    };
  }

  /**
   * If there are 0 listeners left after removing the given listener,
   * the view is destroyed.
   *
   * To opt out of this behavior, pass `autoCleanup: false`
   */
  off(listener: (s: CT, v: Version) => void) {
    this.#listeners.delete(listener);
  }

  destroy() {
    this.#listeners.clear();
  }

  protected abstract _newDifference(data: Multiset<T>): boolean;
}
