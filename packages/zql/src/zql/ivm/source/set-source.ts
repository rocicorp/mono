import {Treap} from '@vlcn.io/ds-and-algos/Treap';
import {Comparator, ITree} from '@vlcn.io/ds-and-algos/types';
import {must} from '../../error/asserts.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {PullMsg, Request, createPullResponseMessage} from '../graph/message.js';
import {MaterialiteForSourceInternal} from '../materialite.js';
import {Entry, Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {Source, SourceInternal} from './source.js';

/**
 * A source that remembers what values it contains.
 *
 * This allows pipelines that are created after a source already
 * exists to be able to receive historical data.
 *
 */
export abstract class SetSource<T extends object> implements Source<T> {
  readonly #stream: DifferenceStream<T>;
  readonly #internal: SourceInternal;
  protected readonly _materialite: MaterialiteForSourceInternal;
  readonly #listeners = new Set<(data: ITree<T>, v: Version) => void>();
  #pending: Entry<T>[] = [];
  #tree: ITree<T>;
  #seeded = false;
  // Use a set instead of an array to avoid duplicates due to branching.
  #historyRequests = new Set<PullMsg>();
  readonly comparator: Comparator<T>;

  constructor(
    materialite: MaterialiteForSourceInternal,
    comparator: Comparator<T>,
    treapConstructor: (comparator: Comparator<T>) => ITree<T>,
  ) {
    this._materialite = materialite;
    this.#stream = new DifferenceStream<T>();
    this.#stream.setUpstream({
      commit: () => {},
      messageUpstream: (message: Request) => {
        this.processMessage(message);
      },
      destroy: () => {},
    });
    this.#tree = treapConstructor(comparator);
    this.comparator = comparator;

    this.#internal = {
      onCommitEnqueue: (version: Version) => {
        for (let i = 0; i < this.#pending.length; i++) {
          const [val, mult] = must(this.#pending[i]);
          // small optimization to reduce operations for replace
          if (i + 1 < this.#pending.length) {
            const [nextVal, nextMult] = must(this.#pending[i + 1]);
            if (
              Math.abs(mult) === 1 &&
              mult === -nextMult &&
              comparator(val, nextVal) === 0
            ) {
              // The tree doesn't allow dupes -- so this is a replace.
              this.#tree = this.#tree.add(nextMult > 0 ? nextVal : val);
              ++i;
              continue;
            }
          }
          if (mult < 0) {
            this.#tree = this.#tree.delete(val);
          } else if (mult > 0) {
            this.#tree = this.#tree.add(val);
          }
        }

        this.#stream.newData(version, this.#pending);
        this.#pending = [];
      },
      onCommitted: (version: Version) => {
        // In case we have direct source observers
        const tree = this.#tree;
        for (const l of this.#listeners) {
          l(tree, version);
        }
        this.#stream.commit(version);
      },
      onRollback: () => {
        this.#pending = [];
      },
    };
  }

  withNewOrdering(comp: Comparator<T>): this {
    const ret = this._withNewOrdering(comp);
    if (this.#seeded) {
      ret.seed(this.#tree);
    }
    return ret;
  }

  protected abstract _withNewOrdering(comp: Comparator<T>): this;

  get stream(): DifferenceStream<T> {
    return this.#stream;
  }

  get value() {
    return this.#tree;
  }

  destroy(): void {
    this.#listeners.clear();
    this.#stream.destroy();
  }

  on(cb: (value: ITree<T>, version: Version) => void): () => void {
    this.#listeners.add(cb);
    return () => this.#listeners.delete(cb);
  }

  off(fn: (value: ITree<T>, version: Version) => void): void {
    this.#listeners.delete(fn);
  }

  add(v: T): this {
    this.#pending.push([v, 1]);
    this._materialite.addDirtySource(this.#internal);
    return this;
  }

  delete(v: T): this {
    this.#pending.push([v, -1]);
    this._materialite.addDirtySource(this.#internal);
    return this;
  }

  /**
   * Seeds the source with historical data.
   *
   * Does not send historical data to downstreams
   * unless they have asked for it.
   */
  seed(values: Iterable<T>): this {
    for (const v of values) {
      this.#tree = this.#tree.add(v);
    }
    this.#seeded = true;
    const requests = this.#historyRequests;
    this.#historyRequests = new Set();
    for (const message of requests) {
      this.#sendHistoricalData(message);
    }
    return this;
  }

  get(key: T): T | undefined {
    const ret = this.#tree.get(key);
    if (ret === null) {
      return undefined;
    }
    return ret;
  }

  processMessage(message: Request): void {
    switch (message.type) {
      case 'pull': {
        this.#sendHistoricalData(message);
        break;
      }
    }
  }

  #sendHistoricalData(message: Request) {
    if (!this.#seeded) {
      // wait till we're seeded.
      this.#historyRequests.add(message);
      return;
    }

    const response = createPullResponseMessage(message);

    this.#stream.newData(
      this._materialite.getVersion(),
      asEntries(this.#tree, message),
      response,
    );
    this.#stream.commit(this._materialite.getVersion());
  }
}

export class MutableSetSource<T extends object> extends SetSource<T> {
  constructor(
    materialite: MaterialiteForSourceInternal,
    comparator: Comparator<T>,
  ) {
    super(materialite, comparator, comparator => new Treap(comparator));
  }

  protected _withNewOrdering(comp: Comparator<T>): this {
    return new MutableSetSource<T>(this._materialite, comp) as this;
  }
}

function asEntries<T>(m: ITree<T>, _message: Request): Multiset<T> {
  // message will contain hoisted expressions so we can do relevant
  // index selection against the source.
  // const after = hoisted.expressions.filter((e) => e._tag === "after")[0];
  // if (after && after.comparator === comparator) {
  //   return {
  //     [Symbol.iterator]() {
  //       return gen(m.iteratorAfter(after.cursor));
  //     },
  //   };
  // }
  return {
    [Symbol.iterator]() {
      return gen(m);
    },
  };
}

function* gen<T>(m: Iterable<T>) {
  for (const v of m) {
    yield [v, 1] as const;
  }
}
