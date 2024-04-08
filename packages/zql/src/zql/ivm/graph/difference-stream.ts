import {Entity} from '../../../generate.js';
import {Primitive} from '../../ast/ast.js';
import {invariant} from '../../error/asserts.js';
import {Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {Reply, Request} from './message.js';
import {ConcatOperator} from './operators/concat-operator.js';
import {DebugOperator} from './operators/debug-operator.js';
import {DifferenceEffectOperator} from './operators/difference-effect-operator.js';
import {DistinctOperator} from './operators/distinct-operator.js';
import {FilterOperator} from './operators/filter-operator.js';
import {
  AggregateOut,
  FullAvgOperator,
  FullCountOperator,
  FullSumOperator,
} from './operators/full-agg-operators.js';
import {MapOperator} from './operators/map-operator.js';
import {Operator} from './operators/operator.js';
import {ReduceOperator} from './operators/reduce-operator.js';

export type Listener<T> = {
  newDifference: (
    version: Version,
    multiset: Multiset<T>,
    reply?: Reply | undefined,
  ) => void;
  commit: (version: Version) => void;
};

/**
 * Holds a reference to a `DifferenceStreamWriter` and allows users
 * to add operators onto it.
 *
 * E.g.,
 * s = new DifferenceStream();
 *
 * mapped = s.map();
 * filtered = s.filter();
 *
 *       s
 *    /     \
 *  mapped filtered
 *
 * mappedAndFiltered = s.map().filter();
 *
 *     s
 *     |
 *    mapped
 *     |
 *   filtered
 */
// T extends object: I believe in the context of ZQL we only deal with object.
export class DifferenceStream<T extends object> {
  /**
   * Operators that are listening to this stream.
   */
  readonly #downstreams: Set<Listener<T>> = new Set();
  /**
   * The operator that is sending data to this stream.
   */
  #upstream: Operator | undefined;
  /**
   * Downstreams that requested historical data.
   */
  readonly #requestors = new Set<Listener<T>>();

  addDownstream(listener: Listener<T>) {
    this.#downstreams.add(listener);
  }

  setUpstream(operator: Operator) {
    invariant(this.#upstream === undefined, 'upstream already set');
    this.#upstream = operator;
    return this;
  }

  newData(version: Version, data: Multiset<T>, reply?: Reply | undefined) {
    if (reply) {
      for (const requestor of this.#requestors) {
        requestor.newDifference(version, data, reply);
      }
    } else {
      for (const listener of this.#downstreams) {
        listener.newDifference(version, data, reply);
      }
    }
  }

  messageUpstream(message: Request, downstream: Listener<T>): void {
    this.#requestors.add(downstream);
    this.#upstream?.messageUpstream(message);
  }

  commit(version: Version) {
    if (this.#requestors.size > 0) {
      for (const requestor of this.#requestors) {
        try {
          requestor.commit(version);
        } catch (e) {
          // `commit` notifies client code
          // If client code throws we'll put IVM back into a consistent state
          // by clearing the requestors.
          this.#requestors.clear();
          throw e;
        }
      }
      this.#requestors.clear();
    } else {
      for (const listener of this.#downstreams) {
        listener.commit(version);
      }
    }
  }

  map<O extends object>(f: (value: T) => O): DifferenceStream<O> {
    const stream = new DifferenceStream<O>();
    return stream.setUpstream(new MapOperator<T, O>(this, stream, f));
  }

  filter<S extends T>(f: (x: T) => x is S): DifferenceStream<S>;
  filter(f: (x: T) => boolean): DifferenceStream<T>;
  filter<S extends T>(f: (x: T) => boolean): DifferenceStream<S> {
    const stream = new DifferenceStream<S>();
    return stream.setUpstream(
      new FilterOperator<T>(this, stream as unknown as DifferenceStream<T>, f),
    );
  }

  distinct(): DifferenceStream<T> {
    const stream = new DifferenceStream<T>();
    return stream.setUpstream(
      new DistinctOperator<Entity>(
        this as unknown as DifferenceStream<Entity>,
        stream as unknown as DifferenceStream<Entity>,
      ),
    );
  }

  reduce<K extends Primitive, O extends object>(
    getKey: (value: T) => K,
    getIdentity: (value: T) => string,
    f: (input: Iterable<T>) => O,
  ): DifferenceStream<O> {
    const stream = new DifferenceStream<O>();
    return stream.setUpstream(
      new ReduceOperator<K, T, O>(this, stream, getIdentity, getKey, f),
    );
  }

  /**
   * This differs from count in that `size` just counts the entire
   * stream whereas `count` counts the number of times each key appears.
   * @returns returns the size of the stream
   */
  count<Alias extends string>(alias: Alias) {
    const stream = new DifferenceStream<AggregateOut<T, [[Alias, number]]>>();
    return stream.setUpstream(new FullCountOperator(this, stream, alias));
  }

  average<Field extends keyof T, Alias extends string>(
    field: Field,
    alias: Alias,
  ) {
    const stream = new DifferenceStream<AggregateOut<T, [[Alias, number]]>>();
    return stream.setUpstream(new FullAvgOperator(this, stream, field, alias));
  }

  sum<Field extends keyof T, Alias extends string>(field: Field, alias: Alias) {
    const stream = new DifferenceStream<AggregateOut<T, [[Alias, number]]>>();
    stream.setUpstream(new FullSumOperator(this, stream, field, alias));
    return stream;
  }

  /**
   * Runs a side-effect for all events in the stream.
   * If `mult < 0` that means the value V was retracted `mult` times.
   * If `mult > 0` that means the value V was added `mult` times.
   * `mult === 0` is a no-op and can be ignored. Generally shouldn't happen.
   */
  effect(f: (i: T, mult: number) => void) {
    const stream = new DifferenceStream<T>();
    stream.setUpstream(new DifferenceEffectOperator(this, stream, f));
    return stream;
  }

  debug(onMessage: (v: Version, data: Multiset<T>) => void) {
    const stream = new DifferenceStream<T>();
    stream.setUpstream(new DebugOperator(this, stream, onMessage));
    return stream;
  }

  destroy() {
    this.#upstream?.destroy();
    this.#downstreams.clear();
    this.#requestors.clear();
  }

  removeDownstream(listener: Listener<T>) {
    this.#downstreams.delete(listener);
    this.#requestors.delete(listener);
    if (this.#downstreams.size === 0) {
      this.destroy();
    }
  }
}

export function concat<T extends object>(
  streams: DifferenceStream<T>[],
): DifferenceStream<T> {
  const stream = new DifferenceStream<T>();
  return stream.setUpstream(new ConcatOperator(streams, stream));
}
