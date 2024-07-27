import {assert} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import type {Ordering, Selector, SimpleOperator} from '../../ast/ast.js';
import type {Entity} from '../../schema/entity-schema.js';
import type {Multiset} from '../multiset.js';
import type {Source} from '../source/source.js';
import type {
  JoinResult,
  PipelineEntity,
  StringOrNumber,
  Version,
} from '../types.js';
import type {Reply, Request} from './message.js';
import {ConcatOperator} from './operators/concat-operator.js';
import {DebugOperator} from './operators/debug-operator.js';
import {DifferenceEffectOperator} from './operators/difference-effect-operator.js';
import {
  DistinctAllOperator,
  DistinctOperator,
} from './operators/distinct-operator.js';
import {FilterOperator} from './operators/filter-operator.js';
import {
  AggregateOut,
  FullAvgOperator,
  FullCountOperator,
  FullSumOperator,
} from './operators/full-agg-operators.js';
import {InnerJoinOperator, JoinArgs} from './operators/join-operator.js';
import {LeftJoinOperator} from './operators/left-join-operator.js';
import {MapOperator} from './operators/map-operator.js';
import type {Operator} from './operators/operator.js';
import {ReduceOperator} from './operators/reduce-operator.js';

export type Listener<T> = {
  newDifference: (
    version: Version,
    multiset: Multiset<T>,
    reply: Reply | undefined,
  ) => void;
  commit: (version: Version) => void;
};

let id = 0;

/**
 * DifferenceStream connects Sources, Operators, and Views together to form
 * pipelines.
 *
 * A DifferenceStream has zero or one "upstream" Operators, and zero or more
 * "downstream" Listeners. The listeners are typically Operators or Views. The
 * upstream is typically either a Source or Operator (although there's no
 * separate type for a Source currently -- it implements Operator).
 *
 * Usage:
 *
 * s = new DifferenceStream(); mapped = s.map(); filtered = s.filter();
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
 *
 * Changes flow through downstream via the `newDifference` method.
 *
 * The `messageUpstream` method is used to send messages up the DAG. See
 * message.ts for more information.
 */
export class DifferenceStream<T extends PipelineEntity> {
  /** Unique identifier for this stream (used for debugging). */
  readonly #id = id++;

  /** Operators that are listening to this stream. */
  readonly #downstreams: Set<Listener<T>> = new Set();

  /** The operator that is sending data to this stream. */
  #upstream: Operator | undefined;

  /** Downstreams that requested historical data. */
  readonly #requestors = new Map<number, Set<Listener<T>>>();

  addDownstream(listener: Listener<T>) {
    this.#downstreams.add(listener);
  }

  setUpstream(operator: Operator) {
    assert(this.#upstream === undefined, 'upstream already set');
    this.#upstream = operator;
    return this;
  }

  get numDownstreams() {
    return this.#downstreams.size;
  }

  newDifference(version: Version, data: Multiset<T>, reply: Reply | undefined) {
    if (reply) {
      const requestors = this.#requestors.get(reply.replyingTo);
      for (const requestor of must(requestors)) {
        requestor.newDifference(version, data, reply);
      }
      this.#requestors.delete(reply.replyingTo);
    } else {
      for (const listener of this.#downstreams) {
        listener.newDifference(version, data, reply);
      }
    }
  }

  messageUpstream(message: Request, downstream: Listener<T>): void {
    let existing = this.#requestors.get(message.id);
    if (!existing) {
      existing = new Set();
      this.#requestors.set(message.id, existing);
    }
    existing.add(downstream);
    this.#upstream?.messageUpstream(message);
  }

  commit(version: Version) {
    if (this.#requestors.size > 0) {
      for (const requestors of this.#requestors.values()) {
        for (const requestor of requestors) {
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
      }
      this.#requestors.clear();
    } else {
      for (const listener of this.#downstreams) {
        listener.commit(version);
      }
    }
  }

  map<O extends PipelineEntity>(f: (value: T) => O): DifferenceStream<O> {
    const stream = new DifferenceStream<O>();
    return stream.setUpstream(new MapOperator<T, O>(this, stream, f));
  }

  filter(
    selector: readonly [string | null, string],
    operator: SimpleOperator,
    value: unknown,
  ): DifferenceStream<T> {
    const stream = new DifferenceStream<T>();
    return stream.setUpstream(
      new FilterOperator<T>(this, stream, selector, operator, value),
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

  distinctAll(keyFn: (e: T) => StringOrNumber): DifferenceStream<T> {
    const stream = new DifferenceStream<T>();
    return stream.setUpstream(new DistinctAllOperator<T>(this, stream, keyFn));
  }

  reduce<O extends PipelineEntity>(
    keyColumns: Selector[],
    getIdentity: (value: T) => string,
    f: (input: Iterable<T>) => O,
  ): DifferenceStream<O> {
    const stream = new DifferenceStream<O>();
    return stream.setUpstream(
      new ReduceOperator<T, O>(this, stream, getIdentity, keyColumns, f),
    );
  }

  leftJoin<
    BValue extends PipelineEntity,
    AAlias extends string,
    BAlias extends string,
  >(
    args: Omit<JoinArgs<T, BValue, AAlias, BAlias>, 'a' | 'output'>,
    sourceProvider:
      | ((
          sourceName: string,
          order: Ordering | undefined,
        ) => Source<PipelineEntity>)
      | undefined,
  ): DifferenceStream<JoinResult<T, BValue, AAlias, BAlias>> {
    const stream = new DifferenceStream<
      JoinResult<T, BValue, AAlias, BAlias>
    >();
    return stream.setUpstream(
      new LeftJoinOperator(
        {
          ...args,
          a: this,
          output: stream,
        },
        sourceProvider,
      ),
    );
  }

  join<
    BValue extends PipelineEntity,
    AAlias extends string,
    BAlias extends string,
  >(
    args: Omit<JoinArgs<T, BValue, AAlias, BAlias>, 'a' | 'output'>,
  ): DifferenceStream<JoinResult<T, BValue, AAlias, BAlias>> {
    const stream = new DifferenceStream<
      JoinResult<T, BValue, AAlias, BAlias>
    >();
    return stream.setUpstream(
      new InnerJoinOperator({
        ...args,
        a: this,
        output: stream,
      }),
    );
  }

  count<Alias extends string>(alias: Alias) {
    const stream = new DifferenceStream<AggregateOut<T, [[Alias, number]]>>();
    return stream.setUpstream(new FullCountOperator(this, stream, alias));
  }

  average<Alias extends string>(selector: Selector, alias: Alias) {
    const stream = new DifferenceStream<AggregateOut<T, [[Alias, number]]>>();
    return stream.setUpstream(
      new FullAvgOperator(this, stream, selector, alias),
    );
  }

  sum<Alias extends string>(selector: Selector, alias: Alias) {
    const stream = new DifferenceStream<AggregateOut<T, [[Alias, number]]>>();
    stream.setUpstream(new FullSumOperator(this, stream, selector, alias));
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
    for (const [id, requestors] of this.#requestors) {
      for (const entry of requestors) {
        if (entry === listener) {
          requestors.delete(entry);
          if (requestors.size === 0) {
            this.#requestors.delete(id);
          }
        }
      }
    }
    if (this.#downstreams.size === 0) {
      this.destroy();
    }
  }

  toString() {
    return this.#upstream?.toString() ?? `DifferenceStream ${this.#id}`;
  }
}

export function concat<T extends PipelineEntity>(
  streams: DifferenceStream<T>[],
): DifferenceStream<T> {
  const stream = new DifferenceStream<T>();
  return stream.setUpstream(new ConcatOperator(streams, stream));
}
