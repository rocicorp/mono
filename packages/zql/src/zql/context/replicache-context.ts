import {ExperimentalNoIndexDiff} from 'replicache';
import {Entity} from '../../generate.js';
import {ReplicacheLike} from '../../replicache-like.js';
import {Ordering} from '../ast/ast.js';
import {assert} from '../error/asserts.js';
import {compareEntityFields} from '../ivm/compare.js';
import {Materialite} from '../ivm/materialite.js';
import {MutableSetSource} from '../ivm/source/set-source.js';
import {Source} from '../ivm/source/source.js';
import {mapIter} from '../util/iterables.js';
import type {Context} from './context.js';

export function makeReplicacheContext(rep: ReplicacheLike): Context {
  const materialite = new Materialite();
  const sourceStore = new ReplicacheSourceStore(rep, materialite);

  return {
    materialite,
    getSource: <T extends Entity>(name: string, ordering?: Ordering) =>
      sourceStore.getSource(name, ordering) as unknown as Source<T>,
  };
}

/**
 * Forwards Replicache changes to ZQL sources so they
 * can be fed into any queries that may exist.
 *
 * Maintains derived orderings of sources as well.
 *
 * If someone runs a query that has an order-by we need to scan the entire collection
 * in order to sort it.
 * To save future work, we save the result of that sort and keep it up to date.
 *
 * This helps:
 * 1. When revisting old queries that were sorted or paging through results
 * 2. When many queries are sorted by the same field
 * 3. When joining sources on a field that we have pre-sorted
 *
 * And shares the work between queries.
 */
class ReplicacheSourceStore {
  readonly #rep: ReplicacheLike;
  readonly #materialite: Materialite;
  readonly #sources = new Map<string, ReplicacheSource>();

  constructor(rep: ReplicacheLike, materialite: Materialite) {
    this.#rep = rep;
    this.#materialite = materialite;
  }

  getSource(name: string, ordering?: Ordering) {
    let source = this.#sources.get(name);
    if (source === undefined) {
      source = new ReplicacheSource(this.#rep, this.#materialite, name);
      this.#sources.set(name, source);
    }

    return source.get(ordering);
  }
}

class ReplicacheSource {
  readonly #materialite;
  readonly #sorts: Map<string, Source<Entity>> = new Map();
  readonly #canonicalSource: MutableSetSource<Entity>;
  #receivedFirstDiff = false;

  constructor(rep: ReplicacheLike, materialite: Materialite, name: string) {
    this.#canonicalSource =
      materialite.newSetSource<Entity>(canonicalComparator);
    this.#materialite = materialite;
    rep.experimentalWatch(this.#onReplicacheDiff, {
      prefix: `${name}/`,
      initialValuesInFirstDiff: true,
    });
  }

  #onReplicacheDiff = (changes: ExperimentalNoIndexDiff) => {
    // The first diff is the set of initial values
    // to seed the source. We call `seed`, rather than add,
    // to process these. `seed` will only send to changes
    // to views that have explicitly requested history whereas `add` will
    // send them to everyone as if they were changes happening _now_.
    if (this.#receivedFirstDiff === false) {
      this.#canonicalSource.seed(
        mapIter(changes, diff => {
          assert(diff.op === 'add');
          return diff.newValue as Entity;
        }),
      );
      for (const derived of this.#sorts.values()) {
        derived.seed(
          mapIter(changes, diff => {
            assert(diff.op === 'add');
            return diff.newValue as Entity;
          }),
        );
      }
      this.#receivedFirstDiff = true;
      return;
    }
    this.#materialite.tx(() => {
      for (const diff of changes) {
        if (diff.op === 'del' || diff.op === 'change') {
          const old = this.#canonicalSource.get(diff.oldValue as Entity);
          assert(old, 'oldValue not found in canonical source');
          this.#canonicalSource.delete(old);
          for (const derived of this.#sorts.values()) {
            derived.delete(old);
          }
        }
        if (diff.op === 'add' || diff.op === 'change') {
          this.#canonicalSource.add(diff.newValue as Entity);
          for (const derived of this.#sorts.values()) {
            derived.add(diff.newValue as Entity);
          }
        }
      }
    });
  };

  get(ordering?: Ordering) {
    if (
      ordering === undefined ||
      ordering[0].length === 0 ||
      (ordering[0].length === 1 && ordering[0][0] === 'id')
    ) {
      return this.#canonicalSource;
    }

    const [keys] = ordering;
    // We do _not_ use the direction to derive a soure. We can iterate backwards for DESC.
    const key = keys.join(',');
    let derivation = this.#sorts.get(key);
    if (derivation === undefined) {
      const comparator = makeComparator(keys);
      derivation = this.#canonicalSource.withNewOrdering(comparator);
      this.#sorts.set(key, derivation);
    }

    return derivation;
  }
}

const canonicalComparator = (l: Entity, r: Entity) =>
  l.id < r.id ? -1 : l.id > r.id ? 1 : 0;

function makeComparator(key: readonly string[]) {
  return <T extends Entity>(l: T, r: T) => {
    let comp = 0;
    for (const k of key) {
      const lVal = l[k as unknown as keyof T];
      const rVal = r[k as unknown as keyof T];
      comp = compareEntityFields(lVal, rVal);
      if (comp !== 0) {
        return comp;
      }
    }
    return comp;
  };
}
