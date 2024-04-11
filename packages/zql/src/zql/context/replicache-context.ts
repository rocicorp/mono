import type {ExperimentalNoIndexDiff} from 'replicache';
import {assert} from 'shared/src//asserts.js';
import type {Entity} from '../../entity.js';
import type {ReplicacheLike} from '../../replicache-like.js';
import {Materialite} from '../ivm/materialite.js';
import type {MutableSetSource} from '../ivm/source/set-source.js';
import type {Source} from '../ivm/source/source.js';
import {mapIter} from '../util/iterables.js';
import type {Context} from './context.js';
import {compareUTF8} from 'compare-utf8';

export function makeReplicacheContext(rep: ReplicacheLike): Context {
  const materialite = new Materialite();
  const sourceStore = new ReplicacheSourceStore(rep, materialite);

  return {
    materialite,
    getSource: <T extends Entity>(name: string) =>
      sourceStore.getSource(name) as unknown as Source<T>,
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

  getSource(name: string) {
    let source = this.#sources.get(name);
    if (source === undefined) {
      source = new ReplicacheSource(this.#rep, this.#materialite, name);
      this.#sources.set(name, source);
    }

    return source.get();
  }
}

class ReplicacheSource {
  readonly #materialite;
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
      this.#receivedFirstDiff = true;
      return;
    }
    this.#materialite.tx(() => {
      for (const diff of changes) {
        if (diff.op === 'del' || diff.op === 'change') {
          const old = this.#canonicalSource.get(diff.oldValue as Entity);
          assert(old, 'oldValue not found in canonical source');
          this.#canonicalSource.delete(old);
        }
        if (diff.op === 'add' || diff.op === 'change') {
          this.#canonicalSource.add(diff.newValue as Entity);
        }
      }
    });
  };

  get() {
    return this.#canonicalSource;
  }
}

const canonicalComparator = (l: Entity, r: Entity) => compareUTF8(l.id, r.id);
