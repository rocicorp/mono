/**
 * Cold-boot hydration: N queries are materialized before the replica has been
 * loaded into the IVM sources (what happens on the client when `useQuery`
 * runs before `ZeroRep.init` completes).
 *
 * - "live pipelines": every row of the replica is pushed through every
 *   already-connected pipeline.
 * - "deferred pipelines": pipelines are built after the load and each view is
 *   hydrated once, via `QueryDelegate.pipelinesReady` / `onPipelinesReady`.
 *
 * Run with:
 *   pnpm --filter zql-benchmarks run bench cold-boot-hydration
 */

import {bench, describe} from '../../shared/src/bench.ts';
import type {AttachPipeline} from '../../zql/src/query/query-delegate.ts';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
import type {TypedView} from '../../zql/src/query/typed-view.ts';
import {load, makeSources, QUERIES} from './cold-boot-data.ts';

class DeferredDelegate extends QueryDelegateImpl {
  #ready = false;
  readonly #pending = new Set<AttachPipeline>();

  override get pipelinesReady(): boolean {
    return this.#ready;
  }

  override onPipelinesReady(cb: AttachPipeline): () => void {
    this.#pending.add(cb);
    return () => {
      this.#pending.delete(cb);
    };
  }

  markReady(): void {
    this.#ready = true;
    const pending = [...this.#pending];
    this.#pending.clear();
    this.batchViewUpdates(() => {
      for (const release of pending.map(attach => attach())) {
        release();
      }
    });
    this.commit();
  }
}

const opts = {max_samples: 20};

describe('cold boot hydration', () => {
  bench(
    'materialize then load: live pipelines',
    () => {
      const sources = makeSources();
      const delegate = new QueryDelegateImpl({sources});
      const views: TypedView<unknown>[] = QUERIES.map(q =>
        delegate.materialize(q()),
      );
      load(sources);
      delegate.commit();
      for (const v of views) {
        v.destroy();
      }
    },
    opts,
  );

  bench(
    'materialize then load: deferred pipelines',
    () => {
      const sources = makeSources();
      const delegate = new DeferredDelegate({sources});
      const views: TypedView<unknown>[] = QUERIES.map(q =>
        delegate.materialize(q()),
      );
      load(sources);
      delegate.markReady();
      for (const v of views) {
        v.destroy();
      }
    },
    opts,
  );

  bench(
    'load then materialize (lower bound)',
    () => {
      const sources = makeSources();
      load(sources);
      const delegate = new QueryDelegateImpl({sources});
      const views: TypedView<unknown>[] = QUERIES.map(q =>
        delegate.materialize(q()),
      );
      for (const v of views) {
        v.destroy();
      }
    },
    opts,
  );
});
