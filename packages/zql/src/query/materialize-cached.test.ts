import {describe, expect, test, vi} from 'vitest';
import {must} from '../../../shared/src/must.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {makeSourceChangeAdd} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import type {GotCallback} from './query-delegate.ts';
import {newQuery} from './query-impl.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';
import {schema} from './test/test-schemas.ts';
import type {TTL} from './ttl.ts';
import type {ResultType} from './typed-view.ts';

/**
 * A delegate that reports a chosen `got` value synchronously at registration,
 * the way QueryManager does when the persisted got set is already loaded.
 */
class CachedDelegate extends QueryDelegateImpl {
  initialGot: boolean | 'cached' = false;

  override addServerQuery(
    ast: AST,
    ttl: TTL,
    gotCallback?: GotCallback,
  ): () => void {
    const cleanup = super.addServerQuery(ast, ttl, gotCallback);
    gotCallback?.(this.initialGot);
    return cleanup;
  }

  /** The got callback of the most recently materialized query. */
  lastGot(got: boolean | 'cached') {
    must(this.gotCallbacks.at(-1))(got);
  }
}

function newDelegate() {
  const delegate = new CachedDelegate();
  const issues = must(delegate.getSource('issue'));
  consume(
    issues.push(
      makeSourceChangeAdd({
        id: 'i1',
        title: 'issue i1',
        description: '',
        closed: false,
        ownerId: null,
        createdAt: 1,
      }),
    ),
  );
  return delegate;
}

function observe(delegate: CachedDelegate) {
  const view = delegate.materialize(newQuery(schema, 'issue'));
  // Every notification is recorded, so a test also pins that a result type
  // change arrives in exactly one notification.
  const types: ResultType[] = [];
  view.addListener((_data, type) => {
    types.push(type);
  });
  return {view, types};
}

describe('cached result type', () => {
  test('boots cached when the got set reports cached at registration', () => {
    const delegate = newDelegate();
    delegate.initialGot = 'cached';
    const {view, types} = observe(delegate);
    expect(types).toEqual(['cached']);
    expect(view.data).toMatchObject([{id: 'i1'}]);
    view.destroy();
  });

  test('cached reported after materialize', () => {
    const delegate = newDelegate();
    const {view, types} = observe(delegate);
    expect(types).toEqual(['unknown']);
    delegate.lastGot('cached');
    expect(types).toEqual(['unknown', 'cached']);
    view.destroy();
  });

  test('cached graduates to complete on the first server confirmation', async () => {
    const delegate = newDelegate();
    delegate.initialGot = 'cached';
    const {view, types} = observe(delegate);
    delegate.lastGot(true);
    await vi.waitFor(() => expect(types.at(-1)).toBe('complete'));
    expect(types).toEqual(['cached', 'complete']);
    view.destroy();
  });

  test('cached reverts to unknown when the got key is evicted', () => {
    const delegate = newDelegate();
    delegate.initialGot = 'cached';
    const {view, types} = observe(delegate);
    delegate.lastGot(false);
    expect(types).toEqual(['cached', 'unknown']);
    view.destroy();
  });

  test('complete is never downgraded to cached', async () => {
    const delegate = newDelegate();
    const {view, types} = observe(delegate);
    delegate.lastGot(true);
    await vi.waitFor(() => expect(types.at(-1)).toBe('complete'));
    delegate.lastGot('cached');
    delegate.lastGot(false);
    expect(types).toEqual(['unknown', 'complete']);
    view.destroy();
  });

  test('cached does not satisfy run({type: complete})', async () => {
    const delegate = newDelegate();
    delegate.initialGot = 'cached';
    let resolved = false;
    const p = delegate
      .run(newQuery(schema, 'issue'), {type: 'complete'})
      .then(data => {
        resolved = true;
        return data;
      });
    await Promise.resolve();
    await Promise.resolve();
    expect(resolved).toBe(false);

    delegate.lastGot(true);
    expect(await p).toMatchObject([{id: 'i1'}]);
  });

  test('run({type: cached}) resolves on a cached result', async () => {
    const delegate = newDelegate();
    delegate.initialGot = 'cached';
    expect(
      await delegate.run(newQuery(schema, 'issue'), {type: 'cached'}),
    ).toMatchObject([{id: 'i1'}]);
  });

  test('run({type: cached}) resolves on complete when nothing is cached', async () => {
    const delegate = newDelegate();
    let resolved = false;
    const p = delegate
      .run(newQuery(schema, 'issue'), {type: 'cached'})
      .then(data => {
        resolved = true;
        return data;
      });
    await Promise.resolve();
    await Promise.resolve();
    expect(resolved).toBe(false);

    delegate.lastGot(true);
    expect(await p).toMatchObject([{id: 'i1'}]);
  });

  test('preload().cached resolves on cached, complete only on confirmation', async () => {
    const delegate = newDelegate();
    delegate.initialGot = 'cached';
    const {complete, cached, cleanup} = delegate.preload(
      newQuery(schema, 'issue'),
    );
    await cached;
    let completed = false;
    void complete.then(() => {
      completed = true;
    });
    await Promise.resolve();
    expect(completed).toBe(false);

    delegate.lastGot(true);
    await complete;
    cleanup();
  });

  test('preload().cached also resolves on a confirmation', async () => {
    const delegate = newDelegate();
    const {complete, cached, cleanup} = delegate.preload(
      newQuery(schema, 'issue'),
    );
    delegate.lastGot(true);
    await Promise.all([cached, complete]);
    cleanup();
  });

  test('cached does not satisfy preload().complete', async () => {
    const delegate = newDelegate();
    delegate.initialGot = 'cached';
    let resolved = false;
    const {complete, cleanup} = delegate.preload(newQuery(schema, 'issue'));
    void complete.then(() => {
      resolved = true;
    });
    await Promise.resolve();
    await Promise.resolve();
    expect(resolved).toBe(false);

    delegate.lastGot(true);
    await complete;
    expect(resolved).toBe(true);
    cleanup();
  });

  describe('with deferred hydration', () => {
    test('cached waits for the pipeline to be attached', () => {
      const delegate = newDelegate();
      delegate.deferPipelines();
      delegate.initialGot = 'cached';
      const {view, types} = observe(delegate);
      // The view is still empty: it cannot vouch for rows it does not hold.
      expect(types).toEqual(['unknown']);
      expect(view.data).toEqual([]);

      delegate.markReady();
      expect(types).toEqual(['unknown', 'cached']);
      expect(view.data).toMatchObject([{id: 'i1'}]);
      view.destroy();
    });

    test('eviction while deferred cancels the pending cached mark', () => {
      const delegate = newDelegate();
      delegate.deferPipelines();
      delegate.initialGot = 'cached';
      const {view, types} = observe(delegate);
      delegate.lastGot(false);
      delegate.markReady();
      // The attach flush delivers the rows, still at 'unknown'.
      expect(types).toEqual(['unknown', 'unknown']);
      view.destroy();
    });

    test('server confirmation while deferred wins over cached', async () => {
      const delegate = newDelegate();
      delegate.deferPipelines();
      delegate.initialGot = 'cached';
      const {view, types} = observe(delegate);
      delegate.lastGot(true);
      delegate.markReady();
      await vi.waitFor(() => expect(types.at(-1)).toBe('complete'));
      // The attach flush delivers the rows at 'unknown'; 'cached' is never
      // shown because the server confirmation arrived first.
      expect(types).toEqual(['unknown', 'unknown', 'complete']);
      view.destroy();
    });
  });
});
