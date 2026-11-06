import {act, Suspense, useEffect, type ReactNode} from 'react';
import {hydrateRoot, type Root} from 'react-dom/client';
import {renderToReadableStream} from 'react-dom/server';
import {afterEach, describe, expect, test, vi} from 'vitest';
import {queryInternalsTag, type QueryImpl} from './bindings.ts';
import {
  QuerySnapshotCollector,
  QuerySnapshotProvider,
  serializeQuerySnapshots,
  type QuerySnapshotCollectorOptions,
  type QuerySnapshots,
} from './query-snapshots.tsx';
import {useQuery, useSuspenseQuery} from './use-query.tsx';
import {ZeroProvider} from './zero-provider.tsx';
import type {Query, Schema, Zero} from './zero.ts';

type Row = {id: string};
type Listener = (...args: unknown[]) => void;

let clientCount = 0;

function newQuery(hash: string): Query<string, Schema> {
  return {
    [queryInternalsTag]: true,
    hash: () => hash,
    format: {singular: false},
  } as unknown as QueryImpl<string, Schema>;
}

/** Each Zero gets a new client id, since views are shared per client id. */
function newZero() {
  const listeners = new Set<Listener>();
  const connectionListeners = new Set<Listener>();
  const connection = {
    state: {
      current: {name: 'connected'} as {name: string},
      subscribe: (cb: Listener) => {
        connectionListeners.add(cb);
        return () => connectionListeners.delete(cb);
      },
    },
  };
  const zero = {
    clientID: `snapshot-client-${clientCount++}`,
    connection,
    hasPendingMutations: false,
    materialize: vi.fn(() => ({
      addListener: (cb: Listener) => listeners.add(cb),
      destroy: () => listeners.clear(),
      updateTTL() {},
    })),
  } as unknown as Zero<Schema>;
  return {
    zero,
    emit: (...args: unknown[]) => {
      for (const cb of listeners) {
        cb(...args);
      }
    },
    connectionListenerCount: () => connectionListeners.size,
    setPendingMutations: (pending: boolean) => {
      (zero as {hasPendingMutations: boolean}).hasPendingMutations = pending;
    },
    setConnection: (name: string) => {
      connection.state.current = {name};
      for (const cb of connectionListeners) {
        cb(connection.state.current);
      }
    },
  };
}

function List({query}: {query: Query<string, Schema>}) {
  const [rows, details] = useQuery(query);
  return (
    <ul data-type={details.type}>
      {(rows as unknown as Row[]).map(r => (
        <li key={r.id}>{r.id}</li>
      ))}
    </ul>
  );
}

function PartialList({query}: {query: Query<string, Schema>}) {
  const [rows] = useSuspenseQuery(query, {suspendUntil: 'partial'});
  return (
    <ul>
      {(rows as unknown as Row[]).map(r => (
        <li key={r.id}>{r.id}</li>
      ))}
    </ul>
  );
}

function SuspenseList({
  query,
  onHydrated,
}: {
  query: Query<string, Schema>;
  onHydrated: () => void;
}) {
  const [rows] = useSuspenseQuery(query, {suspendUntil: 'complete'});
  useEffect(onHydrated, [onHydrated]);
  return (
    <ul>
      {(rows as unknown as Row[]).map(r => (
        <li key={r.id}>{r.id}</li>
      ))}
    </ul>
  );
}

const rows = [{id: 'a'}, {id: 'b'}];

describe('collecting on the server', () => {
  /** Starts a streaming render, as a server does, without waiting for it. */
  async function startRender(
    query: Query<string, Schema>,
    Component: typeof List | typeof PartialList = List,
    client = newZero(),
    options?: QuerySnapshotCollectorOptions,
  ) {
    const {zero, emit} = client;
    const collector = new QuerySnapshotCollector(options);
    const stream = await renderToReadableStream(
      <ZeroProvider zero={zero}>
        <QuerySnapshotProvider collector={collector}>
          <Suspense fallback={<i>fallback</i>}>
            <Component query={query} />
          </Suspense>
        </QuerySnapshotProvider>
      </ZeroProvider>,
    );
    let ready = false;
    const allReady = stream.allReady.then(() => {
      ready = true;
    });
    const settle = () => new Promise(resolve => setTimeout(resolve, 10));
    return {
      emit,
      collector,
      isReady: async () => {
        await settle();
        return ready;
      },
      html: async () => {
        await allReady;
        return new Response(stream).text();
      },
    };
  }

  test('records what was rendered, keyed by query hash', async () => {
    const {emit, collector, html} = await startRender(newQuery('q1'));
    emit(rows, 'complete');
    await html();
    expect(collector.toJSON()).toEqual({q1: {data: rows, type: 'complete'}});
  });

  test('useQuery waits for the server instead of rendering an empty page', async () => {
    const {emit, isReady, html} = await startRender(newQuery('q2'));
    emit([], 'unknown');
    expect(await isReady()).toBe(false);

    emit(rows, 'complete');
    expect(await isReady()).toBe(true);
    expect(await html()).toContain('<li>a</li><li>b</li>');
  });

  test('useQuery does not render local rows the server has not confirmed', async () => {
    const {emit, isReady, html} = await startRender(newQuery('q3'));
    emit([{id: 'partial'}], 'unknown');
    expect(await isReady()).toBe(false);

    emit(rows, 'complete');
    expect(await html()).not.toContain('partial');
  });

  test('suspendUntil partial still waits for the server', async () => {
    const {emit, isReady, html} = await startRender(
      newQuery('q4'),
      PartialList,
    );
    emit([{id: 'partial'}], 'unknown');
    expect(await isReady()).toBe(false);

    emit(rows, 'complete');
    expect(await html()).not.toContain('partial');
  });

  test.each(['needs-auth', 'error', 'disconnected', 'closed'])(
    'fails the render as soon as the connection becomes %s',
    async name => {
      const client = newZero();
      const {isReady, html} = await startRender(
        newQuery(`q6-${name}`),
        List,
        client,
      );
      expect(await isReady()).toBe(false);

      client.setConnection(name);
      expect(await isReady()).toBe(true);
      expect(await html()).toContain('<i>fallback</i>');
    },
  );

  test('fails the render if the connection is already lost', async () => {
    const client = newZero();
    client.setConnection('needs-auth');
    const {isReady, html} = await startRender(newQuery('q7'), List, client);
    expect(await isReady()).toBe(true);
    expect(await html()).toContain('<i>fallback</i>');
  });

  test('keeps waiting while the connection is only reconnecting', async () => {
    const client = newZero();
    const {emit, isReady, html} = await startRender(
      newQuery('q8'),
      List,
      client,
    );
    client.setConnection('connecting');
    expect(await isReady()).toBe(false);
    client.setConnection('connected');
    expect(await isReady()).toBe(false);

    emit(rows, 'complete');
    expect(await html()).toContain('<li>a</li>');
  });

  test('a render after the connection recovers waits for the server again', async () => {
    const client = newZero();
    const failed = await startRender(newQuery('q9'), List, client);
    client.setConnection('needs-auth');
    await failed.html();

    client.setConnection('connected');
    const {emit, isReady, html} = await startRender(
      newQuery('q9'),
      List,
      client,
    );
    expect(await isReady()).toBe(false);

    emit(rows, 'complete');
    expect(await html()).toContain('<li>a</li>');
  });

  describe('a live view when the connection is lost', () => {
    async function liveView(hash: string) {
      const client = newZero();
      const first = await startRender(newQuery(hash), List, client);
      client.emit(rows, 'complete');
      await first.html();
      return client;
    }

    test.each(['needs-auth', 'error', 'disconnected', 'closed'])(
      'fails the render by default when the connection is %s',
      async name => {
        const client = await liveView(`q10-${name}`);
        client.setConnection(name);
        const {html} = await startRender(newQuery(`q10-${name}`), List, client);
        const out = await html();
        expect(out).toContain('<i>fallback</i>');
        expect(out).not.toContain('<li>a</li>');
      },
    );

    test('waits for a reconnect by default', async () => {
      const client = await liveView('q11');
      client.setConnection('connecting');
      const {isReady, html} = await startRender(newQuery('q11'), List, client);
      expect(await isReady()).toBe(false);

      client.setConnection('connected');
      expect(await isReady()).toBe(true);
      expect(await html()).toContain('<li>a</li>');
    });

    test('fails if a reconnect ends with the connection lost', async () => {
      const client = await liveView('q12');
      client.setConnection('connecting');
      const {isReady, html} = await startRender(newQuery('q12'), List, client);
      expect(await isReady()).toBe(false);

      client.setConnection('needs-auth');
      expect(await isReady()).toBe(true);
      expect(await html()).toContain('<i>fallback</i>');
    });

    test.each(['needs-auth', 'error', 'disconnected', 'connecting'])(
      "renders what it holds with 'serve' when the connection is %s",
      async name => {
        const client = await liveView(`q13-${name}`);
        client.setConnection(name);
        const {html} = await startRender(
          newQuery(`q13-${name}`),
          List,
          client,
          {whenDisconnected: 'serve'},
        );
        expect(await html()).toContain('<li>a</li>');
      },
    );

    test("'serve' still fails a query with nothing to show", async () => {
      const client = newZero();
      client.setConnection('needs-auth');
      const {html} = await startRender(newQuery('q14'), List, client, {
        whenDisconnected: 'serve',
      });
      expect(await html()).toContain('<i>fallback</i>');
    });

    test('renders with different options can share a view', async () => {
      const client = await liveView('q15');
      client.setConnection('connecting');
      const failing = await startRender(newQuery('q15'), List, client);
      const serving = await startRender(newQuery('q15'), List, client, {
        whenDisconnected: 'serve',
      });
      expect(await serving.html()).toContain('<li>a</li>');
      expect(await failing.isReady()).toBe(false);

      client.setConnection('connected');
      expect(await failing.html()).toContain('<li>a</li>');
    });
  });

  test('skips errored queries, whose details hold a retry function', async () => {
    const {emit, collector, html} = await startRender(newQuery('q5'));
    emit([], 'error', {error: 'app', id: 'q5', name: 'q5', message: 'boom'});
    await html();
    expect(collector.toJSON()).toEqual({});
  });
});

describe('serializeQuerySnapshots', () => {
  test('cannot close the script tag it is embedded in', () => {
    const json = serializeQuerySnapshots({
      q: {data: ['</script><img src=x>\u2028'], type: 'complete'},
    });
    expect(json).not.toContain('<');
    expect(json).not.toContain('\u2028');
    expect(JSON.parse(json)).toEqual({
      q: {data: ['</script><img src=x>\u2028'], type: 'complete'},
    });
  });
});

describe('hydrating on the client', () => {
  let root: Root | undefined;

  afterEach(() => {
    act(() => root?.unmount());
    root = undefined;
    document.body.replaceChildren();
  });

  function hydrate(ui: ReactNode, html: string) {
    const container = document.createElement('div');
    container.innerHTML = html;
    document.body.append(container);
    const recoverableErrors: unknown[] = [];
    act(() => {
      root = hydrateRoot(container, ui, {
        onRecoverableError: e => recoverableErrors.push(e),
      });
    });
    return {
      recoverableErrors,
      text: () =>
        Array.from(container.querySelectorAll('li'), li => li.innerHTML),
    };
  }

  function hydrateSeeded(hash: string, initialConnection?: string) {
    const client = newZero();
    if (initialConnection) {
      client.setConnection(initialConnection);
    }
    const query = newQuery(hash);
    const snapshots: QuerySnapshots = {
      [hash]: {data: rows, type: 'complete'},
    };
    const app = (
      <ZeroProvider zero={client.zero}>
        <QuerySnapshotProvider snapshots={snapshots}>
          <List query={query} />
        </QuerySnapshotProvider>
      </ZeroProvider>
    );
    return {
      ...client,
      ...hydrate(app, '<ul data-type="complete"><li>a</li><li>b</li></ul>'),
    };
  }

  test('matches the server HTML', () => {
    const {recoverableErrors, text} = hydrateSeeded('h1');
    expect(recoverableErrors).toEqual([]);
    expect(text()).toEqual(['a', 'b']);
  });

  test('keeps showing the seed over an empty view', () => {
    const {emit, text} = hydrateSeeded('h2');
    act(() => emit([], 'unknown'));
    expect(text()).toEqual(['a', 'b']);
  });

  test('keeps showing the seed over cached rows, which may be older', () => {
    const {emit, text} = hydrateSeeded('h3');
    act(() => emit([{id: 'stale'}], 'cached'));
    expect(text()).toEqual(['a', 'b']);
  });

  test('switches to the live view once the server confirms it', () => {
    const {emit, text} = hydrateSeeded('h4');
    act(() => emit([{id: 'fresh'}], 'complete'));
    expect(text()).toEqual(['fresh']);
    act(() => emit([{id: 'fresh'}, {id: 'optimistic'}], 'unknown'));
    expect(text()).toEqual(['fresh', 'optimistic']);
  });

  test('shows a server error rather than holding the seed', () => {
    const {emit, text} = hydrateSeeded('h5');
    act(() =>
      emit([], 'error', {error: 'app', id: 'h5', name: 'h5', message: 'boom'}),
    );
    expect(text()).toEqual([]);
  });

  test.each(['disconnected', 'error', 'needs-auth', 'closed'])(
    'falls back to local rows when the connection becomes %s',
    name => {
      const {emit, setConnection, text} = hydrateSeeded(`h6-${name}`);
      act(() => emit([{id: 'local'}], 'cached'));
      expect(text()).toEqual(['a', 'b']);
      act(() => setConnection(name));
      expect(text()).toEqual(['local']);
    },
  );

  test.each(['disconnected', 'error', 'needs-auth', 'closed'])(
    'keeps the seed over an empty view when the connection becomes %s',
    name => {
      const {emit, setConnection, text} = hydrateSeeded(`h11-${name}`);
      act(() => emit([], 'unknown'));
      act(() => setConnection(name));
      expect(text()).toEqual(['a', 'b']);
    },
  );

  test('keeps the seed when the connection was already lost', () => {
    const {emit, text} = hydrateSeeded('h12', 'disconnected');
    act(() => emit([], 'unknown'));
    expect(text()).toEqual(['a', 'b']);
  });

  test('keeps the seed over unconfirmed rows after the connection was lost', () => {
    const {emit, setConnection, text} = hydrateSeeded('h13');
    act(() => setConnection('disconnected'));
    act(() => emit([{id: 'offline-write'}], 'unknown'));
    expect(text()).toEqual(['a', 'b']);
  });

  test('shows cached rows that arrive after the connection was lost', () => {
    const {emit, setConnection, text} = hydrateSeeded('h14');
    act(() => setConnection('disconnected'));
    act(() => emit([{id: 'local'}], 'cached'));
    expect(text()).toEqual(['local']);
  });

  test('a write ends the hold for a client with cached rows', () => {
    const {emit, setPendingMutations, text} = hydrateSeeded('h15');
    act(() => emit([{id: 'old'}], 'cached'));
    expect(text()).toEqual(['a', 'b']);

    setPendingMutations(true);
    act(() => emit([{id: 'old'}, {id: 'write'}], 'cached'));
    expect(text()).toEqual(['old', 'write']);
  });

  test('a write keeps the seed for a client with no cached rows', () => {
    const {emit, setPendingMutations, text} = hydrateSeeded('h16');
    act(() => emit([], 'unknown'));

    setPendingMutations(true);
    act(() => emit([{id: 'write'}], 'unknown'));
    expect(text()).toEqual(['a', 'b']);

    act(() => emit([{id: 'a'}, {id: 'b'}, {id: 'write'}], 'complete'));
    expect(text()).toEqual(['a', 'b', 'write']);
  });

  test('a write elsewhere leaves a view it did not change on the seed', () => {
    const {emit, setPendingMutations, text} = hydrateSeeded('h17');
    act(() => emit([{id: 'old'}], 'cached'));
    setPendingMutations(true);
    expect(text()).toEqual(['a', 'b']);
  });

  test('stays on the seed through a reconnect attempt', () => {
    const {emit, setConnection, text} = hydrateSeeded('h7');
    act(() => emit([], 'unknown'));
    act(() => setConnection('connected'));
    expect(text()).toEqual(['a', 'b']);
  });

  test('useSuspenseQuery hydrates a complete seed without suspending', () => {
    const {zero} = newZero();
    const query = newQuery('h8');
    const onHydrated = vi.fn();
    const {recoverableErrors, text} = hydrate(
      <ZeroProvider zero={zero}>
        <QuerySnapshotProvider snapshots={{h8: {data: rows, type: 'complete'}}}>
          <Suspense fallback={<i>fallback</i>}>
            <SuspenseList query={query} onHydrated={onHydrated} />
          </Suspense>
        </QuerySnapshotProvider>
      </ZeroProvider>,
      '<!--$--><ul><li>a</li><li>b</li></ul><!--/$-->',
    );
    expect(recoverableErrors).toEqual([]);
    expect(text()).toEqual(['a', 'b']);
    expect(onHydrated).toHaveBeenCalledOnce();
  });

  test('stops watching the connection when the view is destroyed', async () => {
    const {connectionListenerCount, text} = hydrateSeeded('h10');
    expect(text()).toEqual(['a', 'b']);
    expect(connectionListenerCount()).toBe(1);
    act(() => root?.unmount());
    root = undefined;
    await new Promise(resolve => setTimeout(resolve, 30));
    expect(connectionListenerCount()).toBe(0);
  });

  test('a query with no snapshot starts empty as usual', () => {
    const {zero} = newZero();
    const {text} = hydrate(
      <ZeroProvider zero={zero}>
        <QuerySnapshotProvider snapshots={{}}>
          <List query={newQuery('h9')} />
        </QuerySnapshotProvider>
      </ZeroProvider>,
      '<ul data-type="unknown"></ul>',
    );
    expect(text()).toEqual([]);
  });
});
