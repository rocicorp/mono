/* eslint-disable @typescript-eslint/no-explicit-any */
import {resolver} from '@rocicorp/resolver';
import {hashOfNameAndArgs} from '../../../zero-protocol/src/query-hash.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {buildPipeline} from '../builder/builder.ts';
import {ArrayView} from '../ivm/array-view.ts';
import type {Input} from '../ivm/operator.ts';
import type {Format, ViewFactory} from '../ivm/view.ts';
import type {GotCallback, QueryDelegate} from './query-delegate.ts';
import type {AbstractQuery} from './query-impl.ts';
import {
  type HumanReadable,
  type PreloadOptions,
  type Query,
  type RunOptions,
} from './query.ts';
import {DEFAULT_PRELOAD_TTL_MS, DEFAULT_TTL_MS, type TTL} from './ttl.ts';
import type {TypedView} from './typed-view.ts';

export function runQuery<
  TSchema extends Schema,
  TTable extends string,
  TReturn,
>(
  delegate: QueryDelegate,
  query: Query<TSchema, TTable, TReturn>,
  options?: RunOptions,
): Promise<HumanReadable<TReturn>> {
  delegate.assertValidRunOptions(options);

  const v = materializeQuery(delegate, query, options?.ttl) as TypedView<
    HumanReadable<TReturn>
  >;

  if (options?.type === 'complete') {
    return new Promise(resolve => {
      v.addListener((data, type) => {
        if (type === 'complete') {
          v.destroy();
          resolve(data as HumanReadable<TReturn>);
        } else if (type === 'error') {
          v.destroy();
          resolve(Promise.reject(data));
        }
      });
    });
  }

  options?.type satisfies 'unknown' | undefined;

  const ret = v.data;
  v.destroy();
  return Promise.resolve(ret);
}

export function materializeQuery<
  TSchema extends Schema,
  TTable extends string,
  TReturn,
  T,
>(
  delegate: QueryDelegate,
  query: Query<TSchema, TTable, TReturn>,
  factoryOrTTL?: ViewFactory<TSchema, TTable, TReturn, T> | TTL,
  ttl: TTL = DEFAULT_TTL_MS,
): T | TypedView<HumanReadable<TReturn>> {
  let factory: ViewFactory<TSchema, TTable, TReturn, T> | undefined;
  if (typeof factoryOrTTL === 'function') {
    factory = factoryOrTTL;
  } else {
    ttl = factoryOrTTL ?? DEFAULT_TTL_MS;
  }

  const ast = query.completedAST();
  const queryID = query.customQueryID
    ? hashOfNameAndArgs(query.customQueryID.name, query.customQueryID.args)
    : query.hash();
  const queryCompleteResolver = resolver<true>();
  let queryComplete = delegate.defaultQueryComplete;

  const updateTTL = (newTTL: TTL) => {
    query.customQueryID
      ? delegate.updateCustomQuery(query.customQueryID, newTTL)
      : delegate.updateServerQuery(ast, newTTL);
  };

  const gotCallback: GotCallback = (got, error) => {
    if (error) {
      queryCompleteResolver.reject(error);
      queryComplete = true;
      return;
    }

    if (got) {
      delegate.addMetric(
        'query-materialization-end-to-end',
        performance.now() - t0,
        queryID,
        ast,
      );
      queryComplete = true;
      queryCompleteResolver.resolve(true);
    }
  };

  let removeCommitObserver: (() => void) | undefined;
  const onDestroy = () => {
    input.destroy();
    removeCommitObserver?.();
    removeAddedQuery();
  };

  const t0 = performance.now();

  const removeAddedQuery = query.customQueryID
    ? delegate.addCustomQuery(ast, query.customQueryID, ttl, gotCallback)
    : delegate.addServerQuery(ast, ttl, gotCallback);

  const input = buildPipeline(ast, delegate, queryID);

  const view = delegate.batchViewUpdates(() =>
    (factory ?? arrayViewFactory)(
      query as AbstractQuery<TSchema, TTable, TReturn>,
      input,
      query.format,
      onDestroy,
      cb => {
        removeCommitObserver = delegate.onTransactionCommit(cb);
      },
      queryComplete || queryCompleteResolver.promise,
      updateTTL,
    ),
  );

  delegate.addMetric(
    'query-materialization-client',
    performance.now() - t0,
    queryID,
  );

  return view as T;
}

export function preloadQuery<
  TSchema extends Schema,
  TTable extends string,
  TReturn,
>(
  delegate: QueryDelegate,
  query: Query<TSchema, TTable, TReturn>,
  options?: PreloadOptions,
): {
  cleanup: () => void;
  complete: Promise<void>;
} {
  const ttl = options?.ttl ?? DEFAULT_PRELOAD_TTL_MS;
  const ast = query.completedAST();
  const {resolve, promise: complete} = resolver<void>();

  if (query.customQueryID) {
    const cleanup = delegate.addCustomQuery(
      ast,
      query.customQueryID,
      ttl,
      got => {
        if (got) {
          resolve();
        }
      },
    );
    return {
      cleanup,
      complete,
    };
  }

  const cleanup = delegate.addServerQuery(ast, ttl, got => {
    if (got) {
      resolve();
    }
  });
  return {
    cleanup,
    complete,
  };
}

function arrayViewFactory<
  TSchema extends Schema,
  TTable extends string,
  TReturn,
>(
  _query: AbstractQuery<TSchema, TTable, TReturn>,
  input: Input,
  format: Format,
  onDestroy: () => void,
  onTransactionCommit: (cb: () => void) => void,
  queryComplete: true | Promise<true>,
  updateTTL: (ttl: TTL) => void,
): TypedView<HumanReadable<TReturn>> {
  const v = new ArrayView<HumanReadable<TReturn>>(
    input,
    format,
    queryComplete,
    updateTTL,
  );
  v.onDestroy = onDestroy;
  onTransactionCommit(() => {
    v.flush();
  });
  return v;
}
