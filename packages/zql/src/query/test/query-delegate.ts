import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {
  deepEqual,
  type ReadonlyJSONValue,
} from '../../../../shared/src/json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import type {FilterInput} from '../../ivm/filter-operators.ts';
import {MemoryStorage} from '../../ivm/memory-storage.ts';
import type {Input} from '../../ivm/operator.ts';
import type {Source, SourceInput} from '../../ivm/source.ts';
import {createSource} from '../../ivm/test/source-factory.ts';
import type {ViewFactory} from '../../ivm/view.ts';
import type {CustomQueryID} from '../named.ts';
import type {
  CommitListener,
  GotCallback,
  QueryDelegate,
  WithContext,
} from '../query-delegate.ts';
import {materializeImpl, preloadImpl, runImpl} from '../query-impl.ts';
import {type QueryInternals} from '../query-internals.ts';
import type {
  HumanReadable,
  MaterializeOptions,
  PreloadOptions,
  Query,
  RunOptions,
} from '../query.ts';
import type {TTL} from '../ttl.ts';
import type {TypedView} from '../typed-view.ts';
import {
  commentSchema,
  issueLabelSchema,
  issueSchema,
  labelSchema,
  revisionSchema,
  userSchema,
} from './test-schemas.ts';

const lc = createSilentLogContext();

type Entry = {
  ast: AST | undefined;
  name: string | undefined;
  args: readonly ReadonlyJSONValue[] | undefined;
  ttl: TTL;
};
export class QueryDelegateImpl<TContext = undefined>
  implements QueryDelegate<TContext>
{
  readonly #sources: Record<string, Source> = makeSources();
  readonly #commitListeners: Set<CommitListener> = new Set();

  readonly addedServerQueries: Entry[] = [];
  readonly gotCallbacks: (GotCallback | undefined)[] = [];
  synchronouslyCallNextGotCallback = false;
  callGot = false;
  readonly defaultQueryComplete = false;
  readonly #context: TContext | undefined;

  constructor({
    sources = makeSources(),
    callGot = false,
    context,
  }: {
    sources?: Record<string, Source> | undefined;
    callGot?: boolean | undefined;
    context?: TContext | undefined;
  } = {}) {
    this.#sources = sources;
    this.callGot = callGot;
    this.#context = context;
  }

  withContext<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    query: Query<TSchema, TTable, TReturn, TContext>,
  ): QueryInternals<TSchema, TTable, TReturn, TContext> {
    assert('withContext' in query);
    return (
      query as WithContext<TSchema, TTable, TReturn, TContext>
    ).withContext(this.#context as TContext);
  }

  flushQueryChanges() {}

  assertValidRunOptions(): void {}

  batchViewUpdates<T>(applyViewUpdates: () => T): T {
    return applyViewUpdates();
  }

  onTransactionCommit(listener: CommitListener): () => void {
    this.#commitListeners.add(listener);
    return () => {
      this.#commitListeners.delete(listener);
    };
  }

  mapAst(ast: AST): AST {
    return ast;
  }

  commit() {
    for (const listener of this.#commitListeners) {
      listener();
    }
  }

  addCustomQuery(
    ast: AST,
    customQueryID: CustomQueryID,
    ttl: TTL,
    gotCallback?: GotCallback | undefined,
  ): () => void {
    return this.#addQuery({ast, ttl, ...customQueryID}, gotCallback);
  }

  addServerQuery(
    ast: AST,
    ttl: TTL,
    gotCallback?: GotCallback | undefined,
  ): () => void {
    return this.#addQuery(
      {ast, name: undefined, args: undefined, ttl},
      gotCallback,
    );
  }

  #addQuery(entry: Entry, gotCallback?: GotCallback | undefined) {
    this.addedServerQueries.push(entry);
    this.gotCallbacks.push(gotCallback);
    if (this.callGot) {
      void Promise.resolve().then(() => {
        gotCallback?.(true);
      });
    } else {
      if (this.synchronouslyCallNextGotCallback) {
        this.synchronouslyCallNextGotCallback = false;
        gotCallback?.(true);
      }
    }
    return () => {};
  }

  updateServerQuery(ast: AST, ttl: TTL): void {
    const query = this.addedServerQueries.find(({ast: otherAST}) =>
      deepEqual(otherAST, ast),
    );
    assert(query);
    query.ttl = ttl;
  }

  updateCustomQuery(customQueryID: CustomQueryID, ttl: TTL): void {
    const query = this.addedServerQueries.find(
      ({name, args}) =>
        name === customQueryID.name &&
        (args === undefined || deepEqual(args, customQueryID.args)),
    );
    assert(query);
    query.ttl = ttl;
  }

  getSource(name: string): Source {
    return this.#sources[name];
  }

  createStorage() {
    return new MemoryStorage();
  }

  decorateSourceInput(input: SourceInput): Input {
    return input;
  }

  decorateInput(input: Input, _description: string): Input {
    return input;
  }

  decorateFilterInput(input: FilterInput, _description: string): FilterInput {
    return input;
  }

  addEdge() {}

  callAllGotCallbacks() {
    for (const gotCallback of this.gotCallbacks) {
      gotCallback?.(true);
    }
    this.gotCallbacks.length = 0;
  }

  addMetric() {}

  materialize<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TContext,
  >(
    query: Query<TSchema, TTable, TReturn, TContext>,
    factory?: undefined,
    options?: MaterializeOptions,
  ): TypedView<HumanReadable<TReturn>>;

  materialize<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TContext,
    T,
  >(
    query: Query<TSchema, TTable, TReturn, TContext>,
    factory: ViewFactory<TSchema, TTable, TReturn, TContext, T>,
    options?: MaterializeOptions,
  ): T;

  materialize<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    T,
  >(
    query: Query<TSchema, TTable, TReturn, TContext>,
    factory?: ViewFactory<TSchema, TTable, TReturn, TContext, T>,
    options?: MaterializeOptions,
  ): T {
    return materializeImpl(query, this, factory, options);
  }

  run<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    query: Query<TSchema, TTable, TReturn, TContext>,
    options?: RunOptions,
  ): Promise<HumanReadable<TReturn>> {
    return runImpl(query, this, options);
  }

  preload<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    query: Query<TSchema, TTable, TReturn, TContext>,
    options?: PreloadOptions,
  ): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    return preloadImpl(query, this, options);
  }
}

function makeSources() {
  const {user, issue, comment, revision, label, issueLabel} = {
    user: userSchema,
    issue: issueSchema,
    comment: commentSchema,
    revision: revisionSchema,
    label: labelSchema,
    issueLabel: issueLabelSchema,
  };

  return {
    user: createSource(
      lc,
      testLogConfig,
      'user',
      user.columns,
      user.primaryKey,
    ),
    issue: createSource(
      lc,
      testLogConfig,
      'issue',
      issue.columns,
      issue.primaryKey,
    ),
    comment: createSource(
      lc,
      testLogConfig,
      'comment',
      comment.columns,
      comment.primaryKey,
    ),
    revision: createSource(
      lc,
      testLogConfig,
      'revision',
      revision.columns,
      revision.primaryKey,
    ),
    label: createSource(
      lc,
      testLogConfig,
      'label',
      label.columns,
      label.primaryKey,
    ),
    issueLabel: createSource(
      lc,
      testLogConfig,
      'issueLabel',
      issueLabel.columns,
      issueLabel.primaryKey,
    ),
  };
}
