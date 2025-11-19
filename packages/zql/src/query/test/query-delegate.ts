import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {
  deepEqual,
  type ReadonlyJSONValue,
} from '../../../../shared/src/json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Source} from '../../ivm/source.ts';
import {createSource} from '../../ivm/test/source-factory.ts';
import type {CustomQueryID} from '../named.ts';
import {QueryDelegateBase} from '../query-delegate-base.ts';
import type {CommitListener, GotCallback} from '../query-delegate.ts';
import type {TTL} from '../ttl.ts';
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
export class QueryDelegateImpl<TContext = undefined> extends QueryDelegateBase {
  readonly #sources: Record<string, Source> = makeSources();
  readonly #commitListeners: Set<CommitListener> = new Set();

  readonly addedServerQueries: Entry[] = [];
  readonly gotCallbacks: (GotCallback | undefined)[] = [];
  synchronouslyCallNextGotCallback = false;
  callGot = false;
  readonly defaultQueryComplete = false;
  readonly enableNotExists = true; // Allow NOT EXISTS in tests

  constructor({
    sources = makeSources(),
    callGot = false,
  }: {
    sources?: Record<string, Source> | undefined;
    callGot?: boolean | undefined;
    context?: TContext | undefined;
  } = {}) {
    super();
    this.#sources = sources;
    this.callGot = callGot;
  }

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
    gotCallback?: GotCallback,
  ): () => void {
    return this.#addQuery({ast, ttl, ...customQueryID}, gotCallback);
  }

  addServerQuery(ast: AST, ttl: TTL, gotCallback?: GotCallback): () => void {
    return this.#addQuery(
      {ast, name: undefined, args: undefined, ttl},
      gotCallback,
    );
  }

  #addQuery(entry: Entry, gotCallback?: GotCallback) {
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

  callAllGotCallbacks() {
    for (const gotCallback of this.gotCallbacks) {
      gotCallback?.(true);
    }
    this.gotCallbacks.length = 0;
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
