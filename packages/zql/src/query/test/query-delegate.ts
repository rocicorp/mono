import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {
  deepEqual,
  type ReadonlyJSONValue,
} from '../../../../shared/src/json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {FilterInput} from '../../ivm/filter-operators.ts';
import {MemoryStorage} from '../../ivm/memory-storage.ts';
import type {Input} from '../../ivm/operator.ts';
import type {Source} from '../../ivm/source.ts';
import {createSource} from '../../ivm/test/source-factory.ts';
import type {CustomQueryID} from '../named.ts';
import {
  type CommitListener,
  type GotCallback,
  type QueryDelegate,
} from '../query-impl.ts';
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
export class QueryDelegateImpl implements QueryDelegate {
  readonly #sources: Record<string, Source> = makeSources();
  readonly #commitListeners: Set<CommitListener> = new Set();

  readonly addedServerQueries: Entry[] = [];
  readonly gotCallbacks: (GotCallback | undefined)[] = [];
  synchronouslyCallNextGotCallback = false;
  callGot = false;
  readonly defaultQueryComplete = false;

  constructor({
    sources = makeSources(),
    callGot = false,
  }: {
    sources?: Record<string, Source> | undefined;
    callGot?: boolean | undefined;
  } = {}) {
    this.#sources = sources;
    this.callGot = callGot;
  }

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

  onQueryMaterialized() {}

  commit() {
    for (const listener of this.#commitListeners) {
      listener();
    }
  }

  addCustomQuery(
    customQueryID: CustomQueryID,
    ttl: TTL,
    gotCallback?: GotCallback | undefined,
  ): () => void {
    return this.#addQuery({ast: undefined, ttl, ...customQueryID}, gotCallback);
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

  getSource(name: string): Source {
    return this.#sources[name];
  }

  createStorage() {
    return new MemoryStorage();
  }

  decorateInput(input: Input, _description: string): Input {
    return input;
  }

  decorateFilterInput(input: FilterInput, _description: string): FilterInput {
    return input;
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
