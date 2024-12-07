import type {AST} from '../../../../zero-protocol/src/ast.js';
import {normalizeTables} from '../../../../zero-schema/src/normalize-table-schema.js';
import {MemorySource} from '../../ivm/memory-source.js';
import {MemoryStorage} from '../../ivm/memory-storage.js';
import type {Source} from '../../ivm/source.js';
import type {
  CommitListener,
  GotCallback,
  QueryDelegate,
} from '../query-impl.js';
import {
  commentSchema,
  issueLabelSchema,
  issueSchema,
  labelSchema,
  revisionSchema,
  userSchema,
} from './testSchemas.js';

export class QueryDelegateImpl implements QueryDelegate {
  readonly #sources: Record<string, Source> = makeSources();
  readonly #commitListeners: Set<CommitListener> = new Set();

  readonly addedServerQueries: AST[] = [];
  readonly gotCallbacks: (GotCallback | undefined)[] = [];
  synchronouslyCallNextGotCallback = false;

  constructor(sources?: Record<string, Source>) {
    this.#sources = sources ?? makeSources();
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

  commit() {
    for (const listener of this.#commitListeners) {
      listener();
    }
  }
  addServerQuery(ast: AST, gotCallback?: GotCallback | undefined): () => void {
    this.addedServerQueries.push(ast);
    this.gotCallbacks.push(gotCallback);
    if (this.synchronouslyCallNextGotCallback) {
      this.synchronouslyCallNextGotCallback = false;
      gotCallback?.(true);
    }
    return () => {};
  }
  getSource(name: string): Source {
    return this.#sources[name];
  }
  createStorage() {
    return new MemoryStorage();
  }
}

function makeSources() {
  const {user, issue, comment, revision, label, issueLabel} = normalizeTables({
    user: userSchema,
    issue: issueSchema,
    comment: commentSchema,
    revision: revisionSchema,
    label: labelSchema,
    issueLabel: issueLabelSchema,
  });

  return {
    user: new MemorySource('user', user.columns, user.primaryKey),
    issue: new MemorySource('issue', issue.columns, issue.primaryKey),
    comment: new MemorySource('comment', comment.columns, comment.primaryKey),
    revision: new MemorySource(
      'revision',
      revision.columns,
      revision.primaryKey,
    ),
    label: new MemorySource('label', label.columns, label.primaryKey),
    issueLabel: new MemorySource(
      'issueLabel',
      issueLabel.columns,
      issueLabel.primaryKey,
    ),
  };
}
