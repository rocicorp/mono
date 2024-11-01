import type {AST} from '../../../../zero-protocol/src/ast.js';
import {MemorySource} from '../../ivm/memory-source.js';
import {MemoryStorage} from '../../ivm/memory-storage.js';
import type {Source} from '../../ivm/source.js';
import type {CommitListener, QueryDelegate} from '../query-impl.js';
import {
  commentSchema,
  issueLabelSchema,
  issueSchema,
  labelSchema,
  revisionSchema,
  userSchema,
} from './testSchemas.js';

export class QueryDelegateImpl implements QueryDelegate {
  #sources: Record<string, Source> = makeSources();
  #commitListeners: Set<CommitListener> = new Set();

  addedServerQueries: AST[] = [];

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
  addServerQuery(ast: AST): () => void {
    this.addedServerQueries.push(ast);
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
  const userArgs = userSchema;
  const issueArgs = issueSchema;
  const commentArgs = commentSchema;
  const revisionArgs = revisionSchema;
  const labelArgs = labelSchema;
  const issueLabelArgs = issueLabelSchema;
  return {
    user: new MemorySource('user', userArgs.columns, userArgs.primaryKey),
    issue: new MemorySource('issue', issueArgs.columns, issueArgs.primaryKey),
    comment: new MemorySource(
      'comment',
      commentArgs.columns,
      commentArgs.primaryKey,
    ),
    revision: new MemorySource(
      'revision',
      revisionArgs.columns,
      revisionArgs.primaryKey,
    ),
    label: new MemorySource('label', labelArgs.columns, labelArgs.primaryKey),
    issueLabel: new MemorySource(
      'issueLabel',
      issueLabelArgs.columns,
      issueLabelArgs.primaryKey,
    ),
  };
}
