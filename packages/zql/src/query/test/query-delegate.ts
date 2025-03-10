import type {LogConfig} from '../../../../otel/src/log-options.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {MemoryStorage} from '../../ivm/memory-storage.ts';
import type {Input} from '../../ivm/operator.ts';
import type {Source} from '../../ivm/source.ts';
import {createSource} from '../../ivm/test/source-factory.ts';
import type {
  CommitListener,
  GotCallback,
  QueryDelegate,
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
const logConfig: LogConfig = {
  format: 'text',
  level: 'debug',
  ivmSampling: 0,
  slowRowThreshold: 0,
};

export class QueryDelegateImpl implements QueryDelegate {
  readonly #sources: Record<string, Source> = makeSources();
  readonly #commitListeners: Set<CommitListener> = new Set();

  readonly addedServerQueries: {ast: AST; ttl: TTL}[] = [];
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
  addServerQuery(
    ast: AST,
    ttl: TTL,
    gotCallback?: GotCallback | undefined,
  ): () => void {
    this.addedServerQueries.push({ast, ttl});
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
  decorateInput(input: Input, _description: string): Input {
    return input;
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
    user: createSource(lc, logConfig, 'user', user.columns, user.primaryKey),
    issue: createSource(
      lc,
      logConfig,
      'issue',
      issue.columns,
      issue.primaryKey,
    ),
    comment: createSource(
      lc,
      logConfig,
      'comment',
      comment.columns,
      comment.primaryKey,
    ),
    revision: createSource(
      lc,
      logConfig,
      'revision',
      revision.columns,
      revision.primaryKey,
    ),
    label: createSource(
      lc,
      logConfig,
      'label',
      label.columns,
      label.primaryKey,
    ),
    issueLabel: createSource(
      lc,
      logConfig,
      'issueLabel',
      issueLabel.columns,
      issueLabel.primaryKey,
    ),
  };
}
