import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {deepEqual} from '../../../../shared/src/json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../shared/src/must.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {FilterInput} from '../../ivm/filter-operators.ts';
import {MemoryStorage} from '../../ivm/memory-storage.ts';
import type {Input} from '../../ivm/operator.ts';
import type {Source} from '../../ivm/source.ts';
import {createSource} from '../../ivm/test/source-factory.ts';
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

export class QueryDelegateImpl implements QueryDelegate {
  readonly #sources: Record<string, Source> = makeSources();
  readonly #commitListeners: Set<CommitListener> = new Set();

  readonly addedServerQueries: {ast: AST; ttl: TTL}[] = [];
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

  addServerQuery(
    ast: AST,
    ttl: TTL,
    gotCallback?: GotCallback | undefined,
  ): () => void {
    this.addedServerQueries.push({ast, ttl});
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

export function addData(queryDelegate: QueryDelegate) {
  const userSource = must(queryDelegate.getSource('user'));
  const issueSource = must(queryDelegate.getSource('issue'));
  const commentSource = must(queryDelegate.getSource('comment'));
  const revisionSource = must(queryDelegate.getSource('revision'));
  const labelSource = must(queryDelegate.getSource('label'));
  const issueLabelSource = must(queryDelegate.getSource('issueLabel'));
  userSource.push({
    type: 'add',
    row: {
      id: '0001',
      name: 'Alice',
      metadata: {
        registrar: 'github',
        login: 'alicegh',
      },
    },
  });
  userSource.push({
    type: 'add',
    row: {
      id: '0002',
      name: 'Bob',
      metadata: {
        registar: 'google',
        login: 'bob@gmail.com',
        altContacts: ['bobwave', 'bobyt', 'bobplus'],
      },
    },
  });
  issueSource.push({
    type: 'add',
    row: {
      id: '0001',
      title: 'issue 1',
      description: 'description 1',
      closed: false,
      ownerId: '0001',
      createdAt: 1,
    },
  });
  issueSource.push({
    type: 'add',
    row: {
      id: '0002',
      title: 'issue 2',
      description: 'description 2',
      closed: false,
      ownerId: '0002',
      createdAt: 2,
    },
  });
  issueSource.push({
    type: 'add',
    row: {
      id: '0003',
      title: 'issue 3',
      description: 'description 3',
      closed: false,
      ownerId: null,
      createdAt: 3,
    },
  });
  commentSource.push({
    type: 'add',
    row: {
      id: '0001',
      authorId: '0001',
      issueId: '0001',
      text: 'comment 1',
      createdAt: 1,
    },
  });
  commentSource.push({
    type: 'add',
    row: {
      id: '0002',
      authorId: '0002',
      issueId: '0001',
      text: 'comment 2',
      createdAt: 2,
    },
  });
  revisionSource.push({
    type: 'add',
    row: {
      id: '0001',
      authorId: '0001',
      commentId: '0001',
      text: 'revision 1',
    },
  });

  labelSource.push({
    type: 'add',
    row: {
      id: '0001',
      name: 'label 1',
    },
  });
  issueLabelSource.push({
    type: 'add',
    row: {
      issueId: '0001',
      labelId: '0001',
    },
  });

  return {
    userSource,
    issueSource,
    commentSource,
    revisionSource,
    labelSource,
    issueLabelSource,
  };
}
