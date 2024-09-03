import {deepClone} from 'shared/src/deep-clone.js';
import {describe, expect, test} from 'vitest';
import {ChangeType} from '../ivm/change.js';
import {MemorySource} from '../ivm/memory-source.js';
import {MemoryStorage} from '../ivm/memory-storage.js';
import {Storage} from '../ivm/operator.js';
import {Source} from '../ivm/source.js';
import {CommitListener, newQuery, QueryDelegate} from './query-impl.js';
import {
  commentSchema,
  issueLabelSchema,
  issueSchema,
  labelSchema,
  revisionSchema,
  userSchema,
} from './test/testSchemas.js';

export class QueryDelegateImpl implements QueryDelegate {
  #sources: Record<string, Source> = makeSources();
  #commitListeners: Set<CommitListener> = new Set();

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

  addServerQuery(): () => void {
    return () => {};
  }
  getSource(name: string): Source {
    return this.#sources[name];
  }
  createStorage(): Storage {
    return new MemoryStorage();
  }
}

/**
 * Some basic manual tests to get us started.
 *
 * We'll want to implement a "dumb query runner" then
 * 1. generate queries with something like fast-check
 * 2. generate a script of mutations
 * 3. run the queries and mutations against the dumb query runner
 * 4. run the queries and mutations against the real query runner
 * 5. compare the results
 *
 * The idea being there's little to no bugs in the dumb runner
 * and the generative testing will cover more than we can possibly
 * write by hand.
 */

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

function addData(queryDelegate: QueryDelegate) {
  queryDelegate.getSource('user').push({
    type: ChangeType.Add,
    row: {
      id: '0001',
      name: 'Alice',
    },
  });
  queryDelegate.getSource('user').push({
    type: ChangeType.Add,
    row: {
      id: '0002',
      name: 'Bob',
    },
  });
  queryDelegate.getSource('issue').push({
    type: ChangeType.Add,
    row: {
      id: '0001',
      title: 'issue 1',
      description: 'description 1',
      closed: false,
      ownerId: '0001',
    },
  });
  queryDelegate.getSource('issue').push({
    type: ChangeType.Add,
    row: {
      id: '0002',
      title: 'issue 2',
      description: 'description 2',
      closed: false,
      ownerId: '0002',
    },
  });

  queryDelegate.getSource('comment').push({
    type: ChangeType.Add,
    row: {
      id: '0001',
      authorId: '0001',
      issueId: '0001',
      body: 'comment 1',
    },
  });
  queryDelegate.getSource('comment').push({
    type: ChangeType.Add,
    row: {
      id: '0002',
      authorId: '0002',
      issueId: '0001',
      body: 'comment 2',
    },
  });
  queryDelegate.getSource('revision').push({
    type: ChangeType.Add,
    row: {
      id: '0001',
      authorId: '0001',
      commentId: '0001',
      text: 'revision 1',
    },
  });

  queryDelegate.getSource('label').push({
    type: ChangeType.Add,
    row: {
      id: '0001',
      name: 'label 1',
    },
  });
  queryDelegate.getSource('issueLabel').push({
    type: ChangeType.Add,
    row: {
      issueId: '0001',
      labelId: '0001',
    },
  });
}

describe('bare select', () => {
  test('empty source', () => {
    const queryDelegate = new QueryDelegateImpl();
    const issueQuery = newQuery(queryDelegate, issueSchema).select('id');
    const m = issueQuery.materialize();
    m.hydrate();

    let rows: readonly unknown[] = [];
    let called = false;
    m.addListener(data => {
      called = true;
      rows = deepClone(data) as unknown[];
    });

    expect(called).toBe(true);
    expect(rows).toEqual([]);

    called = false;
    m.addListener(_ => {
      called = true;
    });
    expect(called).toBe(true);
  });

  test('empty source followed by changes', () => {
    const queryDelegate = new QueryDelegateImpl();
    const issueQuery = newQuery(queryDelegate, issueSchema).select('id');
    const m = issueQuery.materialize();
    m.hydrate();

    let rows: unknown[] = [];
    m.addListener(data => {
      rows = deepClone(data) as unknown[];
    });

    expect(rows).toEqual([]);

    queryDelegate.getSource('issue').push({
      type: ChangeType.Add,
      row: {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
      },
    });
    queryDelegate.commit();

    expect(rows).toEqual([
      {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
      },
    ]);

    queryDelegate.getSource('issue').push({
      type: ChangeType.Remove,
      row: {
        id: '0001',
      },
    });
    queryDelegate.commit();

    expect(rows).toEqual([]);
  });

  test('source with initial data', () => {
    const queryDelegate = new QueryDelegateImpl();
    queryDelegate.getSource('issue').push({
      type: ChangeType.Add,
      row: {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
      },
    });

    const issueQuery = newQuery(queryDelegate, issueSchema).select('id');
    const m = issueQuery.materialize();
    m.hydrate();

    let rows: unknown[] = [];
    m.addListener(data => {
      rows = deepClone(data) as unknown[];
    });

    expect(rows).toEqual([
      {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
      },
    ]);
  });

  test('source with initial data followed by changes', () => {
    const queryDelegate = new QueryDelegateImpl();

    queryDelegate.getSource('issue').push({
      type: ChangeType.Add,
      row: {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
      },
    });

    const issueQuery = newQuery(queryDelegate, issueSchema).select('id');
    const m = issueQuery.materialize();
    m.hydrate();

    let rows: unknown[] = [];
    m.addListener(data => {
      rows = deepClone(data) as unknown[];
    });

    expect(rows).toEqual([
      {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
      },
    ]);

    queryDelegate.getSource('issue').push({
      type: ChangeType.Add,
      row: {
        id: '0002',
        title: 'title2',
        description: 'description2',
        closed: false,
        ownerId: '0002',
      },
    });
    queryDelegate.commit();

    expect(rows).toEqual([
      {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
      },
      {
        id: '0002',
        title: 'title2',
        description: 'description2',
        closed: false,
        ownerId: '0002',
      },
    ]);
  });
});

describe('joins and filters', () => {
  test('filter', () => {
    const queryDelegate = new QueryDelegateImpl();
    addData(queryDelegate);

    const issueQuery = newQuery(queryDelegate, issueSchema)
      .select('id')
      .where('title', '=', 'issue 1');

    const singleFilterView = issueQuery.materialize();
    singleFilterView.hydrate();
    let singleFilterRows: {id: string}[] = [];
    let doubleFilterRows: {id: string}[] = [];
    let doubleFilterWithNoResultsRows: {id: string}[] = [];
    const doubleFilterView = issueQuery
      .where('closed', '=', false)
      .materialize();
    doubleFilterView.hydrate();
    const doubleFilterViewWithNoResults = issueQuery
      .where('closed', '=', true)
      .materialize();
    doubleFilterViewWithNoResults.hydrate();

    singleFilterView.addListener(data => {
      singleFilterRows = deepClone(data) as {id: string}[];
    });
    doubleFilterView.addListener(data => {
      doubleFilterRows = deepClone(data) as {id: string}[];
    });
    doubleFilterViewWithNoResults.addListener(data => {
      doubleFilterWithNoResultsRows = deepClone(data) as {id: string}[];
    });

    expect(singleFilterRows.map(r => r.id)).toEqual(['0001']);
    expect(doubleFilterRows.map(r => r.id)).toEqual(['0001']);
    expect(doubleFilterWithNoResultsRows).toEqual([]);

    queryDelegate.getSource('issue').push({
      type: ChangeType.Remove,
      row: {
        id: '0001',
        title: 'issue 1',
        description: 'description 1',
        closed: false,
        ownerId: '0001',
      },
    });
    queryDelegate.commit();

    expect(singleFilterRows).toEqual([]);
    expect(doubleFilterRows).toEqual([]);
    expect(doubleFilterWithNoResultsRows).toEqual([]);

    queryDelegate.getSource('issue').push({
      type: ChangeType.Add,
      row: {
        id: '0001',
        title: 'issue 1',
        description: 'description 1',
        closed: true,
        ownerId: '0001',
      },
    });

    // no commit
    expect(singleFilterRows).toEqual([]);
    expect(doubleFilterRows).toEqual([]);
    expect(doubleFilterWithNoResultsRows).toEqual([]);

    queryDelegate.commit();

    expect(singleFilterRows.map(r => r.id)).toEqual(['0001']);
    expect(doubleFilterRows).toEqual([]);
    // has results since we changed closed to true in the mutation
    expect(doubleFilterWithNoResultsRows.map(r => r.id)).toEqual(['0001']);
  });

  test('join', () => {
    const queryDelegate = new QueryDelegateImpl();
    addData(queryDelegate);

    const issueQuery = newQuery(queryDelegate, issueSchema)
      .related('labels', q => q.select('name'))
      .related('owner', q => q.select('name'))
      .related('comments', q => q.select('text'))
      .select('id');
    const view = issueQuery.materialize();
    view.hydrate();

    let rows: unknown[] = [];
    view.addListener(data => {
      rows = deepClone(data) as unknown[];
    });

    expect(rows).toEqual([
      {
        closed: false,
        comments: [
          {
            authorId: '0001',
            body: 'comment 1',
            id: '0001',
            issueId: '0001',
          },
          {
            authorId: '0002',
            body: 'comment 2',
            id: '0002',
            issueId: '0001',
          },
        ],
        description: 'description 1',
        id: '0001',
        labels: [
          {
            id: '0001',
            name: 'label 1',
          },
        ],
        owner: [
          {
            id: '0001',
            name: 'Alice',
          },
        ],
        ownerId: '0001',
        title: 'issue 1',
      },
      {
        closed: false,
        comments: [],
        description: 'description 2',
        id: '0002',
        labels: [],
        owner: [
          {
            id: '0002',
            name: 'Bob',
          },
        ],
        ownerId: '0002',
        title: 'issue 2',
      },
    ]);

    queryDelegate.getSource('issue').push({
      type: ChangeType.Remove,
      row: {
        id: '0001',
        title: 'issue 1',
        description: 'description 1',
        closed: false,
        ownerId: '0001',
      },
    });
    queryDelegate.getSource('issue').push({
      type: ChangeType.Remove,
      row: {
        id: '0002',
        title: 'issue 2',
        description: 'description 2',
        closed: false,
        ownerId: '0002',
      },
    });
    queryDelegate.commit();

    expect(rows).toEqual([]);
  });
});

test('limit -1', () => {
  const queryDelegate = new QueryDelegateImpl();
  expect(() => {
    newQuery(queryDelegate, issueSchema).limit(-1);
  }).toThrow('Limit must be non-negative');
});

test('non int limit', () => {
  const queryDelegate = new QueryDelegateImpl();
  expect(() => {
    newQuery(queryDelegate, issueSchema).limit(1.5);
  }).toThrow('Limit must be an integer');
});
