import {describe, expect, test} from 'vitest';
import {newQuery} from './query-impl.js';
import {MemoryStorage} from '../ivm2/memory-storage.js';
import {MemorySource} from '../ivm2/memory-source.js';
import {
  commentSchema,
  issueLabelSchema,
  issueSchema,
  labelSchema,
  revisionSchema,
  userSchema,
} from './test/testSchemas.js';
import {toInputArgs} from './schema.js';
import {must} from 'shared/src/must.js';
import {Host} from '../builder/builder.js';
import {TypedView} from './typed-view.js';

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
  const userArgs = toInputArgs(userSchema);
  const issueArgs = toInputArgs(issueSchema);
  const commentArgs = toInputArgs(commentSchema);
  const revisionArgs = toInputArgs(revisionSchema);
  const labelArgs = toInputArgs(labelSchema);
  const issueLabelArgs = toInputArgs(issueLabelSchema);
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

function addData(host: Host) {
  host.getSource('user').push({
    type: 'add',
    row: {
      id: '0001',
      name: 'Alice',
    },
  });
  host.getSource('user').push({
    type: 'add',
    row: {
      id: '0002',
      name: 'Bob',
    },
  });
  host.getSource('issue').push({
    type: 'add',
    row: {
      id: '0001',
      title: 'issue 1',
      description: 'description 1',
      closed: false,
      ownerId: '0001',
    },
  });
  host.getSource('issue').push({
    type: 'add',
    row: {
      id: '0002',
      title: 'issue 2',
      description: 'description 2',
      closed: false,
      ownerId: '0002',
    },
  });

  host.getSource('comment').push({
    type: 'add',
    row: {
      id: '0001',
      authorId: '0001',
      issueId: '0001',
      body: 'comment 1',
    },
  });
  host.getSource('comment').push({
    type: 'add',
    row: {
      id: '0002',
      authorId: '0002',
      issueId: '0001',
      body: 'comment 2',
    },
  });
  host.getSource('revision').push({
    type: 'add',
    row: {
      id: '0001',
      authorId: '0001',
      commentId: '0001',
      text: 'revision 1',
    },
  });

  host.getSource('label').push({
    type: 'add',
    row: {
      id: '0001',
      name: 'label 1',
    },
  });
  host.getSource('issueLabel').push({
    type: 'add',
    row: {
      issueId: '0001',
      labelId: '0001',
    },
  });
}

function makeHost() {
  const sources = makeSources();
  return {
    getSource(tableName: string) {
      return must(sources[tableName as keyof typeof sources]);
    },
    createStorage() {
      return new MemoryStorage();
    },
  };
}

describe('bare select', () => {
  test('empty source', () => {
    const host = makeHost();
    const issueQuery = newQuery(host, issueSchema).select('id');
    const view = issueQuery.materialize();

    let rows: {id: string}[] = [];
    let called = false;
    view.addListener(data => {
      called = true;
      rows = [...data];
    });

    view.hydrate();

    expect(called).toBe(true);
    expect(rows).toEqual([]);

    called = false;
    view.addListener(_ => {
      called = true;
    });
    // Hmm.. we probably want a `wantInitialData` sort of API
    // for late comers to a view.
    expect(called).toBe(false);
  });

  test('empty source followed by changes', () => {
    const host = makeHost();
    const issueQuery = newQuery(host, issueSchema).select('id');
    const view = issueQuery.materialize();

    let rows: {id: string}[] = [];
    view.addListener(data => {
      rows = [...data];
    });
    view.hydrate();

    expect(rows).toEqual([]);

    host.getSource('issue').push({
      type: 'add',
      row: {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
      },
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

    host.getSource('issue').push({
      type: 'remove',
      row: {
        id: '0001',
      },
    });

    expect(rows).toEqual([]);
  });

  test('source with initial data', () => {
    const host = makeHost();
    host.getSource('issue').push({
      type: 'add',
      row: {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
      },
    });

    const issueQuery = newQuery(host, issueSchema).select('id');
    const view = issueQuery.materialize();

    let rows: {id: string}[] = [];
    view.addListener(data => {
      rows = [...data];
    });
    view.hydrate();

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
    const host = makeHost();

    host.getSource('issue').push({
      type: 'add',
      row: {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
      },
    });

    const issueQuery = newQuery(host, issueSchema).select('id');
    const view = issueQuery.materialize();

    let rows: {id: string}[] = [];
    view.addListener(data => {
      rows = [...data];
    });
    view.hydrate();

    expect(rows).toEqual([
      {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
      },
    ]);

    host.getSource('issue').push({
      type: 'add',
      row: {
        id: '0002',
        title: 'title2',
        description: 'description2',
        closed: false,
        ownerId: '0002',
      },
    });

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
    const host = makeHost();
    addData(host);

    const issueQuery = newQuery(host, issueSchema)
      .select('id')
      .where('title', '=', 'issue 1');

    const singleFilterView = issueQuery.materialize();
    let singleFilterRows: {id: string}[] = [];
    let doubleFilterRows: {id: string}[] = [];
    let doubleFilterWithNoResultsRows: {id: string}[] = [];
    const doubleFilterView = issueQuery
      .where('closed', '=', false)
      .materialize();
    const doubleFilterViewWithNoResults = issueQuery
      .where('closed', '=', true)
      .materialize();

    singleFilterView.addListener(data => {
      singleFilterRows = [...data];
    });
    doubleFilterView.addListener(data => {
      doubleFilterRows = [...data];
    });
    doubleFilterViewWithNoResults.addListener(data => {
      doubleFilterWithNoResultsRows = [...data];
    });

    singleFilterView.hydrate();
    doubleFilterView.hydrate();
    doubleFilterViewWithNoResults.hydrate();

    expect(singleFilterRows.map(r => r.id)).toEqual(['0001']);
    expect(doubleFilterRows.map(r => r.id)).toEqual(['0001']);
    expect(doubleFilterWithNoResultsRows).toEqual([]);

    host.getSource('issue').push({
      type: 'remove',
      row: {
        id: '0001',
        title: 'issue 1',
        description: 'description 1',
        closed: false,
        ownerId: '0001',
      },
    });

    expect(singleFilterRows).toEqual([]);
    expect(doubleFilterRows).toEqual([]);
    expect(doubleFilterWithNoResultsRows).toEqual([]);

    host.getSource('issue').push({
      type: 'add',
      row: {
        id: '0001',
        title: 'issue 1',
        description: 'description 1',
        closed: true,
        ownerId: '0001',
      },
    });

    expect(singleFilterRows.map(r => r.id)).toEqual(['0001']);
    expect(doubleFilterRows).toEqual([]);
    // has results since we changed closed to true in the mutation
    expect(doubleFilterWithNoResultsRows.map(r => r.id)).toEqual(['0001']);
  });

  test('join', () => {
    const host = makeHost();
    addData(host);

    const issueQuery = newQuery(host, issueSchema)
      .related('labels', q => q.select('name'))
      .related('owner', q => q.select('name'))
      .related('comments', q => q.select('text'))
      .select('id');
    const view = issueQuery.materialize();

    let rows: typeof view extends TypedView<infer TRow> ? TRow : never[] = [];
    view.addListener(data => {
      rows = [...data].map(row => ({
        ...row,
        owner: [...row.owner],
        labels: [...row.labels].map(label => ({
          ...label,
          // The type of `rows` when going through a junction edge
          // is incorrect. The TS Type is hiding the junction table,
          // the actual data has it. Need to pick which one is correct
          // and fix.
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          labels: [...(label as any).labels],
        })),
        comments: [...row.comments],
      }));
    });

    view.hydrate();

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
            issueId: '0001',
            labelId: '0001',
            labels: [
              {
                id: '0001',
                name: 'label 1',
              },
            ],
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

    host.getSource('issue').push({
      type: 'remove',
      row: {
        id: '0001',
        title: 'issue 1',
        description: 'description 1',
        closed: false,
        ownerId: '0001',
      },
    });
    host.getSource('issue').push({
      type: 'remove',
      row: {
        id: '0002',
        title: 'issue 2',
        description: 'description 2',
        closed: false,
        ownerId: '0002',
      },
    });

    expect(rows).toEqual([]);
  });
});
