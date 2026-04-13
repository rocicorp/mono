import {describe, expect, test, vi} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {deepClone} from '../../../shared/src/deep-clone.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {number, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {consume} from '../ivm/stream.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import type {QueryDelegate} from './query-delegate.ts';
import {newQuery} from './query-impl.ts';
import {asQueryInternals} from './query-internals.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';
import {schema} from './test/test-schemas.ts';
import type {TypedView} from './typed-view.ts';

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

const lc = createSilentLogContext();

function addData(queryDelegate: QueryDelegate) {
  const userSource = must(queryDelegate.getSource('user'));
  const issueSource = must(queryDelegate.getSource('issue'));
  const commentSource = must(queryDelegate.getSource('comment'));
  const revisionSource = must(queryDelegate.getSource('revision'));
  const labelSource = must(queryDelegate.getSource('label'));
  const issueLabelSource = must(queryDelegate.getSource('issueLabel'));
  consume(
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
    }),
  );
  consume(
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
    }),
  );
  consume(
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
    }),
  );
  consume(
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
    }),
  );
  consume(
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
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '0001',
        authorId: '0001',
        issueId: '0001',
        text: 'comment 1',
        createdAt: 1,
      },
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '0002',
        authorId: '0002',
        issueId: '0001',
        text: 'comment 2',
        createdAt: 2,
      },
    }),
  );
  consume(
    revisionSource.push({
      type: 'add',
      row: {
        id: '0001',
        authorId: '0001',
        commentId: '0001',
        text: 'revision 1',
      },
    }),
  );

  consume(
    labelSource.push({
      type: 'add',
      row: {
        id: '0001',
        name: 'label 1',
      },
    }),
  );
  consume(
    issueLabelSource.push({
      type: 'add',
      row: {
        issueId: '0001',
        labelId: '0001',
      },
    }),
  );

  return {
    userSource,
    issueSource,
    commentSource,
    revisionSource,
    labelSource,
    issueLabelSource,
  };
}

describe('bare select', () => {
  test('empty source', () => {
    const queryDelegate = new QueryDelegateImpl();
    const issueQuery = newQuery(schema, 'issue');
    const m: TypedView<unknown[]> = queryDelegate.materialize(issueQuery);

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
    const issueQuery = newQuery(schema, 'issue');
    const m: TypedView<unknown[]> = queryDelegate.materialize(issueQuery);

    let rows: unknown[] = [];
    m.addListener(data => {
      rows = deepClone(data) as unknown[];
    });

    expect(rows).toEqual([]);

    consume(
      queryDelegate.getSource('issue').push({
        type: 'add',
        row: {
          id: '0001',
          title: 'title',
          description: 'description',
          closed: false,
          ownerId: '0001',
        },
      }),
    );
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

    consume(
      queryDelegate.getSource('issue').push({
        type: 'remove',
        row: {
          id: '0001',
        },
      }),
    );
    queryDelegate.commit();

    expect(rows).toEqual([]);
  });

  test('source with initial data', () => {
    const queryDelegate = new QueryDelegateImpl();
    consume(
      queryDelegate.getSource('issue').push({
        type: 'add',
        row: {
          id: '0001',
          title: 'title',
          description: 'description',
          closed: false,
          ownerId: '0001',
          createdAt: 10,
        },
      }),
    );

    const issueQuery = newQuery(schema, 'issue');
    const m: TypedView<unknown[]> = queryDelegate.materialize(issueQuery);

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
        createdAt: 10,
      },
    ]);
  });

  test('source with initial data followed by changes', () => {
    const queryDelegate = new QueryDelegateImpl();

    consume(
      queryDelegate.getSource('issue').push({
        type: 'add',
        row: {
          id: '0001',
          title: 'title',
          description: 'description',
          closed: false,
          ownerId: '0001',
          createdAt: 10,
        },
      }),
    );

    const issueQuery = newQuery(schema, 'issue');
    const m: TypedView<unknown[]> = queryDelegate.materialize(issueQuery);

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
        createdAt: 10,
      },
    ]);

    consume(
      queryDelegate.getSource('issue').push({
        type: 'add',
        row: {
          id: '0002',
          title: 'title2',
          description: 'description2',
          closed: false,
          ownerId: '0002',
          createdAt: 20,
        },
      }),
    );
    queryDelegate.commit();

    expect(rows).toEqual([
      {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
        createdAt: 10,
      },
      {
        id: '0002',
        title: 'title2',
        description: 'description2',
        closed: false,
        ownerId: '0002',
        createdAt: 20,
      },
    ]);
  });

  test('changes after destroy', () => {
    const queryDelegate = new QueryDelegateImpl();
    const issueQuery = newQuery(schema, 'issue');
    const m: TypedView<unknown[]> = queryDelegate.materialize(issueQuery);

    let rows: unknown[] = [];
    m.addListener(data => {
      rows = deepClone(data) as unknown[];
    });

    expect(rows).toEqual([]);

    consume(
      queryDelegate.getSource('issue').push({
        type: 'add',
        row: {
          id: '0001',
          title: 'title',
          description: 'description',
          closed: false,
          ownerId: '0001',
          createdAt: 10,
        },
      }),
    );
    queryDelegate.commit();

    expect(rows).toEqual([
      {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
        createdAt: 10,
      },
    ]);

    m.destroy();

    consume(
      queryDelegate.getSource('issue').push({
        type: 'remove',
        row: {
          id: '0001',
        },
      }),
    );
    queryDelegate.commit();

    // rows did not change
    expect(rows).toEqual([
      {
        id: '0001',
        title: 'title',
        description: 'description',
        closed: false,
        ownerId: '0001',
        createdAt: 10,
      },
    ]);
  });
});

describe('joins and filters', () => {
  test('filter', () => {
    const queryDelegate = new QueryDelegateImpl();
    addData(queryDelegate);

    const issueQuery = newQuery(schema, 'issue').where('title', '=', 'issue 1');

    const singleFilterView: TypedView<{id: string}[]> =
      queryDelegate.materialize(issueQuery);
    let singleFilterRows: {id: string}[] = [];
    let doubleFilterRows: {id: string}[] = [];
    let doubleFilterWithNoResultsRows: {id: string}[] = [];
    const doubleFilterQuery = issueQuery.where('closed', '=', false);
    const doubleFilterView: TypedView<{id: string}[]> =
      queryDelegate.materialize(doubleFilterQuery);
    const doubleFilterQueryWithNoResults = issueQuery.where(
      'closed',
      '=',
      true,
    );
    const doubleFilterViewWithNoResults: TypedView<{id: string}[]> =
      queryDelegate.materialize(doubleFilterQueryWithNoResults);

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

    consume(
      queryDelegate.getSource('issue').push({
        type: 'remove',
        row: {
          id: '0001',
          title: 'issue 1',
          description: 'description 1',
          closed: false,
          ownerId: '0001',
          createdAt: 10,
        },
      }),
    );
    queryDelegate.commit();

    expect(singleFilterRows).toEqual([]);
    expect(doubleFilterRows).toEqual([]);
    expect(doubleFilterWithNoResultsRows).toEqual([]);

    consume(
      queryDelegate.getSource('issue').push({
        type: 'add',
        row: {
          id: '0001',
          title: 'issue 1',
          description: 'description 1',
          closed: true,
          ownerId: '0001',
          createdAt: 10,
        },
      }),
    );

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

    const issueQuery = newQuery(schema, 'issue')
      .related('labels')
      .related('owner')
      .related('comments');
    const view: TypedView<unknown[]> = queryDelegate.materialize(issueQuery);

    let rows: unknown[] = [];
    view.addListener(data => {
      rows = deepClone(data) as unknown[];
    });

    expect(rows).toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "comments": [
            {
              "authorId": "0001",
              "createdAt": 1,
              "id": "0001",
              "issueId": "0001",
              "text": "comment 1",
            },
            {
              "authorId": "0002",
              "createdAt": 2,
              "id": "0002",
              "issueId": "0001",
              "text": "comment 2",
            },
          ],
          "createdAt": 1,
          "description": "description 1",
          "id": "0001",
          "labels": [
            {
              "id": "0001",
              "name": "label 1",
            },
          ],
          "owner": {
            "id": "0001",
            "metadata": {
              "login": "alicegh",
              "registrar": "github",
            },
            "name": "Alice",
          },
          "ownerId": "0001",
          "title": "issue 1",
        },
        {
          "closed": false,
          "comments": [],
          "createdAt": 2,
          "description": "description 2",
          "id": "0002",
          "labels": [],
          "owner": {
            "id": "0002",
            "metadata": {
              "altContacts": [
                "bobwave",
                "bobyt",
                "bobplus",
              ],
              "login": "bob@gmail.com",
              "registar": "google",
            },
            "name": "Bob",
          },
          "ownerId": "0002",
          "title": "issue 2",
        },
        {
          "closed": false,
          "comments": [],
          "createdAt": 3,
          "description": "description 3",
          "id": "0003",
          "labels": [],
          "ownerId": null,
          "title": "issue 3",
        },
      ]
    `);

    consume(
      queryDelegate.getSource('issue').push({
        type: 'remove',
        row: {
          id: '0001',
          title: 'issue 1',
          description: 'description 1',
          closed: false,
          ownerId: '0001',
          createdAt: 1,
        },
      }),
    );
    consume(
      queryDelegate.getSource('issue').push({
        type: 'remove',
        row: {
          id: '0002',
          title: 'issue 2',
          description: 'description 2',
          closed: false,
          ownerId: '0002',
          createdAt: 2,
        },
      }),
    );
    consume(
      queryDelegate.getSource('issue').push({
        type: 'remove',
        row: {
          id: '0003',
          title: 'issue 3',
          description: 'description 3',
          closed: false,
          ownerId: null,
          createdAt: 3,
        },
      }),
    );
    queryDelegate.commit();

    expect(rows).toEqual([]);
  });

  test('one', () => {
    const queryDelegate = new QueryDelegateImpl();
    addData(queryDelegate);

    const q1 = newQuery(schema, 'issue').one();
    expect(asQueryInternals(q1).format).toEqual({
      singular: true,
      relationships: {},
    });

    const q2 = newQuery(schema, 'issue')
      .one()
      .related('comments', q => q.one());
    expect(asQueryInternals(q2).format).toEqual({
      singular: true,
      relationships: {
        comments: {
          singular: true,
          relationships: {},
        },
      },
    });

    const q3 = newQuery(schema, 'issue').related('comments', q => q.one());
    expect(asQueryInternals(q3).format).toEqual({
      singular: false,
      relationships: {
        comments: {
          singular: true,
          relationships: {},
        },
      },
    });

    const q4 = newQuery(schema, 'issue')
      .related('comments', q =>
        q.one().where('id', '1').limit(20).orderBy('authorId', 'asc'),
      )
      .one()
      .where('closed', false)
      .limit(100)
      .orderBy('title', 'desc');
    expect(asQueryInternals(q4).format).toEqual({
      singular: true,
      relationships: {
        comments: {
          singular: true,
          relationships: {},
        },
      },
    });
  });

  test('schema applied one', async () => {
    const queryDelegate = new QueryDelegateImpl({callGot: true});
    addData(queryDelegate);

    const query = newQuery(schema, 'issue')
      .related('owner')
      .related('comments', q => q.related('author').related('revisions'))
      .where('id', '=', '0001');
    const data = await queryDelegate.run(query);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "comments": [
            {
              "author": {
                "id": "0001",
                "metadata": {
                  "login": "alicegh",
                  "registrar": "github",
                },
                "name": "Alice",
                Symbol(rc): 1,
              },
              "authorId": "0001",
              "createdAt": 1,
              "id": "0001",
              "issueId": "0001",
              "revisions": [
                {
                  "authorId": "0001",
                  "commentId": "0001",
                  "id": "0001",
                  "text": "revision 1",
                  Symbol(rc): 1,
                },
              ],
              "text": "comment 1",
              Symbol(rc): 1,
            },
            {
              "author": {
                "id": "0002",
                "metadata": {
                  "altContacts": [
                    "bobwave",
                    "bobyt",
                    "bobplus",
                  ],
                  "login": "bob@gmail.com",
                  "registar": "google",
                },
                "name": "Bob",
                Symbol(rc): 1,
              },
              "authorId": "0002",
              "createdAt": 2,
              "id": "0002",
              "issueId": "0001",
              "revisions": [],
              "text": "comment 2",
              Symbol(rc): 1,
            },
          ],
          "createdAt": 1,
          "description": "description 1",
          "id": "0001",
          "owner": {
            "id": "0001",
            "metadata": {
              "login": "alicegh",
              "registrar": "github",
            },
            "name": "Alice",
            Symbol(rc): 1,
          },
          "ownerId": "0001",
          "title": "issue 1",
          Symbol(rc): 1,
        },
      ]
    `);
  });
});

test('limit -1', () => {
  expect(() => {
    void newQuery(schema, 'issue').limit(-1);
  }).toThrow('Limit must be non-negative');
});

test('non int limit', () => {
  expect(() => {
    void newQuery(schema, 'issue').limit(1.5);
  }).toThrow('Limit must be an integer');
});

test('run', async () => {
  const queryDelegate = new QueryDelegateImpl();
  queryDelegate.synchronouslyCallNextGotCallback = true;
  addData(queryDelegate);

  const issueQuery1 = newQuery(schema, 'issue').where('title', '=', 'issue 1');

  const singleFilterRows = await queryDelegate.run(issueQuery1);
  queryDelegate.synchronouslyCallNextGotCallback = true;
  const doubleFilterQuery = issueQuery1.where('closed', '=', false);
  const doubleFilterRows = await queryDelegate.run(doubleFilterQuery);
  queryDelegate.synchronouslyCallNextGotCallback = true;
  const doubleFilterWithNoResultsQuery = issueQuery1.where('closed', '=', true);
  const doubleFilterWithNoResultsRows = await queryDelegate.run(
    doubleFilterWithNoResultsQuery,
  );
  expect(singleFilterRows.map(r => r.id)).toEqual(['0001']);
  expect(doubleFilterRows.map(r => r.id)).toEqual(['0001']);
  expect(doubleFilterWithNoResultsRows).toEqual([]);

  queryDelegate.synchronouslyCallNextGotCallback = true;
  const issueQuery2 = newQuery(schema, 'issue')
    .related('labels')
    .related('owner')
    .related('comments');
  const rows = await queryDelegate.run(issueQuery2);
  expect(rows).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "comments": [
          {
            "authorId": "0001",
            "createdAt": 1,
            "id": "0001",
            "issueId": "0001",
            "text": "comment 1",
            Symbol(rc): 1,
          },
          {
            "authorId": "0002",
            "createdAt": 2,
            "id": "0002",
            "issueId": "0001",
            "text": "comment 2",
            Symbol(rc): 1,
          },
        ],
        "createdAt": 1,
        "description": "description 1",
        "id": "0001",
        "labels": [
          {
            "id": "0001",
            "name": "label 1",
            Symbol(rc): 1,
          },
        ],
        "owner": {
          "id": "0001",
          "metadata": {
            "login": "alicegh",
            "registrar": "github",
          },
          "name": "Alice",
          Symbol(rc): 1,
        },
        "ownerId": "0001",
        "title": "issue 1",
        Symbol(rc): 1,
      },
      {
        "closed": false,
        "comments": [],
        "createdAt": 2,
        "description": "description 2",
        "id": "0002",
        "labels": [],
        "owner": {
          "id": "0002",
          "metadata": {
            "altContacts": [
              "bobwave",
              "bobyt",
              "bobplus",
            ],
            "login": "bob@gmail.com",
            "registar": "google",
          },
          "name": "Bob",
          Symbol(rc): 1,
        },
        "ownerId": "0002",
        "title": "issue 2",
        Symbol(rc): 1,
      },
      {
        "closed": false,
        "comments": [],
        "createdAt": 3,
        "description": "description 3",
        "id": "0003",
        "labels": [],
        "owner": undefined,
        "ownerId": null,
        "title": "issue 3",
        Symbol(rc): 1,
      },
    ]
  `);
});

// These tests would normally go into `chinook.pg.test` but for some reason
// these tests passed when run in the chinook harness. Need to figure that out next,
// especially given chinook flexes the push (add/remove/edit) paths.
describe('pk lookup optimization', () => {
  const queryDelegate = new QueryDelegateImpl();
  addData(queryDelegate);

  test('pk lookup', async () => {
    const pkQuery1 = newQuery(schema, 'issue').where('id', '=', '0001');
    expect(await queryDelegate.run(pkQuery1)).toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "createdAt": 1,
          "description": "description 1",
          "id": "0001",
          "ownerId": "0001",
          "title": "issue 1",
          Symbol(rc): 1,
        },
      ]
    `);
    const pkQuery2 = newQuery(schema, 'user').where('id', '=', '0001');
    expect(await queryDelegate.run(pkQuery2)).toMatchInlineSnapshot(`
      [
        {
          "id": "0001",
          "metadata": {
            "login": "alicegh",
            "registrar": "github",
          },
          "name": "Alice",
          Symbol(rc): 1,
        },
      ]
    `);
  });

  test('pk lookup with sort applied for whatever reason', async () => {
    const pkQuery3 = newQuery(schema, 'issue')
      .where('id', '=', '0001')
      .orderBy('id', 'desc');
    expect(await queryDelegate.run(pkQuery3)).toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "createdAt": 1,
          "description": "description 1",
          "id": "0001",
          "ownerId": "0001",
          "title": "issue 1",
          Symbol(rc): 1,
        },
      ]
    `);

    const pkQuery4 = newQuery(schema, 'user')
      .where('id', '=', '0001')
      .orderBy('name', 'desc');
    expect(await queryDelegate.run(pkQuery4)).toMatchInlineSnapshot(`
      [
        {
          "id": "0001",
          "metadata": {
            "login": "alicegh",
            "registrar": "github",
          },
          "name": "Alice",
          Symbol(rc): 1,
        },
      ]
    `);
  });

  test('related with pk constraint', async () => {
    const relatedPkQuery = newQuery(schema, 'issue')
      .where('id', '=', '0001')
      .related('comments', q => q.where('id', '=', '0001'));
    expect(await queryDelegate.run(relatedPkQuery)).toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "comments": [
            {
              "authorId": "0001",
              "createdAt": 1,
              "id": "0001",
              "issueId": "0001",
              "text": "comment 1",
              Symbol(rc): 1,
            },
          ],
          "createdAt": 1,
          "description": "description 1",
          "id": "0001",
          "ownerId": "0001",
          "title": "issue 1",
          Symbol(rc): 1,
        },
      ]
    `);
  });

  test('exists with pk constraint', async () => {
    const existsPkQuery = newQuery(schema, 'issue')
      .where('id', '=', '0001')
      .whereExists('comments', q => q.where('id', '=', '0001'));
    expect(await queryDelegate.run(existsPkQuery)).toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "createdAt": 1,
          "description": "description 1",
          "id": "0001",
          "ownerId": "0001",
          "title": "issue 1",
          Symbol(rc): 1,
        },
      ]
    `);
  });

  test('junction with pk constraint', async () => {
    const junctionPkQuery = newQuery(schema, 'issue')
      .where('id', '=', '0001')
      .related('labels', q => q.where('id', '=', '0001'));
    expect(await queryDelegate.run(junctionPkQuery)).toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "createdAt": 1,
          "description": "description 1",
          "id": "0001",
          "labels": [
            {
              "id": "0001",
              "name": "label 1",
              Symbol(rc): 1,
            },
          ],
          "ownerId": "0001",
          "title": "issue 1",
          Symbol(rc): 1,
        },
      ]
    `);
  });

  test('junction with exists with pk constraint', async () => {
    const junctionExistsPkQuery = newQuery(schema, 'issue')
      .where('id', '=', '0001')
      .whereExists('labels', q => q.where('id', '=', '0001'));
    expect(await queryDelegate.run(junctionExistsPkQuery))
      .toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "createdAt": 1,
          "description": "description 1",
          "id": "0001",
          "ownerId": "0001",
          "title": "issue 1",
          Symbol(rc): 1,
        },
      ]
    `);
  });

  test('pk constraints in or branches', async () => {
    const pkOrQuery = newQuery(schema, 'issue').where(({or, exists}) =>
      or(
        exists('comments', q => q.where('id', '=', '0001')),
        exists('labels', q => q.where('id', '=', '0001')),
      ),
    );
    expect(await queryDelegate.run(pkOrQuery)).toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "createdAt": 1,
          "description": "description 1",
          "id": "0001",
          "ownerId": "0001",
          "title": "issue 1",
          Symbol(rc): 1,
        },
      ]
    `);
  });

  test('pk exists anded', async () => {
    const existsAndedQuery = newQuery(schema, 'issue')
      .whereExists('comments', q => q.where('id', '=', '0001'))
      .whereExists('labels', q => q.where('id', '=', '0001'));
    expect(await queryDelegate.run(existsAndedQuery)).toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "createdAt": 1,
          "description": "description 1",
          "id": "0001",
          "ownerId": "0001",
          "title": "issue 1",
          Symbol(rc): 1,
        },
      ]
    `);
  });
});

describe('run with options', () => {
  test('run with type', async () => {
    const queryDelegate = new QueryDelegateImpl();
    const {issueSource} = addData(queryDelegate);
    const issueQuery = newQuery(schema, 'issue').where('title', '=', 'issue 1');
    const singleFilterRowsUnknownP = queryDelegate.run(issueQuery, {
      type: 'unknown',
    });
    const singleFilterRowsCompleteP = queryDelegate.run(issueQuery, {
      type: 'complete',
    });
    consume(
      issueSource.push({
        type: 'remove',
        row: {
          id: '0001',
          title: 'issue 1',
          description: 'description 1',
          closed: false,
          ownerId: '0001',
          createdAt: 10,
        },
      }),
    );
    queryDelegate.callAllGotCallbacks();
    const singleFilterRowsUnknown = await singleFilterRowsUnknownP;
    const singleFilterRowsComplete = await singleFilterRowsCompleteP;

    expect(singleFilterRowsUnknown).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": 1,
        "description": "description 1",
        "id": "0001",
        "ownerId": "0001",
        "title": "issue 1",
        Symbol(rc): 1,
      },
    ]
  `);
    expect(singleFilterRowsComplete).toMatchInlineSnapshot(`[]`);
  });

  test('run with ttl', async () => {
    const queryDelegate = new QueryDelegateImpl();
    const issueQuery = newQuery(schema, 'issue').where('title', '=', 'issue 1');
    const unknownP = queryDelegate.run(issueQuery, {
      ttl: '1s',
      type: 'unknown',
    });
    const completeP = queryDelegate.run(issueQuery, {
      ttl: '1m',
      type: 'complete',
    });
    const hourP = queryDelegate.run(issueQuery, {
      ttl: '1h',
      type: 'unknown',
    });
    queryDelegate.callAllGotCallbacks();
    await Promise.all([unknownP, completeP, hourP]);

    expect(queryDelegate.addedServerQueries.map(q => q.ttl))
      .toMatchInlineSnapshot(`
      [
        "1s",
        "1m",
        "1h",
      ]
    `);
  });
});

test('view creation is wrapped in context.batchViewUpdates call', () => {
  let viewFactoryCalls = 0;
  const testView = {};
  const viewFactory = () => {
    viewFactoryCalls++;
    return testView;
  };

  class TestQueryDelegate extends QueryDelegateImpl {
    batchViewUpdates<T>(applyViewUpdates: () => T): T {
      expect(viewFactoryCalls).toEqual(0);
      const result = applyViewUpdates();
      expect(viewFactoryCalls).toEqual(1);
      return result;
    }
  }
  const queryDelegate = new TestQueryDelegate();

  const issueQuery = newQuery(schema, 'issue');
  const view = queryDelegate.materialize(issueQuery, viewFactory);
  expect(viewFactoryCalls).toEqual(1);
  expect(view).toBe(testView);
});

test('json columns are returned as JS objects', async () => {
  const queryDelegate = new QueryDelegateImpl({callGot: true});
  addData(queryDelegate);

  const userQuery = newQuery(schema, 'user');
  const rows = await queryDelegate.run(userQuery);
  expect(rows).toMatchInlineSnapshot(`
    [
      {
        "id": "0001",
        "metadata": {
          "login": "alicegh",
          "registrar": "github",
        },
        "name": "Alice",
        Symbol(rc): 1,
      },
      {
        "id": "0002",
        "metadata": {
          "altContacts": [
            "bobwave",
            "bobyt",
            "bobplus",
          ],
          "login": "bob@gmail.com",
          "registar": "google",
        },
        "name": "Bob",
        Symbol(rc): 1,
      },
    ]
  `);
});

test('complex expression', async () => {
  const queryDelegate = new QueryDelegateImpl({callGot: true});
  addData(queryDelegate);

  const complexQuery1 = newQuery(schema, 'issue').where(({or, cmp}) =>
    or(cmp('title', '=', 'issue 1'), cmp('title', '=', 'issue 2')),
  );
  let rows = await queryDelegate.run(complexQuery1);
  expect(rows).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": 1,
        "description": "description 1",
        "id": "0001",
        "ownerId": "0001",
        "title": "issue 1",
        Symbol(rc): 1,
      },
      {
        "closed": false,
        "createdAt": 2,
        "description": "description 2",
        "id": "0002",
        "ownerId": "0002",
        "title": "issue 2",
        Symbol(rc): 1,
      },
    ]
  `);

  const complexQuery2 = newQuery(schema, 'issue').where(({and, cmp, or}) =>
    and(
      cmp('ownerId', '=', '0001'),
      or(cmp('title', '=', 'issue 1'), cmp('title', '=', 'issue 2')),
    ),
  );
  rows = await queryDelegate.run(complexQuery2);

  expect(rows).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": 1,
        "description": "description 1",
        "id": "0001",
        "ownerId": "0001",
        "title": "issue 1",
        Symbol(rc): 1,
      },
    ]
  `);
});

test('null compare', async () => {
  const queryDelegate = new QueryDelegateImpl({callGot: true});
  addData(queryDelegate);

  const nullQuery1 = newQuery(schema, 'issue').where('ownerId', 'IS', null);
  let rows = await queryDelegate.run(nullQuery1);
  expect(rows).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": 3,
        "description": "description 3",
        "id": "0003",
        "ownerId": null,
        "title": "issue 3",
        Symbol(rc): 1,
      },
    ]
  `);

  const nullQuery2 = newQuery(schema, 'issue').where('ownerId', 'IS NOT', null);
  rows = await queryDelegate.run(nullQuery2);

  expect(rows).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": 1,
        "description": "description 1",
        "id": "0001",
        "ownerId": "0001",
        "title": "issue 1",
        Symbol(rc): 1,
      },
      {
        "closed": false,
        "createdAt": 2,
        "description": "description 2",
        "id": "0002",
        "ownerId": "0002",
        "title": "issue 2",
        Symbol(rc): 1,
      },
    ]
  `);
});

test('where with undefined converts to null', async () => {
  const queryDelegate = new QueryDelegateImpl({callGot: true});
  addData(queryDelegate);

  // 2-arg form: .where(field, undefined) should not throw.
  // It converts undefined to null and uses '=' operator,
  // which returns no rows (NULL = NULL is false in SQL semantics).
  const undefinedQuery = newQuery(schema, 'issue').where('ownerId', undefined);
  const rows = await queryDelegate.run(undefinedQuery);
  expect(rows).toEqual([]);

  // 3-arg form: .where(field, 'IS', undefined) should match null rows
  const isUndefinedQuery = newQuery(schema, 'issue').where(
    'ownerId',
    'IS',
    undefined,
  );
  const isRows = await queryDelegate.run(isUndefinedQuery);
  expect(isRows).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": 3,
        "description": "description 3",
        "id": "0003",
        "ownerId": null,
        "title": "issue 3",
        Symbol(rc): 1,
      },
    ]
  `);
});

test('literal filter', async () => {
  const queryDelegate = new QueryDelegateImpl({callGot: true});
  addData(queryDelegate);

  const literalQuery1 = newQuery(schema, 'issue').where(({cmpLit}) =>
    cmpLit(true, '=', false),
  );
  let rows = await queryDelegate.run(literalQuery1);
  expect(rows).toEqual([]);

  const literalQuery2 = newQuery(schema, 'issue').where(({cmpLit}) =>
    cmpLit(true, '=', true),
  );
  rows = await queryDelegate.run(literalQuery2);

  expect(rows).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": 1,
        "description": "description 1",
        "id": "0001",
        "ownerId": "0001",
        "title": "issue 1",
        Symbol(rc): 1,
      },
      {
        "closed": false,
        "createdAt": 2,
        "description": "description 2",
        "id": "0002",
        "ownerId": "0002",
        "title": "issue 2",
        Symbol(rc): 1,
      },
      {
        "closed": false,
        "createdAt": 3,
        "description": "description 3",
        "id": "0003",
        "ownerId": null,
        "title": "issue 3",
        Symbol(rc): 1,
      },
    ]
  `);
});

test('join with compound keys', async () => {
  const b = table('b')
    .columns({
      id: number(),
      b1: number(),
      b2: number(),
      b3: number(),
    })
    .primaryKey('id');

  const a = table('a')
    .columns({
      id: number(),
      a1: number(),
      a2: number(),
      a3: number(),
    })
    .primaryKey('id');

  const aRelationships = relationships(a, connect => ({
    b: connect.many({
      sourceField: ['a1', 'a2'],
      destField: ['b1', 'b2'],
      destSchema: b,
    }),
  }));

  const schema = createSchema({
    tables: [a, b],
    relationships: [aRelationships],
  });

  const sources = {
    a: createSource(
      lc,
      testLogConfig,
      'a',
      schema.tables.a.columns,
      schema.tables.a.primaryKey,
    ),
    b: createSource(
      lc,
      testLogConfig,
      'b',
      schema.tables.b.columns,
      schema.tables.b.primaryKey,
    ),
  };

  const queryDelegate = new QueryDelegateImpl({sources, callGot: true});
  const aSource = must(queryDelegate.getSource('a'));
  const bSource = must(queryDelegate.getSource('b'));

  for (const row of [
    {id: 0, a1: 1, a2: 2, a3: 3},
    {id: 1, a1: 2, a2: 3, a3: 4},
    {id: 2, a1: 2, a2: 3, a3: 5},
  ]) {
    consume(
      aSource.push({
        type: 'add',
        row,
      }),
    );
  }

  for (const row of [
    {id: 0, b1: 1, b2: 2, b3: 3},
    {id: 1, b1: 1, b2: 2, b3: 4},
    {id: 2, b1: 2, b2: 3, b3: 5},
  ]) {
    consume(
      bSource.push({
        type: 'add',
        row,
      }),
    );
  }

  const relatedQuery = newQuery(schema, 'a').related('b');
  const rows = await queryDelegate.run(relatedQuery);

  expect(rows).toMatchInlineSnapshot(`
    [
      {
        "a1": 1,
        "a2": 2,
        "a3": 3,
        "b": [
          {
            "b1": 1,
            "b2": 2,
            "b3": 3,
            "id": 0,
            Symbol(rc): 1,
          },
          {
            "b1": 1,
            "b2": 2,
            "b3": 4,
            "id": 1,
            Symbol(rc): 1,
          },
        ],
        "id": 0,
        Symbol(rc): 1,
      },
      {
        "a1": 2,
        "a2": 3,
        "a3": 4,
        "b": [
          {
            "b1": 2,
            "b2": 3,
            "b3": 5,
            "id": 2,
            Symbol(rc): 1,
          },
        ],
        "id": 1,
        Symbol(rc): 1,
      },
      {
        "a1": 2,
        "a2": 3,
        "a3": 5,
        "b": [
          {
            "b1": 2,
            "b2": 3,
            "b3": 5,
            "id": 2,
            Symbol(rc): 1,
          },
        ],
        "id": 2,
        Symbol(rc): 1,
      },
    ]
  `);
});

test('where exists', () => {
  const queryDelegate = new QueryDelegateImpl();
  const issueSource = must(queryDelegate.getSource('issue'));
  const labelSource = must(queryDelegate.getSource('label'));
  const issueLabelSource = must(queryDelegate.getSource('issueLabel'));
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '0001',
        title: 'issue 1',
        description: 'description 1',
        closed: false,
        ownerId: '0001',
        createdAt: 10,
      },
    }),
  );
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '0002',
        title: 'issue 2',
        description: 'description 2',
        closed: true,
        ownerId: '0002',
        createdAt: 20,
      },
    }),
  );
  consume(
    labelSource.push({
      type: 'add',
      row: {
        id: '0001',
        name: 'bug',
      },
    }),
  );

  const materializedQuery = newQuery(schema, 'issue')
    .where('closed', true)
    .whereExists('labels', q => q.where('name', 'bug'))
    .related('labels');
  const materialized: TypedView<unknown[]> =
    queryDelegate.materialize(materializedQuery);

  expect(materialized.data).toEqual([]);

  consume(
    issueLabelSource.push({
      type: 'add',
      row: {
        issueId: '0002',
        labelId: '0001',
      },
    }),
  );

  expect(materialized.data).toMatchInlineSnapshot(`
    [
      {
        "closed": true,
        "createdAt": 20,
        "description": "description 2",
        "id": "0002",
        "labels": [
          {
            "id": "0001",
            "name": "bug",
            Symbol(rc): 1,
          },
        ],
        "ownerId": "0002",
        "title": "issue 2",
        Symbol(rc): 1,
      },
    ]
  `);

  consume(
    issueLabelSource.push({
      type: 'remove',
      row: {
        issueId: '0002',
        labelId: '0001',
      },
    }),
  );

  expect(materialized.data).toEqual([]);
});

// More comprehensive tests of flipped exists are in `chinook-join-flip.pg.test`
test("flipped exists, or'ed", () => {
  const queryDelegate: QueryDelegate = new QueryDelegateImpl();
  const commentSource = must(queryDelegate.getSource('comment'));
  const issueSource = must(queryDelegate.getSource('issue'));

  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '0001',
        title: 'issue 1',
        description: 'description 1',
        closed: false,
        ownerId: '0001',
        createdAt: 10,
      },
    }),
  );

  const q = newQuery(schema, 'issue').where(({or, exists}) =>
    or(
      exists('comments', q => q.where('text', 'bug'), {flip: true}),
      exists('comments', q => q.where('text', 'bug'), {flip: true}),
    ),
  );

  const view = queryDelegate.materialize(q);

  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: 'c1',
        issueId: '0001',
        authorId: 'a1',
        text: 'bug',
        createdAt: 1,
      },
    }),
  );

  // Symbol(rc) should be ONE
  // as only a single add should have been made
  // it through the `push` call above.
  // I.e., both branches add the node but `union-fan-in` distincts them
  expect(view.data).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": 10,
        "description": "description 1",
        "id": "0001",
        "ownerId": "0001",
        "title": "issue 1",
        Symbol(rc): 1,
      },
    ]
  `);

  consume(
    commentSource.push({
      type: 'edit',
      oldRow: {
        id: 'c1',
        issueId: '0001',
        authorId: 'a1',
        text: 'bug',
        createdAt: 1,
      },
      row: {
        id: 'c1',
        issueId: '0001',
        authorId: 'a2',
        text: 'bug',
        createdAt: 1,
      },
    }),
  );

  expect(view.data).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": 10,
        "description": "description 1",
        "id": "0001",
        "ownerId": "0001",
        "title": "issue 1",
        Symbol(rc): 1,
      },
    ]
  `);

  consume(
    commentSource.push({
      type: 'remove',
      row: {
        id: 'c1',
        issueId: '0001',
        authorId: 'a1',
        text: 'bug',
        createdAt: 1,
      },
    }),
  );

  // should have retracted once, without error
  expect(view.data).toMatchInlineSnapshot(`[]`);

  // will not match the filter
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: 'c1',
        issueId: '0001',
        authorId: 'a1',
        text: 'not a bug',
        createdAt: 1,
      },
    }),
  );

  expect(view.data).toMatchInlineSnapshot(`[]`);

  consume(
    commentSource.push({
      type: 'edit',
      oldRow: {
        id: 'c1',
        issueId: '0001',
        authorId: 'a1',
        text: 'not a bug',
        createdAt: 1,
      },
      row: {
        id: 'c1',
        issueId: '0001',
        authorId: 'a2',
        text: 'bug',
        createdAt: 1,
      },
    }),
  );

  expect(view.data.length).toBe(1);
  expect(view.data[0]?.id).toBe('0001');
});

test('broken flipped exists', async () => {
  const queryDelegate = new QueryDelegateImpl();
  const commentSource = must(queryDelegate.getSource('comment'));
  const issueSource = must(queryDelegate.getSource('issue'));

  // issue 1 will have comments
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '0001',
        title: 'issue 1',
        description: 'description 1',
        closed: false,
        ownerId: '0001',
        createdAt: 10,
      },
    }),
  );
  // issue 2 will have no comments
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '0002',
        title: 'issue 2',
        description: 'description 2',
        closed: false,
        ownerId: '0001',
        createdAt: 10,
      },
    }),
  );

  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: 'c1',
        issueId: '0001',
        authorId: 'a1',
        text: 'not a bug',
        createdAt: 1,
      },
    }),
  );

  const flipQuery = newQuery(schema, 'issue').whereExists('comments', q =>
    q.whereExists('issue', {flip: true}),
  );
  const data = await queryDelegate.run(flipQuery);

  // only issue 1 is returned since issue 2 has no comments
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": 10,
        "description": "description 1",
        "id": "0001",
        "ownerId": "0001",
        "title": "issue 1",
        Symbol(rc): 1,
      },
    ]
  `);
});

test('duplicative where exists', () => {
  const queryDelegate = new QueryDelegateImpl();
  const issueSource = must(queryDelegate.getSource('issue'));
  const labelSource = must(queryDelegate.getSource('label'));
  const issueLabelSource = must(queryDelegate.getSource('issueLabel'));
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '0001',
        title: 'issue 1',
        description: 'description 1',
        closed: false,
        ownerId: '0001',
        createdAt: 10,
      },
    }),
  );
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '0002',
        title: 'issue 2',
        description: 'description 2',
        closed: true,
        ownerId: '0002',
        createdAt: 20,
      },
    }),
  );
  consume(
    labelSource.push({
      type: 'add',
      row: {
        id: '0001',
        name: 'bug',
      },
    }),
  );

  const materializedQuery2 = newQuery(schema, 'issue')
    .where('closed', true)
    .whereExists('labels', q => q.where('name', 'bug'))
    .whereExists('labels', q => q.where('name', 'bug'))
    .related('labels');
  const materialized: TypedView<unknown[]> =
    queryDelegate.materialize(materializedQuery2);

  expect(materialized.data).toEqual([]);

  consume(
    issueLabelSource.push({
      type: 'add',
      row: {
        issueId: '0002',
        labelId: '0001',
      },
    }),
  );

  expect(materialized.data).toMatchInlineSnapshot(`
    [
      {
        "closed": true,
        "createdAt": 20,
        "description": "description 2",
        "id": "0002",
        "labels": [
          {
            "id": "0001",
            "name": "bug",
            Symbol(rc): 1,
          },
        ],
        "ownerId": "0002",
        "title": "issue 2",
        Symbol(rc): 1,
      },
    ]
  `);

  consume(
    issueLabelSource.push({
      type: 'remove',
      row: {
        issueId: '0002',
        labelId: '0001',
      },
    }),
  );

  expect(materialized.data).toEqual([]);
});

test('where exists before where, see https://bugs.rocicorp.dev/issue/3417', () => {
  const queryDelegate = new QueryDelegateImpl();
  const issueSource = must(queryDelegate.getSource('issue'));

  const materializedQuery3 = newQuery(schema, 'issue')
    .whereExists('labels')
    .where('closed', true);
  const materialized: TypedView<unknown[]> =
    queryDelegate.materialize(materializedQuery3);

  // push a row that does not match the where filter
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '0001',
        title: 'issue 1',
        description: 'description 1',
        closed: false,
        ownerId: '0001',
        createdAt: 10,
      },
    }),
  );

  expect(materialized.data).toEqual([]);
});

test('result type unknown then complete', async () => {
  const queryDelegate = new QueryDelegateImpl();
  const issueQuery = newQuery(schema, 'issue');
  const m: TypedView<unknown[]> = queryDelegate.materialize(issueQuery);

  let rows: unknown[] = [undefined];
  let resultType = '';
  m.addListener((data, type) => {
    rows = deepClone(data) as unknown[];
    resultType = type;
  });

  expect(rows).toEqual([]);
  expect(resultType).toEqual('unknown');

  expect(queryDelegate.gotCallbacks.length).toBe(1);
  queryDelegate.gotCallbacks[0]?.(true);

  // updating of resultType is promised based, so check in a new
  // microtask
  // oxlint-disable-next-line unicorn/no-unnecessary-await -- intentional for test timing
  await 1;

  expect(rows).toEqual([]);
  expect(resultType).toEqual('complete');
});

test('result type initially complete', () => {
  const queryDelegate = new QueryDelegateImpl();
  queryDelegate.synchronouslyCallNextGotCallback = true;
  const issueQuery = newQuery(schema, 'issue');
  const m: TypedView<unknown[]> = queryDelegate.materialize(issueQuery);

  let rows: unknown[] = [undefined];
  let resultType = '';
  m.addListener((data, type) => {
    rows = deepClone(data) as unknown[];
    resultType = type;
  });

  expect(rows).toEqual([]);
  expect(resultType).toEqual('complete');
});

describe('junction relationship limitations', () => {
  const issueQuery = newQuery(schema, 'issue');
  const labelQuery = newQuery(schema, 'label');
  test('cannot limit a junction edge', () => {
    expect(() => issueQuery.related('labels', q => q.limit(10))).toThrow(
      'Limit is not supported in junction',
    );
  });

  test('can apply limit after exiting the junction edge', () => {
    expect(() =>
      issueQuery.related('labels', q =>
        q.related('issues', q => q.related('comments', q => q.limit(10))),
      ),
    ).not.toThrow();

    expect(() =>
      labelQuery.related('issues', q =>
        q.related('comments', q => q.limit(10)),
      ),
    ).not.toThrow();
  });

  test('cannot limit exists junction', () => {
    expect(() => issueQuery.whereExists('labels', q => q.limit(10))).toThrow(
      'Limit is not supported in junction',
    );
  });

  test('can limit exists after exiting the junction', () => {
    expect(() =>
      issueQuery.whereExists('labels', q =>
        q.whereExists('issues', q =>
          q.whereExists('comments', q => q.limit(10)),
        ),
      ),
    ).not.toThrow();

    expect(() =>
      labelQuery.whereExists('issues', q =>
        q.whereExists('comments', q => q.limit(10)),
      ),
    ).not.toThrow();
  });

  test('cannot order by a junction edge', () => {
    expect(() =>
      issueQuery.related('labels', q => q.orderBy('id', 'asc')),
    ).toThrow('Order by is not supported in junction');
  });

  test('can order by after exiting the junction edge', () => {
    expect(() =>
      issueQuery.related('labels', q =>
        q.related('issues', q =>
          q.related('comments', q => q.orderBy('id', 'asc')),
        ),
      ),
    ).not.toThrow();

    expect(() =>
      labelQuery.related('issues', q =>
        q.related('comments', q => q.orderBy('id', 'asc')),
      ),
    ).not.toThrow();
  });

  test('cannot order by exists junction', () => {
    expect(() =>
      issueQuery.whereExists('labels', q => q.orderBy('id', 'asc')),
    ).toThrow('Order by is not supported in junction');
  });

  test('can order by exists after exiting the junction', () => {
    expect(() =>
      issueQuery.whereExists('labels', q =>
        q.whereExists('issues', q =>
          q.whereExists('comments', q => q.orderBy('id', 'asc')),
        ),
      ),
    ).not.toThrow();

    expect(() =>
      labelQuery.whereExists('issues', q =>
        q.whereExists('comments', q => q.orderBy('id', 'asc')),
      ),
    ).not.toThrow();
  });
});

describe('addCustom / addServer are called', () => {
  async function check(
    type: 'addCustomQuery' | 'addServerQuery',
    op: 'preload' | 'materialize' | 'run',
  ) {
    const queryDelegate = new QueryDelegateImpl();
    let query = newQuery(schema, 'issue');
    if (type === 'addCustomQuery') {
      query = asQueryInternals(query).nameAndArgs('issue', []);
    }
    const spy = vi.spyOn(queryDelegate, type);
    switch (op) {
      case 'preload':
        queryDelegate.preload(query);
        break;
      case 'materialize':
        queryDelegate.materialize(query);
        break;
      case 'run':
        await queryDelegate.run(query);
        break;
    }

    expect(spy).toHaveBeenCalledOnce();
  }

  test('preload, materialize, run', async () => {
    for (const type of ['addCustomQuery', 'addServerQuery'] as const) {
      for (const op of ['preload', 'materialize', 'run'] as const) {
        await check(type, op);
      }
    }
  });
});
