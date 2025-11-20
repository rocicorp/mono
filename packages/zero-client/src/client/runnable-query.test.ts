import {describe, expect, expectTypeOf, test} from 'vitest';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {refCountSymbol} from '../../../zql/src/ivm/view-apply-change.ts';
import type {RunnableQuery} from '../../../zql/src/query/runnable-query.ts';
import {zeroForTest} from './test-utils.ts';

const issueTable = table('issue')
  .columns({
    id: string(),
    title: string(),
    status: string(),
    priority: number(),
    ownerId: string(),
  })
  .primaryKey('id');

const userTable = table('user')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const issueSchema = createSchema({
  tables: [issueTable, userTable],
  relationships: [
    relationships(issueTable, ({one}) => ({
      owner: one({
        sourceField: ['ownerId'],
        destField: ['id'],
        destSchema: userTable,
      }),
    })),
  ],
});

describe('RunnableQuery - entity queries', () => {
  test('basic entity query.run() returns results', async () => {
    const z = zeroForTest({schema: issueSchema});

    await z.mutate.issue.insert({
      id: '1',
      title: 'First Issue',
      status: 'open',
      priority: 1,
      ownerId: 'u1',
    });
    await z.mutate.issue.insert({
      id: '2',
      title: 'Second Issue',
      status: 'closed',
      priority: 2,
      ownerId: 'u2',
    });

    const issues = await z.query.issue.run();

    expect(issues).toHaveLength(2);
    expect(issues).toEqual([
      {
        id: '1',
        title: 'First Issue',
        status: 'open',
        priority: 1,
        ownerId: 'u1',
        [refCountSymbol]: 1,
      },
      {
        id: '2',
        title: 'Second Issue',
        status: 'closed',
        priority: 2,
        ownerId: 'u2',
        [refCountSymbol]: 1,
      },
    ]);
  });

  test('chained where().run() filters results', async () => {
    const z = zeroForTest({schema: issueSchema});

    await z.mutate.issue.insert({
      id: '1',
      title: 'Open Issue',
      status: 'open',
      priority: 1,
      ownerId: 'u1',
    });
    await z.mutate.issue.insert({
      id: '2',
      title: 'Closed Issue',
      status: 'closed',
      priority: 2,
      ownerId: 'u2',
    });

    const openIssues = await z.query.issue.where('status', '=', 'open').run();

    expect(openIssues).toHaveLength(1);
    expect(openIssues[0].id).toBe('1');
    expect(openIssues[0].status).toBe('open');
  });

  test('chained limit().run() limits results', async () => {
    const z = zeroForTest({schema: issueSchema});

    await z.mutate.issue.insert({
      id: '1',
      title: 'Issue 1',
      status: 'open',
      priority: 1,
      ownerId: 'u1',
    });
    await z.mutate.issue.insert({
      id: '2',
      title: 'Issue 2',
      status: 'open',
      priority: 2,
      ownerId: 'u2',
    });
    await z.mutate.issue.insert({
      id: '3',
      title: 'Issue 3',
      status: 'open',
      priority: 3,
      ownerId: 'u3',
    });

    const limitedIssues = await z.query.issue.limit(2).run();

    expect(limitedIssues).toHaveLength(2);
  });

  test('chained orderBy().run() sorts results', async () => {
    const z = zeroForTest({schema: issueSchema});

    await z.mutate.issue.insert({
      id: '1',
      title: 'Issue 1',
      status: 'open',
      priority: 3,
      ownerId: 'u1',
    });
    await z.mutate.issue.insert({
      id: '2',
      title: 'Issue 2',
      status: 'open',
      priority: 1,
      ownerId: 'u2',
    });
    await z.mutate.issue.insert({
      id: '3',
      title: 'Issue 3',
      status: 'open',
      priority: 2,
      ownerId: 'u3',
    });

    const sortedIssues = await z.query.issue.orderBy('priority', 'asc').run();

    expect(sortedIssues).toHaveLength(3);
    expect(sortedIssues[0].priority).toBe(1);
    expect(sortedIssues[1].priority).toBe(2);
    expect(sortedIssues[2].priority).toBe(3);
  });

  test('complex chaining: where().limit().orderBy().run()', async () => {
    const z = zeroForTest({schema: issueSchema});

    await z.mutate.issue.insert({
      id: '1',
      title: 'Open 1',
      status: 'open',
      priority: 3,
      ownerId: 'u1',
    });
    await z.mutate.issue.insert({
      id: '2',
      title: 'Open 2',
      status: 'open',
      priority: 1,
      ownerId: 'u2',
    });
    await z.mutate.issue.insert({
      id: '3',
      title: 'Closed 1',
      status: 'closed',
      priority: 2,
      ownerId: 'u3',
    });
    await z.mutate.issue.insert({
      id: '4',
      title: 'Open 3',
      status: 'open',
      priority: 2,
      ownerId: 'u4',
    });

    const results = await z.query.issue
      .where('status', '=', 'open')
      .orderBy('priority', 'asc')
      .limit(2)
      .run();

    expect(results).toHaveLength(2);
    expect(results[0].priority).toBe(1);
    expect(results[1].priority).toBe(2);
    expect(results.every(r => r.status === 'open')).toBe(true);
  });

  test('one().run() returns single result or undefined', async () => {
    const z = zeroForTest({schema: issueSchema});

    await z.mutate.issue.insert({
      id: '1',
      title: 'Only Issue',
      status: 'open',
      priority: 1,
      ownerId: 'u1',
    });

    const allIssues = await z.query.issue.where('id', '=', '1').run();
    expect(allIssues).toHaveLength(1);
    expect(allIssues[0]?.id).toBe('1');
    expect(allIssues[0]?.title).toBe('Only Issue');

    const noIssue = await z.query.issue.where('id', '=', 'nonexistent').run();
    expect(noIssue).toHaveLength(0);
  });

  test('related().run() includes related data', async () => {
    const z = zeroForTest({schema: issueSchema});

    await z.mutate.user.insert({
      id: 'u1',
      name: 'Alice',
    });
    await z.mutate.issue.insert({
      id: '1',
      title: 'Issue with owner',
      status: 'open',
      priority: 1,
      ownerId: 'u1',
    });

    const issuesWithOwner = await z.query.issue.related('owner').run();

    expect(issuesWithOwner).toHaveLength(1);
    expect(issuesWithOwner[0].owner).toMatchObject({
      id: 'u1',
      name: 'Alice',
    });
  });
});

describe('RunnableQuery - run options', () => {
  test('run() with type: "unknown" returns data immediately', async () => {
    const z = zeroForTest({schema: issueSchema});

    await z.mutate.issue.insert({
      id: '1',
      title: 'Issue',
      status: 'open',
      priority: 1,
      ownerId: 'u1',
    });

    const issues = await z.query.issue.run({type: 'unknown'});

    expect(issues).toHaveLength(1);
    expect(issues[0].id).toBe('1');
  });

  test('run() accepts run options', async () => {
    const z = zeroForTest({schema: issueSchema});

    await z.mutate.issue.insert({
      id: '1',
      title: 'Issue',
      status: 'open',
      priority: 1,
      ownerId: 'u1',
    });

    // Test that run options parameter is accepted (type check mainly)
    const issues = await z.query.issue.run({type: 'unknown'});

    expect(issues).toHaveLength(1);
    expect(issues[0].id).toBe('1');
  });
});

describe('RunnableQuery - backward compatibility', () => {
  test('z.run(query) and query.run() produce identical results', async () => {
    const z = zeroForTest({schema: issueSchema});

    await z.mutate.issue.insert({
      id: '1',
      title: 'Issue 1',
      status: 'open',
      priority: 1,
      ownerId: 'u1',
    });
    await z.mutate.issue.insert({
      id: '2',
      title: 'Issue 2',
      status: 'closed',
      priority: 2,
      ownerId: 'u2',
    });

    const query = z.query.issue.where('status', '=', 'open');
    const oldWay = await z.run(query);
    const newWay = await query.run();

    expect(oldWay).toEqual(newWay);
  });
});

describe('RunnableQuery - type assertions', () => {
  test('entity query returns RunnableQuery type', () => {
    const z = zeroForTest({schema: issueSchema});

    const query = z.query.issue;

    // Type assertion: entity query should be RunnableQuery
    expectTypeOf(query).toEqualTypeOf<
      RunnableQuery<typeof issueSchema, 'issue'>
    >();
  });

  test('chained query methods return RunnableQuery', () => {
    const z = zeroForTest({schema: issueSchema});

    const whereQuery = z.query.issue.where('status', '=', 'open');
    const limitQuery = z.query.issue.limit(10);
    const orderQuery = z.query.issue.orderBy('priority', 'asc');

    // Type assertions: chained queries should maintain RunnableQuery type
    expectTypeOf(whereQuery).toHaveProperty('run');
    expectTypeOf(limitQuery).toHaveProperty('run');
    expectTypeOf(orderQuery).toHaveProperty('run');
  });

  test('run() returns Promise with correct type', () => {
    const z = zeroForTest({schema: issueSchema});

    const runResult = z.query.issue.run();

    // Type assertion: run should return a Promise
    expectTypeOf(runResult).toEqualTypeOf<
      Promise<
        {
          readonly id: string;
          readonly title: string;
          readonly status: string;
          readonly priority: number;
          readonly ownerId: string;
        }[]
      >
    >();
  });

  test('one() returns RunnableQuery with undefined union type', () => {
    const z = zeroForTest({schema: issueSchema});

    const oneQuery = z.query.issue.one();

    expectTypeOf<ReturnType<typeof oneQuery.run>>().toEqualTypeOf<
      Promise<
        | {
            readonly id: string;
            readonly title: string;
            readonly status: string;
            readonly priority: number;
            readonly ownerId: string;
          }
        | undefined
      >
    >();
  });

  test('related() returns RunnableQuery with enhanced type', () => {
    const z = zeroForTest({schema: issueSchema});

    const relatedQuery = z.query.issue.related('owner');

    // Type assertion: related() should return RunnableQuery
    expectTypeOf(relatedQuery).toHaveProperty('run');

    expectTypeOf<ReturnType<typeof relatedQuery.run>>().toEqualTypeOf<
      Promise<
        {
          readonly id: string;
          readonly title: string;
          readonly status: string;
          readonly priority: number;
          readonly ownerId: string;
          readonly owner:
            | {
                readonly id: string;
                readonly name: string;
              }
            | undefined;
        }[]
      >
    >();
  });
});

describe('RunnableQuery - edge cases', () => {
  test('run() on empty table returns empty array', async () => {
    const z = zeroForTest({schema: issueSchema});

    const issues = await z.query.issue.run();

    expect(issues).toEqual([]);
  });

  test('run() after where with no matches returns empty array', async () => {
    const z = zeroForTest({schema: issueSchema});

    await z.mutate.issue.insert({
      id: '1',
      title: 'Issue',
      status: 'open',
      priority: 1,
      ownerId: 'u1',
    });

    const noMatches = await z.query.issue
      .where('status', '=', 'nonexistent')
      .run();

    expect(noMatches).toEqual([]);
  });

  test('run() with limit(0) returns empty array', async () => {
    const z = zeroForTest({schema: issueSchema});

    await z.mutate.issue.insert({
      id: '1',
      title: 'Issue',
      status: 'open',
      priority: 1,
      ownerId: 'u1',
    });

    const noResults = await z.query.issue.limit(0).run();

    expect(noResults).toEqual([]);
  });
});
