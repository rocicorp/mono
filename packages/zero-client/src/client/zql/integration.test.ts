import {joinSymbol} from '@rocicorp/zql/src/zql/ivm/types.js';
import * as agg from '@rocicorp/zql/src/zql/query/agg.js';
import {exp, not, or} from '@rocicorp/zql/src/zql/query/entity-query.js';
import fc from 'fast-check';
import * as v from 'shared/src/valita.js';
import {expect, test} from 'vitest';
import {nanoid} from '../../util/nanoid.js';
import {ENTITIES_KEY_PREFIX} from '../keys.js';
import {Zero, getInternalReplicacheImplForTesting} from '../zero.js';

export async function tickAFewTimes(n = 10, time = 0) {
  for (let i = 0; i < n; i++) {
    await new Promise(resolve => setTimeout(resolve, time));
  }
}

const issueSchema = v.object({
  id: v.string(),
  title: v.string(),
  status: v.union(v.literal('open'), v.literal('closed')),
  priority: v.union(v.literal('high'), v.literal('medium'), v.literal('low')),
  assignee: v.string(),
  created: v.number(),
  updated: v.number(),
  closed: v.number().optional(),
});

type Issue = v.Infer<typeof issueSchema>;
type Label = {id: string; name: string};
type IssueLabel = {id: string; issueID: string; labelID: string};

const defaultIssues: readonly Issue[] = [
  {
    id: 'a',
    title: 'foo',
    status: 'open',
    priority: 'high',
    assignee: 'charles',
    created: Date.now(),
    updated: Date.now(),
  },
  {
    id: 'b',
    title: 'bar',
    status: 'open',
    priority: 'medium',
    assignee: 'bob',
    created: Date.now(),
    updated: Date.now(),
  },
  {
    id: 'c',
    title: 'baz',
    status: 'closed',
    priority: 'low',
    assignee: 'alice',
    created: Date.now(),
    updated: Date.now(),
  },
] as const;

const defaultLabels: readonly Label[] = [
  {id: 'a', name: 'foo'},
  {id: 'b', name: 'bar'},
  {id: 'c', name: 'baz'},
];

const defaultIssueLabels: readonly IssueLabel[] = [
  {id: 'a-a', issueID: 'a', labelID: 'a'},
  {id: 'a-b', issueID: 'a', labelID: 'b'},
  {id: 'b-b', issueID: 'b', labelID: 'b'},
  {id: 'b-c', issueID: 'b', labelID: 'c'},
  {id: 'c-c', issueID: 'c', labelID: 'c'},
];

const issueArbitrary: fc.Arbitrary<Issue> = fc.record({
  id: fc.string({
    minLength: 1,
    maxLength: 10,
  }),
  title: fc.string(),
  status: fc.constantFrom('open', 'closed'),
  priority: fc.constantFrom('high', 'medium', 'low'),
  assignee: fc.string(),
  created: fc.integer(),
  updated: fc.integer(),
  closed: fc.option(fc.integer(), {nil: undefined}),
});

const tenUniqueIssues = fc.uniqueArray(issueArbitrary, {
  comparator: (a, b) => a.id === b.id,
  minLength: 10,
  maxLength: 10,
});

// TODO: we have to make this non-empty for now
// otherwise we infinitely hang for an unknown reason.
const uniqueNonEmptyIssuesArbitrary = fc.uniqueArray(issueArbitrary, {
  comparator: (a, b) => a.id === b.id,
  minLength: 1,
  maxLength: 10,
});

function sampleTenUniqueIssues() {
  return fc.sample(tenUniqueIssues, 1)[0];
}

function newZero() {
  const z = new Zero({
    userID: 'user-' + nanoid(),
    queries: {
      issue: v => v as Issue,
      label: v => v as Label,
      issueLabel: v => v as IssueLabel,
    },
  });

  return z;
}

const compareIds = (a: {id: string}, b: {id: string}) =>
  a.id < b.id ? -1 : a.id > b.id ? 1 : 0;

function makeComparator(...fields: (keyof Issue)[]) {
  return (l: Partial<Issue>, r: Partial<Issue>) => {
    for (const field of fields) {
      const lVal = l[field];
      const rVal = r[field];
      if (lVal === rVal) {
        continue;
      }
      if (lVal === null || lVal === undefined) {
        return -1;
      }
      if (rVal === null || rVal === undefined) {
        return 1;
      }
      return lVal < rVal ? -1 : lVal > rVal ? 1 : 0;
    }
    return 0;
  };
}

test('1-shot against an empty collection', async () => {
  const z = newZero();
  const rows = z.query.issue.select('id').prepare().exec();
  expect(await rows).toEqual([]);
});

test('prepare a query before the collection has writes then run it', async () => {
  const issues = sampleTenUniqueIssues();
  const z = newZero();
  const stmt = z.query.issue.select('id').prepare();
  await Promise.all(issues.map(z.mutate.issue.create));

  const rows = await stmt.exec();
  expect(rows).toEqual(issues.sort(compareIds));

  await z.close();
});

test('prepare a query then run it once `experimentalWatch` has completed', async () => {
  const issues = sampleTenUniqueIssues();
  const z = newZero();
  await Promise.all(issues.map(z.mutate.issue.create));

  const stmt = z.query.issue.select('id').prepare();
  // This is a hacky way to wait for the watch to complete.
  await new Promise(resolve => setTimeout(resolve, 0));
  const rows = await stmt.exec();

  expect(rows).toEqual(issues.sort(compareIds));

  await z.close();
}, 30000);

test('exec a query before the source has been filled by anything', async () => {
  const issues = sampleTenUniqueIssues();
  const z = newZero();
  await Promise.all(issues.map(z.mutate.issue.create));

  // it should wait until the source has been seeded
  // before returning.
  const rows = await z.query.issue.select('id').prepare().exec();

  expect(rows).toEqual(issues.sort(compareIds));

  await z.close();
}, 30000);

test('subscribing to a query calls us with the complete query results on change', async () => {
  const issues = sampleTenUniqueIssues();
  const z = newZero();
  await Promise.all(issues.map(z.mutate.issue.create));

  let resolve: (v: unknown) => void;
  const calledPromise = new Promise(res => {
    resolve = res;
  });

  let callCount = 0;
  z.query.issue
    .select('id')
    .prepare()
    .subscribe(value => {
      expect(value).toEqual(issues.sort(compareIds));
      if (callCount === 0) {
        resolve(value);
      }
      ++callCount;
    });

  // make sure our subscription actually gets called with initial data!
  await calledPromise;

  // retract some issues
  const deletedIssues = issues.slice(0, 5);

  let lastCallCount = callCount;
  for (const issue of deletedIssues) {
    issues.shift();
    await z.mutate.issue.delete({id: issue.id});
    // check that our observer was called after each deletion.
    // TODO: if a mutator deletes many things in a single
    // transaction, we need to tie that to the lifetime of
    // a Materialite transaction. So observers are not notified
    // until the full Replicache mutation completes.
    expect(callCount).toBe(lastCallCount + 1);
    lastCallCount = callCount;
  }

  await z.close();
});

test('subscribing to differences', () => {});

test('each where operator', async () => {
  // go through each operator
  // double check it against a `filter` in JS
  const now = Date.now();
  const future = now + 1000;
  const past = now - 1000;
  const issues: Issue[] = [
    {
      id: 'a',
      title: 'a',
      status: 'open',
      priority: 'high',
      assignee: 'charles',
      created: past,
      updated: Date.now(),
    },
    {
      id: 'b',
      title: 'b',
      status: 'open',
      priority: 'medium',
      assignee: 'bob',
      created: now,
      updated: Date.now(),
    },
    {
      id: 'c',
      title: 'c',
      status: 'closed',
      priority: 'low',
      assignee: 'alice',
      created: future,
      updated: Date.now(),
    },
  ];

  const z = newZero();
  await Promise.all(issues.map(z.mutate.issue.create));

  let stmt = z.query.issue.select('id').where('id', '=', 'a').prepare();
  const rows = await stmt.exec();
  expect(rows).toEqual([issues[0]]);
  stmt.destroy();

  stmt = z.query.issue.select('id').where('id', '<', 'b').prepare();
  expect(await stmt.exec()).toEqual([issues[0]]);
  stmt.destroy();

  stmt = z.query.issue.select('id').where('id', '>', 'a').prepare();
  expect(await stmt.exec()).toEqual([issues[1], issues[2]]);
  stmt.destroy();

  stmt = z.query.issue.select('id').where('id', '>=', 'b').prepare();
  expect(await stmt.exec()).toEqual([issues[1], issues[2]]);
  stmt.destroy();

  stmt = z.query.issue.select('id').where('id', '<=', 'b').prepare();
  expect(await stmt.exec()).toEqual([issues[0], issues[1]]);
  stmt.destroy();

  // TODO: this breaks
  // stmt = z.query.issue.select('id').where('id', 'IN', ['a', 'b']).prepare();
  // expect(await stmt.exec()).toEqual([{id: 'a'}, {id: 'b'}]);
  // stmt.destroy();

  stmt = z.query.issue.select('id').where('assignee', 'LIKE', 'al%').prepare();
  expect(await stmt.exec()).toEqual([issues[2]]);
  stmt.destroy();

  stmt = z.query.issue.select('id').where('assignee', 'ILIKE', 'AL%').prepare();
  expect(await stmt.exec()).toEqual([issues[2]]);
  stmt.destroy();

  // now compare against created date
  // TODO: this breaks
  // stmt = z.query.issue.select('id').where('created', '=', now).prepare();
  // expect(await stmt.exec()).toEqual([{id: 'b'}]);
  // stmt.destroy();

  stmt = z.query.issue.select('id').where('created', '<', now).prepare();
  expect(await stmt.exec()).toEqual([issues[0]]);
  stmt.destroy();

  stmt = z.query.issue.select('id').where('created', '>', now).prepare();
  expect(await stmt.exec()).toEqual([issues[2]]);
  stmt.destroy();

  stmt = z.query.issue.select('id').where('created', '>=', now).prepare();
  expect(await stmt.exec()).toEqual([issues[1], issues[2]]);
  stmt.destroy();

  stmt = z.query.issue.select('id').where('created', '<=', now).prepare();
  expect(await stmt.exec()).toEqual([issues[0], issues[1]]);
  stmt.destroy();

  await z.close();
});

test('order by single field', async () => {
  await fc.assert(
    fc.asyncProperty(uniqueNonEmptyIssuesArbitrary, async issues => {
      const z = newZero();
      await Promise.all(issues.map(z.mutate.issue.create));
      await new Promise(resolve => setTimeout(resolve, 0));

      const compareAssignees = makeComparator('assignee', 'id');
      const stmt = z.query.issue
        .select('id', 'assignee')
        .asc('assignee')
        .prepare();
      const rows = await stmt.exec();
      try {
        expect(rows).toEqual(issues.sort(compareAssignees));
      } finally {
        await z.close();
      }
    }),
    {interruptAfterTimeLimit: 4000},
  );
});

test('order by id', async () => {
  await fc.assert(
    fc.asyncProperty(uniqueNonEmptyIssuesArbitrary, async issues => {
      const z = newZero();
      await Promise.all(issues.map(z.mutate.issue.create));

      const stmt = z.query.issue.select('id').asc('id').prepare();
      const rows = await stmt.exec();
      expect(rows).toEqual(issues.sort(compareIds));

      await z.close();
    }),
    {interruptAfterTimeLimit: 4000},
  );
});

test('order by compound fields', async () => {
  await fc.assert(
    fc.asyncProperty(uniqueNonEmptyIssuesArbitrary, async issues => {
      const z = newZero();
      await Promise.all(issues.map(z.mutate.issue.create));

      const compareExpected = makeComparator('assignee', 'created', 'id');
      const stmt = z.query.issue
        .select('id', 'assignee', 'created')
        .asc('assignee', 'created')
        .prepare();
      const rows = await stmt.exec();
      expect(rows).toEqual(issues.sort(compareExpected));

      await z.close();
    }),
    {interruptAfterTimeLimit: 4000},
  );
});

test('order by optional field', async () => {
  await fc.assert(
    fc.asyncProperty(uniqueNonEmptyIssuesArbitrary, async issues => {
      const z = newZero();
      await Promise.all(issues.map(z.mutate.issue.create));

      const compareExpected = makeComparator('closed', 'id');
      const stmt = z.query.issue.select('id', 'closed').asc('closed').prepare();
      const rows = await stmt.exec();
      expect(rows).toEqual(issues.sort(compareExpected));

      await z.close();
    }),
    {interruptAfterTimeLimit: 4000},
  );
});

test('qualified selectors in where', async () => {
  const z = newZero();
  const issues = defaultIssues;
  await Promise.all(issues.map(z.mutate.issue.create));

  const stmt = z.query.issue
    .select('id')
    .where('issue.status', '=', 'open')
    .where('issue.priority', '>=', 'medium')
    .prepare();

  const rows = await stmt.exec();
  expect(rows).toEqual([issues[1]]);

  await z.close();
});

test('qualified selectors in group-by', async () => {
  const z = newZero();
  const issues = defaultIssues;
  await Promise.all(issues.map(z.mutate.issue.create));

  const stmt = z.query.issue
    .select(agg.count())
    .groupBy('issue.status')
    .prepare();

  const rows = await stmt.exec();
  expect(rows).toEqual([
    {...issues[0], count: 2},
    {...issues[2], count: 1},
  ]);

  await z.close();
});

test('qualified selectors in order-by', async () => {
  const z = newZero();
  const issues = defaultIssues;
  await Promise.all(issues.map(z.mutate.issue.create));

  const stmt = z.query.issue.select('id').asc('issue.priority').prepare();
  const rows = await stmt.exec();
  expect(rows).toEqual([issues[0], issues[2], issues[1]]);

  await z.close();
});

test('join', async () => {
  const z = newZero();
  const issues = defaultIssues;
  const labels = defaultLabels;
  const issueLabels = defaultIssueLabels;

  await Promise.all([
    ...issues.map(z.mutate.issue.create),
    ...labels.map(z.mutate.label.create),
    ...issueLabels.map(z.mutate.issueLabel.create),
  ]);

  const stmt = z.query.issue
    .join(z.query.issueLabel, 'issueLabel', 'issue.id', 'issueLabel.issueID')
    .join(z.query.label, 'label', 'issueLabel.labelID', 'label.id')
    .select('issue.*', 'label.name')
    .prepare();
  const rows = await stmt.exec();

  expect(rows).toEqual([
    {
      id: 'a_a-b_b',
      issue: issues[0],
      issueLabel: issueLabels[1],
      label: labels[1],
      [joinSymbol]: true,
    },
    {
      id: 'a_a_a-a',
      issue: issues[0],
      issueLabel: issueLabels[0],
      label: labels[0],
      [joinSymbol]: true,
    },
    {
      id: 'b_b-c_c',
      issue: issues[1],
      issueLabel: issueLabels[3],
      label: labels[2],
      [joinSymbol]: true,
    },
    {
      id: 'b_b_b-b',
      issue: issues[1],
      issueLabel: issueLabels[2],
      label: labels[1],
      [joinSymbol]: true,
    },
    {
      id: 'c_c_c-c',
      issue: issues[2],
      issueLabel: issueLabels[4],
      label: labels[2],
      [joinSymbol]: true,
    },
  ]);

  // TODO:
  // - order the joined result
  // - group by
  // - where
  // - test deltas
  // - test after
  // - test limit
  // - test deletes
  // - benchmark

  await z.close();
});

test('having', () => {});

test('group by', async () => {
  const z = newZero();
  const issues: readonly Issue[] = [
    {
      id: 'a',
      title: 'foo',
      status: 'open',
      priority: 'high',
      assignee: 'charles',
      created: new Date('2024-01-01').getTime(),
      updated: Date.now(),
    },
    {
      id: 'b',
      title: 'bar',
      status: 'open',
      priority: 'medium',
      assignee: 'bob',
      created: new Date('2024-01-02').getTime(),
      updated: Date.now(),
    },
    {
      id: 'c',
      title: 'baz',
      status: 'closed',
      priority: 'low',
      assignee: 'alice',
      created: new Date('2024-01-03').getTime(),
      updated: Date.now(),
    },
  ] as const;
  await Promise.all(issues.map(z.mutate.issue.create));
  const stmt = z.query.issue
    .select('status', agg.count())
    .groupBy('status')
    .prepare();
  const rows = await stmt.exec();

  expect(rows).toEqual([
    {
      ...issues[0],
      count: 2,
    },
    {
      ...issues[2],
      count: 1,
    },
  ]);

  stmt.destroy();

  const stmt2 = z.query.issue
    .select('status', agg.array('assignee'))
    .groupBy('status')
    .prepare();
  const rows2 = await stmt2.exec();

  expect(rows2).toEqual([
    {
      ...issues[0],
      assignee: ['charles', 'bob'],
    },
    {
      ...issues[2],
      assignee: ['alice'],
    },
  ]);

  const stmt3 = z.query.issue
    .select('status', agg.array('assignee'), agg.min('created'))
    .groupBy('status')
    .prepare();
  const rows3 = await stmt3.exec();

  expect(rows3).toEqual([
    {
      ...issues[0],
      assignee: ['charles', 'bob'],
      created: issues[0].created,
    },
    {
      ...issues[2],
      assignee: ['alice'],
      created: issues[2].created,
    },
  ]);

  const stmt4 = z.query.issue
    .select(
      'status',
      agg.array('assignee'),
      agg.min('created', 'minCreated'),
      agg.max('created', 'maxCreated'),
    )
    .groupBy('status')
    .prepare();
  const rows4 = await stmt4.exec();

  expect(rows4).toEqual([
    {
      ...issues[0],
      status: 'open',
      assignee: ['charles', 'bob'],
      minCreated: issues[0].created,
      maxCreated: issues[1].created,
    },
    {
      ...issues[2],
      status: 'closed',
      assignee: ['alice'],
      minCreated: issues[2].created,
      maxCreated: issues[2].created,
    },
  ]);

  {
    const statement = z.query.issue
      .select(
        'status',
        agg.min('created', 'minCreated'),
        agg.max('created', 'maxCreated'),
      )
      .where('assignee', '=', 'dan')
      .groupBy('status')
      .prepare();
    const rows = await statement.exec();

    expect(rows).toEqual([]);
  }

  {
    const statement = z.query.issue
      .select(
        'status',
        agg.min('assignee', 'minAssignee'),
        agg.max('assignee', 'maxAssignee'),
      )
      .groupBy('status')
      .prepare();
    const rows = await statement.exec();

    expect(rows).toEqual([
      {
        ...issues[0],
        maxAssignee: 'charles',
        minAssignee: 'bob',
      },
      {
        ...issues[2],
        maxAssignee: 'alice',
        minAssignee: 'alice',
      },
    ]);
  }

  await z.close();
});

test('sorted groupings', () => {});

test('compound where', async () => {
  const z = newZero();
  const issues = defaultIssues;
  await Promise.all(issues.map(z.mutate.issue.create));

  const stmt = z.query.issue
    .select('id')
    .where('status', '=', 'open')
    .where('priority', '>=', 'medium')
    .prepare();
  const rows = await stmt.exec();
  expect(rows).toEqual([issues[1]]);

  await z.close();
});

test('0 copy', async () => {
  const issues = sampleTenUniqueIssues();
  const z = newZero();
  const rep = getInternalReplicacheImplForTesting(z);
  await Promise.all(issues.map(z.mutate.issue.create));
  const replicacheIssues = (await rep.query(tx =>
    tx
      .scan({
        prefix: `${ENTITIES_KEY_PREFIX}issue`,
      })
      .toArray(),
  )) as Issue[];

  const stmt = z.query.issue.select('id').prepare();
  const zqlIssues = await stmt.exec();

  replicacheIssues.sort(compareIds);

  expect(zqlIssues).toEqual(replicacheIssues);
  for (let i = 0; i < zqlIssues.length; i++) {
    expect(zqlIssues[i]).toBe(replicacheIssues[i]);
  }

  const stmt2 = z.query.issue.select('title').prepare();
  const zqlIssues2 = await stmt2.exec();
  expect(zqlIssues2).toEqual(replicacheIssues);
  for (let i = 0; i < zqlIssues2.length; i++) {
    expect(zqlIssues2[i]).toBe(replicacheIssues[i]);
  }

  // just a sanity check to make sure the test actually checked items
  expect(replicacheIssues.length).toBe(10);
});

test('or where', async () => {
  const z = newZero();
  const issues: readonly Issue[] = [
    {
      id: 'a',
      title: 'foo',
      status: 'open',
      priority: 'high',
      assignee: 'charles',
      created: Date.now(),
      updated: Date.now(),
    },
    {
      id: 'b',
      title: 'bar',
      status: 'open',
      priority: 'medium',
      assignee: 'bob',
      created: Date.now(),
      updated: Date.now(),
    },
    {
      id: 'c',
      title: 'baz',
      status: 'closed',
      priority: 'low',
      assignee: 'alice',
      created: Date.now(),
      updated: Date.now(),
    },
  ] as const;
  await Promise.all(issues.map(z.mutate.issue.create));

  const stmt = z.query.issue
    .select('id')
    .where(or(exp('status', '=', 'open'), exp('priority', '>=', 'medium')))
    .prepare();
  const rows = await stmt.exec();
  expect(rows).toEqual([issues[0], issues[1]]);

  await z.mutate.issue.delete({id: 'a'});
  const rows2 = await stmt.exec();
  expect(rows2).toEqual([issues[1]]);

  await z.close();
});

test('not', async () => {
  const z = newZero();
  const issues: readonly Issue[] = [
    {
      id: 'a',
      title: 'foo',
      status: 'open',
      priority: 'high',
      assignee: 'charles',
      created: Date.now(),
      updated: Date.now(),
    },
    {
      id: 'b',
      title: 'bar',
      status: 'open',
      priority: 'medium',
      assignee: 'bob',
      created: Date.now(),
      updated: Date.now(),
    },
    {
      id: 'c',
      title: 'baz',
      status: 'closed',
      priority: 'low',
      assignee: 'alice',
      created: Date.now(),
      updated: Date.now(),
    },
  ] as const;
  await Promise.all(issues.map(z.mutate.issue.create));

  const stmt = z.query.issue
    .select('id')
    .where(not(exp('status', '=', 'closed')))
    .prepare();
  const rows = await stmt.exec();
  expect(rows).toEqual([issues[0], issues[1]]);

  await z.mutate.issue.delete({id: 'a'});
  const rows2 = await stmt.exec();
  expect(rows2).toEqual([issues[1]]);

  await z.close();
});

test('count', async () => {
  const z = newZero();
  const issues: readonly Issue[] = [
    {
      id: 'a',
      title: 'foo',
      status: 'open',
      priority: 'high',
      assignee: 'charles',
      created: Date.now(),
      updated: Date.now(),
    },
    {
      id: 'b',
      title: 'bar',
      status: 'open',
      priority: 'medium',
      assignee: 'bob',
      created: Date.now(),
      updated: Date.now(),
    },
    {
      id: 'c',
      title: 'baz',
      status: 'closed',
      priority: 'low',
      assignee: 'alice',
      created: Date.now(),
      updated: Date.now(),
    },
  ] as const;
  await Promise.all(issues.map(z.mutate.issue.create));

  const stmt = z.query.issue.select(agg.count()).prepare();
  const rows = await stmt.exec();
  let {count} = rows[0];
  expect(count).toBe(3);

  await z.mutate.issue.delete({id: 'a'});
  const rows2 = await stmt.exec();
  ({count} = rows2[0]);
  expect(count).toBe(2);

  await z.close();
});

// test('limit', () => {});
// test('adding items late to a source materializes them in the correct order', () => {});
// test('disposing of a subscription causes us to no longer be called back', () => {});
// test('hoisting `after` operations to the source', () => {});
// test('hoisting `limit` operations to the source', () => {});
// test('hoisting `where` operations to the source', () => {});
// test('correctly sorted source is used to optimize joins', () => {});
// test('order-by selects the correct source', () => {});
// test('write delay with 1, 10, 100, 1000s of active queries', () => {});
// test('asc/desc difference does not create new sources', () => {});
// test('we do not do a full scan when the source order matches the view order', () => {});
