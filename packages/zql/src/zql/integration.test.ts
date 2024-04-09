import {generate} from '@rocicorp/rails';
import fc from 'fast-check';
import {nanoid} from 'nanoid';
import {Replicache, TEST_LICENSE_KEY} from 'replicache';
import {expect, test} from 'vitest';
import {z} from 'zod';
import {makeReplicacheContext} from './context/replicache-context.js';
import * as agg from './query/agg.js';
import {EntityQuery, expression, not, or} from './query/entity-query.js';

export async function tickAFewTimes(n = 10, time = 0) {
  for (let i = 0; i < n; i++) {
    await new Promise(resolve => setTimeout(resolve, time));
  }
}

const issueSchema = z.object({
  id: z.string(),
  title: z.string(),
  status: z.enum(['open', 'closed']),
  priority: z.enum(['high', 'medium', 'low']),
  assignee: z.string(),
  created: z.number(),
  updated: z.number(),
  closed: z.number().optional(),
});

type Issue = z.infer<typeof issueSchema>;

const {
  init: initIssue,
  set: setIssue,
  update: updateIssue,
  delete: deleteIssue,
} = generate<Issue>('issue', issueSchema.parse);

const mutators = {
  initIssue,
  setIssue,
  updateIssue,
  deleteIssue,
};

function newRep() {
  return new Replicache({
    licenseKey: TEST_LICENSE_KEY,
    name: nanoid(),
    mutators,
  });
}

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

function setup() {
  const r = newRep();
  const c = makeReplicacheContext(r);
  const q = new EntityQuery<{issue: Issue}>(c, 'issue');
  return {r, c, q};
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
// test('experimental watch with no data', async () => {
//   const {r} = setup();
//   const spy = vi.fn(() => {});
//   r.experimentalWatch(spy, {initialValuesInFirstDiff: true});
//   await tickAFewTimes();
//   expect(spy).toHaveBeenCalledTimes(1);

//   await r.close();
// });

// This test fails because `experimentalWatch` does not call us with an empty array when we want initial data from an empty collection.
// So we wait for forever for data to be available.
// test('1-shot against an empty collection', async () => {
//   expect(
//     'This test fails for some unknown reason. ExperimentalWatch does notify with empty data so it is not that',
//   ).toEqual('');
//   const {q} = setup();
//   const rows = q.select('id').prepare().exec();
//   expect(await rows).toEqual([]);
// });

test('prepare a query before the collection has writes then run it', async () => {
  const issues = sampleTenUniqueIssues();
  const {q, r} = setup();
  const stmt = q.select('id').prepare();
  await Promise.all(issues.map(r.mutate.initIssue));

  const rows = await stmt.exec();
  expect(rows).toEqual(issues.sort(compareIds));

  await r.close();
});

test('prepare a query then run it once `experimentalWatch` has completed', async () => {
  const issues = sampleTenUniqueIssues();
  const {q, r} = setup();
  await Promise.all(issues.map(r.mutate.initIssue));

  const stmt = q.select('id').prepare();
  // This is a hacky way to wait for the watch to complete.
  await new Promise(resolve => setTimeout(resolve, 0));
  const rows = await stmt.exec();

  expect(rows).toEqual(issues.sort(compareIds));

  await r.close();
});

test('exec a query before the source has been filled by anything', async () => {
  const issues = sampleTenUniqueIssues();
  const {q, r} = setup();
  await Promise.all(issues.map(r.mutate.initIssue));

  // it should wait until the source has been seeded
  // before returning.
  const rows = await q.select('id').prepare().exec();

  expect(rows).toEqual(issues.sort(compareIds));

  await r.close();
});

test('subscribing to a query calls us with the complete query results on change', async () => {
  const issues = sampleTenUniqueIssues();
  const {q, r} = setup();
  await Promise.all(issues.map(r.mutate.initIssue));

  let resolve: (v: unknown) => void;
  const calledPromise = new Promise(res => {
    resolve = res;
  });

  let callCount = 0;
  q.select('id')
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
    await r.mutate.deleteIssue(issue.id);
    // check that our observer was called after each deletion.
    // TODO: if a mutator deletes many things in a single
    // transaction, we need to tie that to the lifetime of
    // a Materialite transaction. So observers are not notified
    // until the full Replicache mutation completes.
    expect(callCount).toBe(lastCallCount + 1);
    lastCallCount = callCount;
  }

  await r.close();
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

  const {q, r} = setup();
  await Promise.all(issues.map(r.mutate.initIssue));

  let stmt = q.select('id').where('id', '=', 'a').prepare();
  const rows = await stmt.exec();
  expect(rows).toEqual([issues[0]]);
  stmt.destroy();

  stmt = q.select('id').where('id', '<', 'b').prepare();
  expect(await stmt.exec()).toEqual([issues[0]]);
  stmt.destroy();

  stmt = q.select('id').where('id', '>', 'a').prepare();
  expect(await stmt.exec()).toEqual([issues[1], issues[2]]);
  stmt.destroy();

  stmt = q.select('id').where('id', '>=', 'b').prepare();
  expect(await stmt.exec()).toEqual([issues[1], issues[2]]);
  stmt.destroy();

  stmt = q.select('id').where('id', '<=', 'b').prepare();
  expect(await stmt.exec()).toEqual([issues[0], issues[1]]);
  stmt.destroy();

  // TODO: this breaks
  // stmt = q.select('id').where('id', 'IN', ['a', 'b']).prepare();
  // expect(await stmt.exec()).toEqual([{id: 'a'}, {id: 'b'}]);
  // stmt.destroy();

  stmt = q.select('id').where('assignee', 'LIKE', 'al%').prepare();
  expect(await stmt.exec()).toEqual([issues[2]]);
  stmt.destroy();

  stmt = q.select('id').where('assignee', 'ILIKE', 'AL%').prepare();
  expect(await stmt.exec()).toEqual([issues[2]]);
  stmt.destroy();

  // now compare against created date
  // TODO: this breaks
  // stmt = q.select('id').where('created', '=', now).prepare();
  // expect(await stmt.exec()).toEqual([{id: 'b'}]);
  // stmt.destroy();

  stmt = q.select('id').where('created', '<', now).prepare();
  expect(await stmt.exec()).toEqual([issues[0]]);
  stmt.destroy();

  stmt = q.select('id').where('created', '>', now).prepare();
  expect(await stmt.exec()).toEqual([issues[2]]);
  stmt.destroy();

  stmt = q.select('id').where('created', '>=', now).prepare();
  expect(await stmt.exec()).toEqual([issues[1], issues[2]]);
  stmt.destroy();

  stmt = q.select('id').where('created', '<=', now).prepare();
  expect(await stmt.exec()).toEqual([issues[0], issues[1]]);
  stmt.destroy();

  await r.close();
});

test('order by single field', async () => {
  await fc.assert(
    fc.asyncProperty(uniqueNonEmptyIssuesArbitrary, async issues => {
      const {q, r} = setup();
      await Promise.all(issues.map(r.mutate.initIssue));
      await new Promise(resolve => setTimeout(resolve, 0));

      const compareAssignees = makeComparator('assignee', 'id');
      const stmt = q.select('id', 'assignee').asc('assignee').prepare();
      const rows = await stmt.exec();
      try {
        expect(rows).toEqual(issues.sort(compareAssignees));
      } finally {
        await r.close();
      }
    }),
    {interruptAfterTimeLimit: 4000},
  );
});

test('order by id', async () => {
  await fc.assert(
    fc.asyncProperty(uniqueNonEmptyIssuesArbitrary, async issues => {
      const {q, r} = setup();
      await Promise.all(issues.map(r.mutate.initIssue));

      const stmt = q.select('id').asc('id').prepare();
      const rows = await stmt.exec();
      expect(rows).toEqual(issues.sort(compareIds));

      await r.close();
    }),
    {interruptAfterTimeLimit: 4000},
  );
});

test('order by compound fields', async () => {
  await fc.assert(
    fc.asyncProperty(uniqueNonEmptyIssuesArbitrary, async issues => {
      const {q, r} = setup();
      await Promise.all(issues.map(r.mutate.initIssue));

      const compareExpected = makeComparator('assignee', 'created', 'id');
      const stmt = q
        .select('id', 'assignee', 'created')
        .asc('assignee', 'created')
        .prepare();
      const rows = await stmt.exec();
      expect(rows).toEqual(issues.sort(compareExpected));

      await r.close();
    }),
    {interruptAfterTimeLimit: 4000},
  );
});

test('order by optional field', async () => {
  await fc.assert(
    fc.asyncProperty(uniqueNonEmptyIssuesArbitrary, async issues => {
      const {q, r} = setup();
      await Promise.all(issues.map(r.mutate.initIssue));

      const compareExpected = makeComparator('closed', 'id');
      const stmt = q.select('id', 'closed').asc('closed').prepare();
      const rows = await stmt.exec();
      expect(rows).toEqual(issues.sort(compareExpected));

      await r.close();
    }),
    {interruptAfterTimeLimit: 4000},
  );
});

test('join', () => {});
test('having', () => {});

test('group by', async () => {
  const {q, r} = setup();
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
  await Promise.all(issues.map(r.mutate.initIssue));
  const stmt = q.select('status', agg.count()).groupBy('status').prepare();
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

  const stmt2 = q
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

  const stmt3 = q
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

  const stmt4 = q
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

  await r.close();
});

test('sorted groupings', () => {});

test('compound where', async () => {
  const {q, r} = setup();
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
  await Promise.all(issues.map(r.mutate.initIssue));

  const stmt = q
    .select('id')
    .where('status', '=', 'open')
    .where('priority', '>=', 'medium')
    .prepare();
  const rows = await stmt.exec();
  expect(rows).toEqual([issues[1]]);

  await r.close();
});

test('0 copy', async () => {
  const issues = sampleTenUniqueIssues();
  const {q, r} = setup();
  await Promise.all(issues.map(r.mutate.initIssue));
  const replicacheIssues = (await r.query(tx =>
    tx
      .scan({
        prefix: 'issue',
      })
      .toArray(),
  )) as Issue[];

  const stmt = q.select('id').prepare();
  const zqlIssues = await stmt.exec();

  replicacheIssues.sort(compareIds);

  expect(zqlIssues).toEqual(replicacheIssues);
  for (let i = 0; i < zqlIssues.length; i++) {
    expect(zqlIssues[i]).toBe(replicacheIssues[i]);
  }

  const stmt2 = q.select('title').prepare();
  const zqlIssues2 = await stmt2.exec();
  expect(zqlIssues2).toEqual(replicacheIssues);
  for (let i = 0; i < zqlIssues2.length; i++) {
    expect(zqlIssues2[i]).toBe(replicacheIssues[i]);
  }

  // just a sanity check to make sure the test actually checked items
  expect(replicacheIssues.length).toBe(10);
});

// Need to pull this implementation into here from Materialite.
// The one thing we need to address when doing so is when the
// view goes under the limit (because a remove). in that case we should re-compute the query.
test('limit', () => {});

// To be implemented here: `asEntries` in `set-source.ts`
test('after', () => {});

test('adding items late to a source materializes them in the correct order', () => {});
test('disposing of a subscription causes us to no longer be called back', () => {});

test('hoisting `after` operations to the source', () => {});
test('hoisting `limit` operations to the source', () => {});
test('hoisting `where` operations to the source', () => {});

test('order by joined fields', () => {});

test('correctly sorted source is used to optimize joins', () => {});

test('order-by selects the correct source', () => {});

test('write delay with 1, 10, 100, 1000s of active queries', () => {});

test('asc/desc difference does not create new sources', () => {});

test('we do not do a full scan when the source order matches the view order', () => {});

test('or where', async () => {
  const {q, r} = setup();
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
  await Promise.all(issues.map(r.mutate.initIssue));

  const stmt = q
    .select('id')
    .where(
      or(
        expression('status', '=', 'open'),
        expression('priority', '>=', 'medium'),
      ),
    )
    .prepare();
  const rows = await stmt.exec();
  expect(rows).toEqual([issues[0], issues[1]]);

  await r.mutate.deleteIssue('a');
  const rows2 = await stmt.exec();
  expect(rows2).toEqual([issues[1]]);

  await r.close();
});

test('not', async () => {
  const {q, r} = setup();
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
  await Promise.all(issues.map(r.mutate.initIssue));

  const stmt = q
    .select('id')
    .where(not(expression('status', '=', 'closed')))
    .prepare();
  const rows = await stmt.exec();
  expect(rows).toEqual([issues[0], issues[1]]);

  await r.mutate.deleteIssue('a');
  const rows2 = await stmt.exec();
  expect(rows2).toEqual([issues[1]]);

  await r.close();
});

test('count', async () => {
  const {q, r} = setup();
  const issues: Issue[] = [
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
  await Promise.all(issues.map(r.mutate.initIssue));

  const stmt = q.select(agg.count()).prepare();
  const rows = await stmt.exec();
  let {count} = rows[0];
  expect(count).toBe(3);

  await r.mutate.deleteIssue('a');
  const rows2 = await stmt.exec();
  ({count} = rows2[0]);
  expect(count).toBe(2);

  await r.close();
});
