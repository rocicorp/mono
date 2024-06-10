/**
 * Benchmarks ZQL against the theoretically fastest possible implementation.
 *
 * This is done by:
 * 1. Asking ZQL to do a query
 * 2. Crafting the fastest possible way to run that query by hand. E.g., direct array lookup.
 */

import {bench, describe, expect} from 'vitest';
import {TestContext} from '../context/test-context.js';
import {EntityQuery} from '../query/entity-query.js';

type Issue = {
  id: string;
  title: string;
};

/*
- We have an optimization in ZQL that allows us to look up an entity by id in O(1) time.

Results as of 86bfe06a5d0d0f868c31449fb90c1f1d8bb4ee86:
✓ [HydrationPlanner] point query to look up something by id (3) 1983ms
     name                         hz     min     max    mean     p75     p99    p995    p999     rme  samples
   · prepare and run      160,035.99  0.0000  3.3000  0.0062  0.0000  0.1000  0.1000  0.5000  ±3.96%    80034   slowest
   · previously prepared  738,472.31  0.0000  0.2000  0.0014  0.0000  0.1000  0.1000  0.1000  ±2.75%   369310
   · theory               880,897.82  0.0000  0.2000  0.0011  0.0000  0.1000  0.1000  0.1000  ±2.76%   440537   fastest
*/
describe('[HydrationPlanner] point query to look up something by id', async () => {
  const context = new TestContext();
  const source = context.getSource<Issue>('issue');
  const theory = new Map<string, Issue>();

  for (let i = 0; i < 10_000; ++i) {
    const issue = {id: i.toString().padStart(6, '0'), title: `Issue ${i}`};
    source.add(issue);
    theory.set(issue.id, issue);
  }

  const prepared = await new EntityQuery<{issue: Issue}>(context, 'issue')
    .select('id')
    .where('id', '=', '005000')
    .prepare();

  bench('prepare and run', async () => {
    const stmt = new EntityQuery<{issue: Issue}>(context, 'issue')
      .select('id')
      .where('id', '=', '005000')
      .prepare();
    const result = await stmt.exec();

    expect(result[0].id).toEqual('005000');
    stmt.destroy();
  });

  bench('previously prepared', async () => {
    const result = await prepared.exec();
    expect(result[0].id).toEqual('005000');
  });

  // Make sure we _actually_ cleaned up statements.
  // Prior versions of this benchmark forgot to clean up statements which caused
  // horribly wrong results.
  // This raises a good point thought:
  // We do, at some point, need to add query-deduplication and argument indices to our operators so
  //  1 million point queries do not take 1 million iterations.
  expect(source.stream.numDownstreams).toBe(0);

  bench('theory', () => {
    const issue = theory.get('005000');
    expect(issue?.id).toEqual('005000');
  });
});

/*
- ZQL does a full table scan in this case. How much slower is a ZQL table scan vs an array scan?

Results as of 86bfe06a5d0d0f868c31449fb90c1f1d8bb4ee86:
✓ [Hydration Planner] table scan to look up something by an un-indexed field (3) 1874ms
     name                         hz     min     max    mean     p75     p99    p995    p999     rme  samples
   · prepare and run        2,744.35  0.2000  0.7000  0.3644  0.4000  0.6000  0.6000  0.6000  ±1.12%     1373   slowest
   · previously prepared  698,318.00  0.0000  4.3000  0.0014  0.0000  0.1000  0.1000  0.1000  ±3.22%   349159   fastest
   · theory                28,224.00  0.0000  0.2000  0.0354  0.1000  0.1000  0.2000  0.2000  ±2.30%    14112

TODO: "Prepare and run" is off by 1 order of magnitude from theory. Seems like we can do better here.
*/
describe('[Hydration Planner] table scan to look up something by an un-indexed field', async () => {
  const context = new TestContext();
  const source = context.getSource<Issue>('issue');
  const theory: Issue[] = [];

  for (let i = 0; i < 10_000; ++i) {
    const issue = {id: i.toString().padStart(6, '0'), title: `Issue ${i}`};
    source.add(issue);
    theory.push(issue);
  }

  const prepared = await new EntityQuery<{issue: Issue}>(context, 'issue')
    .select('id')
    .where('title', '=', 'Issue 5000')
    .prepare();

  bench('prepare and run', async () => {
    const stmt = new EntityQuery<{issue: Issue}>(context, 'issue')
      .select('id')
      .where('title', '=', 'Issue 5000')
      .prepare();
    const result = await stmt.exec();

    expect(result[0].id).toEqual('005000');
    stmt.destroy();
  });

  bench('previously prepared', async () => {
    const result = await prepared.exec();
    expect(result[0].id).toEqual('005000');
  });

  // Make sure we _actually_ cleaned up statements.
  expect(source.stream.numDownstreams).toBe(0);

  bench('theory', () => {
    const result = [];
    for (const issue of theory) {
      if (issue.title === 'Issue 5000') {
        result.push(issue);
        // intentionally not breaking since title may not be unique in the real world.
        // break;
      }
    }
    expect(result[0].id).toEqual('005000');
  });
});
