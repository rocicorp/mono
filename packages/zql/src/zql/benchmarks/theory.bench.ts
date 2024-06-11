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
import {compareUTF8} from 'compare-utf8';

type Issue = {
  id: string;
  title: string;
};

describe.each([
  /*
  - We have an optimization in ZQL that allows us to look up an entity by id in O(1) time.

  Results as of 3f1ddb1a3156fc4f39ea0f83e6e3c1ee458708b6:
  ✓ [Hydration Planner] 'lookup by primary key' (3) 2910ms
     name                           hz     min     max    mean     p75     p99    p995    p999     rme  samples
   · prepare and run        275,860.83  0.0000  2.9000  0.0036  0.0000  0.1000  0.1000  0.1000  ±4.23%   137958   slowest
   · previously prepared  3,227,896.00  0.0000  0.2000  0.0003  0.0000  0.0000  0.0000  0.1000  ±2.77%  1613948
   · theory               8,733,549.29  0.0000  0.2000  0.0001  0.0000  0.0000  0.0000  0.1000  ±2.77%  4367648   fastest
  */
  {
    name: 'lookup by primary key',
    getZql: (context: TestContext) =>
      new EntityQuery<{issue: Issue}>(context, 'issue')
        .select('id')
        .where('id', '=', '005000'),
    zqlExpected: (_: Issue[]) => {},
    theoryQuery: (collection: Map<string, Issue>) => collection.get('005000'),
  },
  /*
  Results as of 3f1ddb1a3156fc4f39ea0f83e6e3c1ee458708b6:
  ✓ [Hydration Planner] 'table scan with lookup on an un-index…' (3) 2092ms
     name                           hz     min     max    mean     p75     p99    p995    p999     rme  samples
   · prepare and run          2,946.00  0.2000  0.5000  0.3394  0.4000  0.5000  0.5000  0.5000  ±0.93%     1473   slowest
   · previously prepared  3,155,474.91  0.0000  0.2000  0.0003  0.0000  0.0000  0.0000  0.1000  ±2.77%  1578053   fastest
   · theory                  24,516.00  0.0000  0.2000  0.0408  0.1000  0.2000  0.2000  0.2000  ±2.23%    12258
  */
  {
    name: 'table scan with lookup on an un-indexed field',
    getZql: (context: TestContext) =>
      new EntityQuery<{issue: Issue}>(context, 'issue')
        .select('id')
        .where('title', '=', 'Issue 5000'),
    zqlExpected: (_: Issue[]) => {},
    theoryQuery: (collection: Map<string, Issue>) => {
      const ret: Issue[] = [];
      for (const issue of collection.values()) {
        if (issue.title === 'Issue 5000') {
          ret.push(issue);
        }
      }
      ret.sort((a, b) => compareUTF8(a.id, b.id));
      return ret;
    },
  },
  /*
  Results as of 3f1ddb1a3156fc4f39ea0f83e6e3c1ee458708b6:
  ✓ [Hydration Planner] 'table scan with no comparisons' (3) 2102ms
     name                           hz     min     max    mean     p75     p99    p995    p999     rme  samples
   · prepare and run            166.40  5.8000  6.2000  6.0095  6.1000  6.2000  6.2000  6.2000  ±0.27%       84   slowest
   · previously prepared  3,228,352.00  0.0000  0.2000  0.0003  0.0000  0.0000  0.0000  0.1000  ±2.77%  1614176   fastest
   · theory                   3,925.21  0.1000  0.4000  0.2548  0.3000  0.4000  0.4000  0.4000  ±1.03%     1963
  */
  {
    name: 'table scan with no comparisons',
    getZql: (context: TestContext) =>
      new EntityQuery<{issue: Issue}>(context, 'issue').select('id'),
    zqlExpected: (_: Issue[]) => {},
    theoryQuery: (collection: Map<string, Issue>) => {
      const ret: Issue[] = [];
      for (const issue of collection.values()) {
        ret.push(issue);
      }
      ret.sort((a, b) => compareUTF8(a.id, b.id));
      return ret;
    },
  },
])(`[Hydration Planner] $name`, async ({getZql, zqlExpected, theoryQuery}) => {
  const context = new TestContext();
  const source = context.getSource<Issue>('issue');
  const theory = new Map<string, Issue>();

  for (let i = 0; i < 10_000; ++i) {
    const issue = {id: i.toString().padStart(6, '0'), title: `Issue ${i}`};
    source.add(issue);
    theory.set(issue.id, issue);
  }

  const prepared = await getZql(context).prepare();

  bench('prepare and run', async () => {
    const stmt = getZql(context).prepare();
    const result = await stmt.exec();
    zqlExpected(result as Issue[]);
    stmt.destroy();
  });

  bench('previously prepared', async () => {
    const result = await prepared.exec();
    zqlExpected(result as Issue[]);
  });

  // Make sure we _actually_ cleaned up statements.
  // Prior versions of this benchmark forgot to clean up statements which caused
  // horribly wrong results.
  // This raises a good point thought:
  // We do, at some point, need to add query-deduplication and argument indices to our operators so
  //  1 million point queries do not take 1 million iterations.
  prepared.destroy();
  expect(source.stream.numDownstreams).toBe(0);
  source.stream.destroy();

  bench('theory', () => {
    theoryQuery(theory);
  });
});

// table scan with limit 1
