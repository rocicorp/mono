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
type Label = {
  id: string;
  name: string;
};
type IssueLabel = {
  id: string;
  issueId: string;
  labelId: string;
};

type TheoryInput = {
  issueMap: Map<string, Issue>;
  issueLabelMap: Map<string, IssueLabel[]>;
  labelMap: Map<string, Label>;
  issueArray: Issue[];
  issuesByTitle: Issue[];
  issueLabelArray: IssueLabel[];
  labelArray: Label[];
};

const makeQueries = (context: TestContext) => ({
  issueQuery: new EntityQuery<{issue: Issue}>(context, 'issue'),
  issueLabelQuery: new EntityQuery<{issueLabel: IssueLabel}>(
    context,
    'issueLabel',
  ),
  labelQuery: new EntityQuery<{label: Label}>(context, 'label'),
});

const tableScanWithCompareTheory = ({issueMap}: TheoryInput) => {
  const ret: Issue[] = [];
  for (const issue of issueMap.values()) {
    if (issue.title === 'Issue 5000') {
      ret.push(issue);
    }
  }
  return ret;
};

describe.each([
  /*
  Results as of e2f73698dcc77664422fd2a765376aae73bb782a:
     name                           hz     min     max    mean     p75     p99    p995    p999     rme  samples
   · prepare and run        241,845.63  0.0000  3.5000  0.0041  0.0000  0.1000  0.1000  0.5000  ±4.21%   120947
   · previously prepared  3,213,160.00  0.0000  2.1000  0.0003  0.0000  0.0000  0.0000  0.1000  ±2.88%  1606580
   · theory               8,622,483.51  0.0000  1.6000  0.0001  0.0000  0.0000  0.0000  0.1000  ±2.84%  4312104   fastest
   · theory unplanned        23,177.36  0.0000  0.2000  0.0431  0.1000  0.2000  0.2000  0.2000  ±2.19%    11591   slowest

   FIXME: where's the time go for `prepare and run`? We're only touching 1 row, right?
  */
  {
    name: 'table.* WHERE id = x | primary key index on id',
    getZql: (context: TestContext) =>
      new EntityQuery<{issue: Issue}>(context, 'issue')
        .select('id')
        .where('id', '=', '005000'),
    zqlExpected: (_: Issue[]) => {},
    theoryQuery: ({issueMap}: TheoryInput) => issueMap.get('005000'),
    theoryUnplanned: ({issueMap}: TheoryInput) => {
      const ret = [];
      for (const issue of issueMap.values()) {
        if (issue.id === '005000') {
          ret.push(issue);
          // totally unplanned has no idea if anything is unique. must full scan. No break.
        }
      }
      ret.sort((a, b) => compareUTF8(a.id, b.id));
      return ret;
    },
  },
  /*
  Results as of e2f73698dcc77664422fd2a765376aae73bb782a:
     name                           hz     min     max    mean     p75     p99    p995    p999     rme  samples
   · prepare and run        377,360.53  0.0000  0.8000  0.0026  0.0000  0.1000  0.1000  0.1000  ±3.58%   188718
   · previously prepared  3,146,894.62  0.0000  1.9000  0.0003  0.0000  0.0000  0.0000  0.1000  ±2.86%  1573762
   · theory               7,756,918.63  0.0000  0.1000  0.0001  0.0000  0.0000  0.0000  0.1000  ±2.77%  3879235   fastest
   · theory unplanned         3,916.00  0.1000  3.1000  0.2554  0.3000  0.4000  0.4000  0.6000  ±1.52%     1958   slowest
  */
  {
    name: 'table.* LIMIT 1',
    getZql: (context: TestContext) =>
      new EntityQuery<{issue: Issue}>(context, 'issue').select('*').limit(1),
    zqlExpected: (_: Issue[]) => {},
    theoryQuery: ({issueMap}: TheoryInput) => {
      const ret: Issue[] = [];
      for (const issue of issueMap.values()) {
        ret.push(issue);
        break;
      }
      return ret;
    },
    theoryUnplanned: ({issueMap}: TheoryInput) => {
      const ret: Issue[] = [];
      for (const issue of issueMap.values()) {
        ret.push(issue);
      }
      return ret.sort((a, b) => compareUTF8(a.id, b.id)).slice(0, 1);
    },
  },
  /*
  Results as of e2f73698dcc77664422fd2a765376aae73bb782a:
   name                           hz     min     max    mean     p75     p99    p995    p999     rme  samples
   · prepare and run          2,209.56  0.3000  0.7000  0.4526  0.5000  0.6000  0.7000  0.7000  ±0.87%     1105   slowest
   · previously prepared  3,149,862.03  0.0000  1.6000  0.0003  0.0000  0.0000  0.0000  0.1000  ±2.84%  1575246
   · theory               6,447,190.00  0.0000  0.2000  0.0002  0.0000  0.0000  0.0000  0.1000  ±2.77%  3223595   fastest
   · theory unplanned         3,890.44  0.1000  0.4000  0.2570  0.3000  0.4000  0.4000  0.4000  ±1.00%     1946

   FIXME: `ASC LIMIT 1` is 100x faster. What is going on with DESC?
  */
  {
    name: 'table.* ORDER BY id DESC LIMIT 1',
    getZql: (context: TestContext) =>
      new EntityQuery<{issue: Issue}>(context, 'issue')
        .select('*')
        .orderBy('id', 'desc')
        .limit(1),
    zqlExpected: (_: Issue[]) => {},
    theoryQuery: ({issueArray}: TheoryInput) => {
      const ret = [];
      ret.push(issueArray[issueArray.length - 1]);
      return ret;
    },
    theoryUnplanned: ({issueMap}: TheoryInput) => {
      const ret: Issue[] = [];
      for (const issue of issueMap.values()) {
        ret.push(issue);
      }
      return ret.sort((a, b) => compareUTF8(b.id, a.id)).slice(0, 1);
    },
  },
  /*
  Results as of e2f73698dcc77664422fd2a765376aae73bb782a:
  name                           hz     min     max    mean     p75     p99    p995    p999     rme  samples
   · prepare and run          3,189.36  0.2000  0.5000  0.3135  0.4000  0.4000  0.5000  0.5000  ±0.97%     1595   slowest
   · previously prepared  3,240,693.86  0.0000  0.2000  0.0003  0.0000  0.0000  0.0000  0.1000  ±2.77%  1620671   fastest
   · theory                  24,548.00  0.0000  0.2000  0.0407  0.1000  0.2000  0.2000  0.2000  ±2.24%    12274
   · theory unplanned        24,150.00  0.0000  0.2000  0.0414  0.1000  0.2000  0.2000  0.2000  ±2.22%    12075
  */
  {
    name: 'table.* WHERE title = x | no index on title',
    getZql: (context: TestContext) =>
      new EntityQuery<{issue: Issue}>(context, 'issue')
        .select('*')
        .where('title', '=', 'Issue 500'),
    zqlExpected: (_: Issue[]) => {},
    theoryQuery: tableScanWithCompareTheory,
    theoryUnplanned: tableScanWithCompareTheory,
  },
  /*
  Results as of e2f73698dcc77664422fd2a765376aae73bb782a:
  name                           hz     min     max    mean     p75     p99    p995    p999     rme  samples
   · prepare and run            158.45  6.0000  6.7000  6.3113  6.4000  6.7000  6.7000  6.7000  ±0.50%       80   slowest
   · previously prepared  3,287,060.59  0.0000  0.2000  0.0003  0.0000  0.0000  0.0000  0.1000  ±2.77%  1643859
   · theory               8,591,162.00  0.0000  0.1000  0.0001  0.0000  0.0000  0.0000  0.1000  ±2.77%  4295581   fastest
   · theory unplanned         3,908.44  0.1000  0.4000  0.2559  0.3000  0.4000  0.4000  0.4000  ±1.02%     1955

   Notes:
   1. For the "query entire table" case, we could just return the `source` as the `view`. This would be instant.
   2. If no order is explicitly present we could default to undefined order. This seems like a confusing devx though
   and, I believe, diverges from PG which would order by primary key.
  */
  {
    name: 'table.*',
    getZql: (context: TestContext) =>
      new EntityQuery<{issue: Issue}>(context, 'issue').select('*'),
    zqlExpected: (_: Issue[]) => {},
    theoryQuery: ({issueArray}: TheoryInput) => issueArray,
    theoryUnplanned: ({issueMap}: TheoryInput) => {
      const ret: Issue[] = [];
      for (const issue of issueMap.values()) {
        ret.push(issue);
      }
      ret.sort((a, b) => compareUTF8(a.id, b.id));
      return ret;
    },
  },
  /*
  Results as of e2f73698dcc77664422fd2a765376aae73bb782a:
   ✓ 'table.* ORDER BY id DESC' (3) 2102ms
     name                           hz     min     max    mean     p75     p99    p995    p999     rme  samples
   · prepare and run            151.94  6.4000  6.9000  6.5816  6.6000  6.9000  6.9000  6.9000  ±0.33%       76   slowest
   · previously prepared  3,177,718.46  0.0000  0.2000  0.0003  0.0000  0.0000  0.0000  0.1000  ±2.77%  1589177
   · theory               8,291,076.00  0.0000  0.2000  0.0001  0.0000  0.0000  0.0000  0.1000  ±2.77%  4145538   fastest
   · theory unplanned         3,798.00  0.1000  0.4000  0.2633  0.3000  0.4000  0.4000  0.4000  ±1.04%     1899
   */
  {
    name: 'table.* ORDER BY id DESC',
    getZql: (context: TestContext) =>
      new EntityQuery<{issue: Issue}>(context, 'issue')
        .select('*')
        .orderBy('id', 'desc'),
    zqlExpected: (_: Issue[]) => {},
    theoryQuery: ({issueArray}: TheoryInput) => {
      const ret: Issue[] = [];
      for (let i = issueArray.length - 1; i >= 0; --i) {
        ret.push(issueArray[i]);
      }
      return ret;
    },
    theoryUnplanned: ({issueMap}: TheoryInput) => {
      const ret: Issue[] = [];
      for (const issue of issueMap.values()) {
        ret.push(issue);
      }
      return ret.sort((a, b) => compareUTF8(b.id, a.id));
    },
  },
  {
    name: 'issues and labels, sorted by title, first 100',
    getZql: (context: TestContext) => {
      const {issueQuery, issueLabelQuery, labelQuery} = makeQueries(context);
      return issueQuery
        .join(issueLabelQuery, 'issueLabel', 'issue.id', 'issueLabel.issueId')
        .leftJoin(labelQuery, 'label', 'issueLabel.labelId', 'label.id')
        .orderBy('issue.title', 'asc')
        .limit(100);
    },
    zqlExpected: () => {},
    theoryQuery: ({issuesByTitle, issueLabelMap, labelMap}: TheoryInput) => {
      const ret: {
        issue: Issue;
        label?: Label | undefined;
      }[] = [];
      for (let i = 0; i < 100; ++i) {
        const issue = issuesByTitle[i];
        const issueLabels = issueLabelMap.get(issue.id);
        if (issueLabels === undefined) {
          ret.push({
            issue,
          });
        } else {
          for (const issueLabel of issueLabels) {
            ret.push({
              issue,
              label: labelMap.get(issueLabel.labelId),
            });
          }
        }
      }
      return ret;
    },
    theoryUnplanned: ({
      issueArray,
      issueLabelArray,
      labelArray,
    }: TheoryInput) => {
      const ret: {
        issue: Issue;
        label?: Label;
      }[] = [];

      for (const issue of issueArray) {
        const row: (typeof ret)[number] = {
          issue,
        };
        for (const issueLabel of issueLabelArray) {
          if (issueLabel.issueId === issue.id) {
            for (const label of labelArray) {
              if (issueLabel.labelId === label.id) {
                row.label = label;
                break;
              }
            }
            break;
          }
        }
      }

      return ret
        .sort((l, r) => l.issue.title.localeCompare(r.issue.title))
        .slice(0, 100);
    },
  },
])(`$name`, ({getZql, theoryQuery, theoryUnplanned}) => {
  const context = new TestContext();
  const source = context.getSource<Issue>('issue');
  const labelSource = context.getSource<Label>('label');
  const issueLabelSource = context.getSource<IssueLabel>('issueLabel');
  const theoryMap = new Map<string, Issue>();
  const theoryLabelMap = new Map<string, Label>();
  const theoryIssueLabelMap = new Map<string, IssueLabel[]>();
  const theoryArray: Issue[] = [];
  const labelArray: Label[] = [];
  const issueLabelArray: IssueLabel[] = [];

  for (let i = 0; i < 10_000; ++i) {
    const issue = {id: i.toString().padStart(6, '0'), title: `Issue ${i}`};
    source.add(issue);
    theoryArray.push(issue);
    theoryMap.set(issue.id, issue);
  }

  for (let i = 0; i < 10; ++i) {
    const label = {id: i.toString().padStart(3, '0'), name: 'Label ${i}'};
    labelSource.add(label);
    labelArray.push(label);
    theoryLabelMap.set(label.id, label);
  }

  for (let i = 0; i < 10_000; ++i) {
    const numLabels = Math.floor(Math.random() * 4);
    for (let j = 0; j < numLabels; ++j) {
      const issueLabel = {
        id: i.toString(),
        issueId: i.toString().padStart(6, '0'),
        labelId: (i % 10).toString().padStart(3, '0'),
      };
      issueLabelArray.push(issueLabel);
      issueLabelSource.add(issueLabel);
      const existing = theoryIssueLabelMap.get(issueLabel.issueId);
      if (existing) {
        existing.push(issueLabel);
      } else {
        theoryIssueLabelMap.set(issueLabel.issueId, [issueLabel]);
      }
    }
  }
  const issuesByTitle = theoryArray
    .concat()
    .sort((l, r) => l.title.localeCompare(r.title));

  const prepared = getZql(context).prepare();

  bench('prepare and run', async () => {
    const stmt = getZql(context).prepare();
    await stmt.exec();
    stmt.destroy();
  });

  bench('previously prepared', async () => {
    await prepared.exec();
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
    theoryQuery({
      issueMap: theoryMap,
      labelMap: theoryLabelMap,
      issueLabelMap: theoryIssueLabelMap,
      issueArray: theoryArray,
      issuesByTitle,
      issueLabelArray,
      labelArray,
    });
  });

  bench('theory unplanned', () => {
    theoryUnplanned({
      issueMap: theoryMap,
      labelMap: theoryLabelMap,
      issueLabelMap: theoryIssueLabelMap,
      issueArray: theoryArray,
      issuesByTitle,
      issueLabelArray,
      labelArray,
    });
  });
});
