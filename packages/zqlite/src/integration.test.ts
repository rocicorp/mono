import {test} from 'vitest';

import {EntityQuery} from '@rocicorp/zql/src/zql/query/entity-query.js';
import Database from 'better-sqlite3';
import {Materialite} from '@rocicorp/zql/src/zql/ivm/materialite.js';
import * as agg from '@rocicorp/zql/src/zql/query/agg.js';
import {createContext} from './context.js';

type Issue = {
  id: string;
  title: string;
  modified: number;
};

type IssueLabel = {
  id: string;
  issueId: string;
  labelId: string;
};

type Label = {
  id: string;
  name: string;
};

test('good old issues query', async () => {
  const db = new Database('./test.db');
  const context = createContext(new Materialite(), db);

  const issueQuery = new EntityQuery<{issue: Issue}>(context, 'issue');
  const labelQuery = new EntityQuery<{label: Label}>(context, 'label');
  const issueLabelQuery = new EntityQuery<{issueLabel: IssueLabel}>(
    context,
    'issueLabel',
  );

  const start = performance.now();

  const stmt = issueQuery
    .leftJoin(issueLabelQuery, 'issueLabel', 'issue.id', 'issueLabel.issueId')
    .leftJoin(labelQuery, 'label', 'issueLabel.labelId', 'label.id')
    .groupBy('issue.id')
    .select('issue.*', agg.array('label.*', 'labels'))
    .desc('issue.modified')
    .limit(10_000)
    .prepare();

  const rows = await stmt.exec();

  const end = performance.now();
  console.log('Time:', (end - start).toFixed(2) + 'ms');
  console.log('Rows: ', rows.length.toLocaleString());
});
