import {expect, test} from 'vitest';
import {addData, QueryDelegateImpl} from './test/query-delegate.ts';
import {schema} from './test/test-schemas.ts';
import {StaticQuery} from './static-query.ts';
import {defaultFormat} from './query-impl.ts';

test('static query can be converted to a real query', () => {
  const queryDelegate = new QueryDelegateImpl();
  addData(queryDelegate);
  const issueQuery = new StaticQuery(
    schema,
    'issue',
    {table: 'issue'},
    defaultFormat,
  );

  const m = issueQuery.asRunnableQuery(queryDelegate).materialize();

  let rows: readonly unknown[] = [];
  m.addListener(data => {
    rows = data;
  });

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
