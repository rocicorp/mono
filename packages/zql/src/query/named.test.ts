/* eslint-disable @typescript-eslint/no-explicit-any */
import {expect, expectTypeOf, test} from 'vitest';
import {schema} from './test/test-schemas.ts';
import {query, type NamedQuery} from './named.ts';
import {StaticQuery} from './static-query.ts';
import {ast, defaultFormat} from './query-impl.ts';

test('defining a named query', () => {
  const named = query(schema, 'issue', (tx, id: string) =>
    tx.issue.where('id', id),
  );
  expectTypeOf(named).toEqualTypeOf<NamedQuery<typeof schema, [string]>>();
  check(named);
});

test('binding query to a schema', () => {
  const bound = query.bindTo(schema);

  const named = bound('issue', (tx, id: string) => tx.issue.where('id', id));

  expectTypeOf(named).toEqualTypeOf<NamedQuery<typeof schema, [string]>>();
  check(named);
});

function check(named: NamedQuery<typeof schema, [string]>) {
  const r = named(
    {
      issue: new StaticQuery(schema, 'issue', {table: 'issue'}, defaultFormat),
    } as any,
    '123',
  );

  expect(r.name).toBe('issue');
  expect(r.args).toEqual(['123']);
  expect(ast(r.query)).toMatchInlineSnapshot(`
    {
      "table": "issue",
      "where": {
        "left": {
          "name": "id",
          "type": "column",
        },
        "op": "=",
        "right": {
          "type": "literal",
          "value": "123",
        },
        "type": "simple",
      },
    }
  `);
}
