/* eslint-disable @typescript-eslint/no-explicit-any */
import {expect, expectTypeOf, test} from 'vitest';
import {
  hashOfAST,
  hashOfNameAndArgs,
} from '../../../zero-protocol/src/query-hash.ts';
import {query, type NamedQuery} from './named.ts';
import {ast, defaultFormat} from './query-impl.ts';
import {StaticQuery} from './static-query.ts';
import {schema} from './test/test-schemas.ts';

const tx = {
  issue: new StaticQuery(schema, 'issue', {table: 'issue'}, defaultFormat),
} as any;

test('defining a named query', () => {
  const named = query(schema, 'myName', (tx, id: string) =>
    tx.issue.where('id', id),
  );
  const q = named(tx, '123');
  expectTypeOf<ReturnType<typeof q.run>>().toEqualTypeOf<
    Promise<
      {
        readonly id: string;
        readonly title: string;
        readonly description: string;
        readonly closed: boolean;
        readonly ownerId: string | null;
        readonly createdAt: number;
      }[]
    >
  >();
  check(named);
});

test('binding query to a schema', () => {
  const bound = query.bindTo(schema);

  const named = bound('myName', (tx, id: string) => tx.issue.where('id', id));
  const q = named(tx, '123');
  expectTypeOf<ReturnType<typeof q.run>>().toEqualTypeOf<
    Promise<
      {
        readonly id: string;
        readonly title: string;
        readonly description: string;
        readonly closed: boolean;
        readonly ownerId: string | null;
        readonly createdAt: number;
      }[]
    >
  >();
  check(named);
});

function check(named: NamedQuery<typeof schema, [string], any>) {
  const r = named(tx, '123');

  const id = r.customQueryID;
  expect(id?.name).toBe('myName');
  expect(id?.args).toEqual(['123']);
  expect(ast(r)).toMatchInlineSnapshot(`
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

  // see comment on `r.hash()`
  expect(r.hash()).not.toEqual(hashOfNameAndArgs('issue', ['123']));
  expect(r.hash()).toEqual(
    hashOfAST((r as StaticQuery<typeof schema, 'issue'>).ast),
  );
}

test('makeSchemaQuery', () => {
  const builders = query(schema);
  const q1 = builders.issue.where('id', '123').nameAndArgs('myName', ['123']);
  expect(ast(q1)).toMatchInlineSnapshot(`
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
});
