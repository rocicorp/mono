import {describe, expect, expectTypeOf, test} from 'vitest';
import * as v from '../../../shared/src/valita.ts';
import {
  createBuilder,
  syncedQuery,
  syncedQueryWithContext,
  withValidation,
} from './named.ts';
import {queryWithContext} from './query-internals.ts';
import type {QueryResultType, QueryReturn, QueryRowType, Row} from './query.ts';
import {schema} from './test/test-schemas.ts';

const builder = createBuilder(schema);

type IssueRow = {
  readonly id: string;
  readonly title: string;
  readonly description: string;
  readonly closed: boolean;
  readonly ownerId: string | null;
  readonly createdAt: number;
};

describe('types', () => {
  test('helpers for named query instances', () => {
    const idArgs = v.tuple([v.string()]);
    const def = syncedQuery('myQuery', idArgs, (id: string) =>
      builder.issue.where('id', id),
    );

    const query = def('123');
    type NamedQuery = typeof query;

    expectTypeOf<Row<NamedQuery>>().toEqualTypeOf<IssueRow>();
    expectTypeOf<QueryRowType<NamedQuery>>().toEqualTypeOf<IssueRow>();
    expectTypeOf<QueryResultType<NamedQuery>>().toEqualTypeOf<IssueRow[]>();

    type OneNamedQuery = ReturnType<typeof query.one>;
    expectTypeOf<QueryRowType<OneNamedQuery>>().toEqualTypeOf<
      IssueRow | undefined
    >();
    expectTypeOf<QueryResultType<OneNamedQuery>>().toEqualTypeOf<
      IssueRow | undefined
    >();
  });

  test('helpers for validated named query instances', () => {
    const idArgs = v.tuple([v.string()]);
    const def = syncedQuery('myQuery', idArgs, (id: string) =>
      builder.issue.where('id', id),
    );
    const validated = withValidation(def);

    const query = validated('ignored', '123');
    type NamedQuery = typeof query;

    expectTypeOf<Row<NamedQuery>>().toEqualTypeOf<IssueRow>();
    expectTypeOf<QueryRowType<NamedQuery>>().toEqualTypeOf<IssueRow>();
    expectTypeOf<QueryResultType<NamedQuery>>().toEqualTypeOf<IssueRow[]>();

    type OneNamedQuery = ReturnType<typeof query.one>;
    expectTypeOf<QueryRowType<OneNamedQuery>>().toEqualTypeOf<
      IssueRow | undefined
    >();
    expectTypeOf<QueryResultType<OneNamedQuery>>().toEqualTypeOf<
      IssueRow | undefined
    >();
  });
});

test('syncedQuery', () => {
  const idArgs = v.tuple([v.string()]);
  const def = syncedQuery('myQuery', idArgs, (id: string) =>
    builder.issue.where('id', id),
  );
  expect(def.queryName).toEqual('myQuery');
  expect(def.parse).toBeDefined();
  expect(def.takesContext).toEqual(false);

  expectTypeOf<Row<typeof def>>().toEqualTypeOf<QueryRowType<typeof def>>();
  expectTypeOf<QueryRowType<typeof def>>().toEqualTypeOf<{
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly closed: boolean;
    readonly ownerId: string | null;
    readonly createdAt: number;
  }>();
  expectTypeOf<QueryResultType<typeof def>>().toEqualTypeOf<
    {
      readonly id: string;
      readonly title: string;
      readonly description: string;
      readonly closed: boolean;
      readonly ownerId: string | null;
      readonly createdAt: number;
    }[]
  >();

  const query = def('123');
  expectTypeOf<QueryReturn<typeof query>>().toEqualTypeOf<{
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly closed: boolean;
    readonly ownerId: string | null;
    readonly createdAt: number;
  }>();

  const q = queryWithContext(query, undefined);
  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: ['123'],
  });

  expect(q.ast).toEqual({
    table: 'issue',
    where: {
      left: {
        name: 'id',
        type: 'column',
      },
      op: '=',
      right: {
        type: 'literal',
        value: '123',
      },
      type: 'simple',
    },
    orderBy: [['id', 'asc']],
  });

  const wv = withValidation(def);
  expect(wv.queryName).toEqual('myQuery');
  expect(wv.parse).toBeDefined();
  expect(wv.takesContext).toEqual(true);
  // @ts-expect-error 123 is not a string
  expect(() => wv('ignored', 123)).toThrow(
    'invalid_type at .0 (expected string)',
  );

  expectTypeOf<Row<typeof wv>>().toEqualTypeOf<QueryRowType<typeof wv>>();
  expectTypeOf<QueryRowType<typeof wv>>().toEqualTypeOf<{
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly closed: boolean;
    readonly ownerId: string | null;
    readonly createdAt: number;
  }>();
  expectTypeOf<QueryResultType<typeof wv>>().toEqualTypeOf<
    {
      readonly id: string;
      readonly title: string;
      readonly description: string;
      readonly closed: boolean;
      readonly ownerId: string | null;
      readonly createdAt: number;
    }[]
  >();

  const vquery = wv('ignored', '123');
  expectTypeOf<QueryReturn<typeof vquery>>().toEqualTypeOf<{
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly closed: boolean;
    readonly ownerId: string | null;
    readonly createdAt: number;
  }>();

  const vq = queryWithContext(vquery, undefined);
  expect(vq.customQueryID).toEqual({
    name: 'myQuery',
    args: ['123'],
  });

  expect(vq.ast).toEqual({
    table: 'issue',
    where: {
      left: {
        name: 'id',
        type: 'column',
      },
      op: '=',
      right: {
        type: 'literal',
        value: '123',
      },
      type: 'simple',
    },
    orderBy: [['id', 'asc']],
  });
});

test('syncedQueryWithContext', () => {
  const idArgs = v.tuple([v.string()]);
  const def = syncedQueryWithContext(
    'myQuery',
    idArgs,
    (context: string, id: string) =>
      builder.issue.where('id', id).where('ownerId', context),
  );
  expect(def.queryName).toEqual('myQuery');
  expect(def.parse).toBeDefined();
  expect(def.takesContext).toEqual(true);

  expectTypeOf<Row<typeof def>>().toEqualTypeOf<QueryRowType<typeof def>>();
  expectTypeOf<QueryRowType<typeof def>>().toEqualTypeOf<{
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly closed: boolean;
    readonly ownerId: string | null;
    readonly createdAt: number;
  }>();
  expectTypeOf<QueryResultType<typeof def>>().toEqualTypeOf<
    {
      readonly id: string;
      readonly title: string;
      readonly description: string;
      readonly closed: boolean;
      readonly ownerId: string | null;
      readonly createdAt: number;
    }[]
  >();

  const query2 = def('user1', '123');
  expectTypeOf<QueryReturn<typeof query2>>().toEqualTypeOf<{
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly closed: boolean;
    readonly ownerId: string | null;
    readonly createdAt: number;
  }>();

  const q = queryWithContext(query2, undefined);
  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: ['123'],
  });

  expect(q.ast).toEqual({
    table: 'issue',
    where: {
      conditions: [
        {
          left: {
            name: 'id',
            type: 'column',
          },
          op: '=',
          right: {
            type: 'literal',
            value: '123',
          },
          type: 'simple',
        },
        {
          left: {
            name: 'ownerId',
            type: 'column',
          },
          op: '=',
          right: {
            type: 'literal',
            value: 'user1',
          },
          type: 'simple',
        },
      ],
      type: 'and',
    },
    orderBy: [['id', 'asc']],
  });

  const wv = withValidation(def);
  expect(wv.queryName).toEqual('myQuery');
  expect(wv.parse).toBeDefined();
  expect(wv.takesContext).toEqual(true);
  // @ts-expect-error 123 is not a string
  expect(() => wv('ignored', 123)).toThrow(
    'invalid_type at .0 (expected string)',
  );

  expectTypeOf<Row<typeof wv>>().toEqualTypeOf<QueryRowType<typeof wv>>();
  expectTypeOf<QueryRowType<typeof wv>>().toEqualTypeOf<{
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly closed: boolean;
    readonly ownerId: string | null;
    readonly createdAt: number;
  }>();
  expectTypeOf<QueryResultType<typeof wv>>().toEqualTypeOf<
    {
      readonly id: string;
      readonly title: string;
      readonly description: string;
      readonly closed: boolean;
      readonly ownerId: string | null;
      readonly createdAt: number;
    }[]
  >();

  const vquery2 = wv('user1', '123');
  expectTypeOf<QueryReturn<typeof vquery2>>().toEqualTypeOf<{
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly closed: boolean;
    readonly ownerId: string | null;
    readonly createdAt: number;
  }>();

  const vq = queryWithContext(vquery2, undefined);
  expect(vq.customQueryID).toEqual({
    name: 'myQuery',
    args: ['123'],
  });

  expect(vq.ast).toEqual({
    table: 'issue',
    where: {
      conditions: [
        {
          left: {
            name: 'id',
            type: 'column',
          },
          op: '=',
          right: {
            type: 'literal',
            value: '123',
          },
          type: 'simple',
        },
        {
          left: {
            name: 'ownerId',
            type: 'column',
          },
          op: '=',
          right: {
            type: 'literal',
            value: 'user1',
          },
          type: 'simple',
        },
      ],
      type: 'and',
    },
    orderBy: [['id', 'asc']],
  });
});

// TODO: test unions

test('makeSchemaQuery', () => {
  const builders = createBuilder(schema);
  const q1 = queryWithContext(
    queryWithContext(builders.issue.where('id', '123'), undefined).nameAndArgs(
      'myName',
      ['123'],
    ),
    undefined,
  );
  expect(q1.ast).toMatchInlineSnapshot(`
    {
      "orderBy": [
        [
          "id",
          "asc",
        ],
      ],
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
