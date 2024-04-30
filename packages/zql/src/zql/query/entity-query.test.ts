import {describe, expect, expectTypeOf, test} from 'vitest';
import {z} from 'zod';
import type {AST, SimpleOperator} from '../ast/ast.js';
import {makeTestContext} from '../context/context.js';
import * as agg from './agg.js';
import {conditionToString} from './condition-to-string.js';
import {
  EntityQuery,
  FieldAsOperatorInput,
  ValueAsOperatorInput,
  WhereCondition,
  and,
  astForTesting,
  exp,
  not,
  or,
} from './entity-query.js';

type WeakKey = object;
function ast(q: WeakKey): AST {
  const {alias: _, ...rest} = astForTesting(q);
  return rest;
}

const entitiesPrefix = 'e/';

const context = makeTestContext();
test('query types', () => {
  const sym = Symbol('sym');
  type E1 = {
    id: string;
    str: string;
    num: number;
    bool: boolean;
    optStr?: string | undefined;
    [sym]: boolean;
  };

  const q = new EntityQuery<{e1: E1}>(context, 'e1', entitiesPrefix);

  // @ts-expect-error - selecting fields that do not exist in the schema is a type error
  q.select('does-not-exist');

  expectTypeOf(q.select).toBeCallableWith('id');
  expectTypeOf(q.select).toBeCallableWith('str', 'optStr');

  expectTypeOf(q.select('id', 'str').prepare().exec()).toMatchTypeOf<
    Promise<readonly {id: string; str: string}[]>
  >();
  expectTypeOf(q.select('id').prepare().exec()).toMatchTypeOf<
    Promise<readonly {id: string}[]>
  >();
  expectTypeOf(q.select('optStr').prepare().exec()).toMatchTypeOf<
    Promise<readonly {optStr?: string | undefined}[]>
  >();

  // where/order/limit do not change return type
  expectTypeOf(q.where).toBeCallableWith('id', '=', 'foo');
  expectTypeOf(q.where).toBeCallableWith('str', '<', 'foo');
  expectTypeOf(q.where).toBeCallableWith('optStr', '>', 'foo');

  // @ts-expect-error - comparing on missing fields is an error
  q.where('does-not-exist', '=', 'x');

  // @ts-expect-error - comparing with the wrong data type for the value is an error
  q.where('id', '=', 1);

  expectTypeOf(q.select(agg.count()).prepare().exec()).toMatchTypeOf<
    Promise<readonly {readonly count: number}[]>
  >();

  // @ts-expect-error - 'x' is not a field that we can aggregate on
  q.select(agg.array('x')).groupBy('id');

  expectTypeOf(
    q.select('id', agg.array('str')).groupBy('optStr').prepare().exec(),
  ).toMatchTypeOf<Promise<readonly {id: string; str: readonly string[]}[]>>();

  expectTypeOf(q.select('*').prepare().exec()).toMatchTypeOf<
    Promise<readonly E1[]>
  >();

  expectTypeOf(
    q
      .select('id', agg.array('str', 'alias'))
      .groupBy('optStr')
      .prepare()
      .exec(),
  ).toMatchTypeOf<Promise<readonly {id: string; alias: readonly string[]}[]>>();

  // @ts-expect-error - Argument of type 'number' is not assignable to parameter of type 'string'.ts(2345)
  q.where(exp('id', '=', 123));

  // @ts-expect-error - Argument of type '"id2"' is not assignable to parameter of type 'Selectable<{ fields: E1; }>'.ts(2345)
  q.where(exp('id2', '=', 'abc'));

  // @ts-expect-error - Argument of type 'number' is not assignable to parameter of type 'string'.ts(2345)
  q.where(and(exp('id', '=', 'a'), exp('str', '=', 42)));

  // @ts-expect-error - Argument of type 'number' is not assignable to parameter of type 'string'.ts(2345)
  q.where(or(exp('id', '=', 'a'), exp('str', '=', 42)));

  // @ts-expect-error - Argument of type '"id2"' is not assignable to parameter of type 'Selectable<{ fields: E1; }>'.ts(2345)
  q.where(and(exp('id2', '=', 'a'), exp('str', '=', 42)));

  // @ts-expect-error - Argument of type '"id2"' is not assignable to parameter of type 'Selectable<{ fields: E1; }>'.ts(2345)
  q.where(or(exp('id2', '=', 'a'), exp('str', '=', 42)));

  // and nest
  q.where(
    or(
      // @ts-expect-error - Argument of type 'number' is not assignable to parameter of type 'string'.ts(2345)
      and(exp('id', '=', 'a'), exp('str', '=', 123)),
      // @ts-expect-error - Argument of type '"id2"' is not assignable to parameter of type 'Selectable<{ fields: E1; }>'.ts(2345)
      and(exp('id2', '=', 'a'), exp('str', '=', 'b')),
    ),
  );
  q.where(
    and(
      // @ts-expect-error - Argument of type 'number' is not assignable to parameter of type 'string'.ts(2345)
      or(exp('id', '=', 'a'), exp('str', '=', 123)),
      // @ts-expect-error - Argument of type '"id2"' is not assignable to parameter of type 'Selectable<{ fields: E1; }>'.ts(2345)
      or(exp('id2', '=', 'a'), exp('str', '=', 'b')),
    ),
  );

  expectTypeOf(
    q.select(agg.min('num')).groupBy('str').prepare().exec(),
  ).toMatchTypeOf<Promise<readonly {readonly num: number}[]>>();
  expectTypeOf(
    q.select(agg.min('str')).groupBy('num').prepare().exec(),
  ).toMatchTypeOf<Promise<readonly {readonly str: string}[]>>();
  expectTypeOf(
    q.select(agg.min('bool')).groupBy('num').prepare().exec(),
  ).toMatchTypeOf<Promise<readonly {readonly bool: boolean}[]>>();
  expectTypeOf(
    q.select(agg.max('num')).groupBy('str').prepare().exec(),
  ).toMatchTypeOf<Promise<readonly {readonly num: number}[]>>();
  expectTypeOf(
    q.select(agg.max('str')).groupBy('num').prepare().exec(),
  ).toMatchTypeOf<Promise<readonly {readonly str: string}[]>>();
  expectTypeOf(
    q.select(agg.max('bool')).groupBy('num').prepare().exec(),
  ).toMatchTypeOf<Promise<readonly {readonly bool: boolean}[]>>();
});

test('join types', () => {
  type Issue = {
    id: string;
    title: string;
    ownerId: string;
    creatorId: string;
  };

  type User = {
    id: string;
    name: string;
  };

  const issueQuery = new EntityQuery<{issue: Issue}>(
    context,
    'issue',
    entitiesPrefix,
  );
  const userQuery = new EntityQuery<{user: User}>(
    context,
    'user',
    entitiesPrefix,
  );

  expectTypeOf(
    issueQuery
      .join(userQuery, 'owner', 'ownerId', 'id')
      .select('issue.id', 'issue.title', 'owner.name')
      .prepare()
      .exec(),
  ).toMatchTypeOf<
    Promise<
      readonly {
        readonly owner: {
          readonly name: string;
        };
        readonly issue: {
          readonly title: string;
          readonly id: string;
        };
      }[]
    >
  >();

  expectTypeOf(
    issueQuery
      .join(userQuery, 'owner', 'ownerId', 'id')
      .join(userQuery, 'creator', 'issue.creatorId', 'id')
      .select('issue.id', 'issue.title', 'owner.name', 'creator.name')
      .prepare()
      .exec(),
  ).toMatchTypeOf<
    Promise<
      readonly {
        readonly creator: {
          readonly name: string;
        };
        readonly owner: {
          readonly name: string;
        };
        readonly issue: {
          readonly title: string;
          readonly id: string;
        };
      }[]
    >
  >();

  expectTypeOf(
    issueQuery
      .join(userQuery, 'owner', 'ownerId', 'id')
      .select('owner.id')
      .prepare()
      .exec(),
  ).toMatchTypeOf<
    Promise<
      readonly {
        readonly owner: {
          readonly id: string;
        };
      }[]
    >
  >();

  // ambiguity fails
  issueQuery
    .join(userQuery, 'owner', 'ownerId', 'id')
    .join(userQuery, 'creator', 'issue.creatorId', 'id')
    // @ts-expect-error - Argument of type '"name"' is not assignable to parameter of type
    .select('name');
});

test('left join types', () => {
  type Issue = {
    id: string;
    title: string;
    ownerId: string;
    creatorId: string;
  };

  type User = {
    id: string;
    name: string;
  };

  const issueQuery = new EntityQuery<{issue: Issue}>(
    context,
    'issue',
    entitiesPrefix,
  );
  const userQuery = new EntityQuery<{user: User}>(
    context,
    'user',
    entitiesPrefix,
  );

  const r1 = issueQuery
    .leftJoin(userQuery, 'owner', 'ownerId', 'id')
    .select('owner.name')
    .prepare()
    .exec();

  expectTypeOf(r1).toMatchTypeOf<
    Promise<readonly {readonly owner?: {readonly name: string} | undefined}[]>
  >();

  const r2 = issueQuery
    .leftJoin(userQuery, 'owner', 'ownerId', 'id')
    .select('owner.*')
    .prepare()
    .exec();

  expectTypeOf(r2).toMatchTypeOf<
    Promise<
      readonly {
        readonly owner?: User | undefined;
      }[]
    >
  >();

  const r3 = issueQuery
    .leftJoin(userQuery, 'owner', 'ownerId', 'id')
    .select('*')
    .prepare()
    .exec();

  expectTypeOf(r3).toMatchTypeOf<
    Promise<
      readonly {
        readonly issue: Issue;
        readonly owner?: User | undefined;
      }[]
    >
  >();
});

test('FieldValue type', () => {
  type E = {
    e: {
      id: string;
      n: number;
      s: string;
      b: boolean;
      optN?: number | undefined;
      optS?: string | undefined;
      optB?: boolean | undefined;
    };
  };
  expectTypeOf<FieldAsOperatorInput<E, 'id', '='>>().toEqualTypeOf<string>();
  expectTypeOf<FieldAsOperatorInput<E, 'e.id', '='>>().toEqualTypeOf<string>();
  expectTypeOf<FieldAsOperatorInput<E, 'n', '='>>().toEqualTypeOf<number>();
  expectTypeOf<FieldAsOperatorInput<E, 'e.n', '='>>().toEqualTypeOf<number>();
  expectTypeOf<FieldAsOperatorInput<E, 's', '!='>>().toEqualTypeOf<string>();
  expectTypeOf<FieldAsOperatorInput<E, 'e.s', '!='>>().toEqualTypeOf<string>();
  expectTypeOf<FieldAsOperatorInput<E, 'b', '='>>().toEqualTypeOf<boolean>();
  expectTypeOf<FieldAsOperatorInput<E, 'optN', '='>>().toEqualTypeOf<number>();
  expectTypeOf<
    FieldAsOperatorInput<E, 'e.optN', '='>
  >().toEqualTypeOf<number>();
  expectTypeOf<FieldAsOperatorInput<E, 'optS', '!='>>().toEqualTypeOf<string>();
  expectTypeOf<
    FieldAsOperatorInput<E, 'e.optS', '!='>
  >().toEqualTypeOf<string>();
  expectTypeOf<FieldAsOperatorInput<E, 'optB', '='>>().toEqualTypeOf<boolean>();
  expectTypeOf<
    FieldAsOperatorInput<E, 'e.optB', '='>
  >().toEqualTypeOf<boolean>();

  // booleans not allowed with order operators
  expectTypeOf<FieldAsOperatorInput<E, 'b', '<'>>().toEqualTypeOf<never>();
  expectTypeOf<FieldAsOperatorInput<E, 'b', '<='>>().toEqualTypeOf<never>();
  expectTypeOf<FieldAsOperatorInput<E, 'b', '>'>>().toEqualTypeOf<never>();
  expectTypeOf<FieldAsOperatorInput<E, 'b', '>='>>().toEqualTypeOf<never>();
  expectTypeOf<FieldAsOperatorInput<E, 'n', '<'>>().toEqualTypeOf<number>();
  expectTypeOf<FieldAsOperatorInput<E, 'n', '<='>>().toEqualTypeOf<number>();
  expectTypeOf<FieldAsOperatorInput<E, 'n', '>'>>().toEqualTypeOf<number>();
  expectTypeOf<FieldAsOperatorInput<E, 'n', '>='>>().toEqualTypeOf<number>();
  expectTypeOf<FieldAsOperatorInput<E, 's', '<'>>().toEqualTypeOf<string>();
  expectTypeOf<FieldAsOperatorInput<E, 's', '<='>>().toEqualTypeOf<string>();
  expectTypeOf<FieldAsOperatorInput<E, 's', '>'>>().toEqualTypeOf<string>();
  expectTypeOf<FieldAsOperatorInput<E, 's', '>='>>().toEqualTypeOf<string>();

  expectTypeOf<FieldAsOperatorInput<E, 'optB', '<'>>().toEqualTypeOf<never>();
  expectTypeOf<FieldAsOperatorInput<E, 'optB', '<='>>().toEqualTypeOf<never>();
  expectTypeOf<FieldAsOperatorInput<E, 'optB', '>'>>().toEqualTypeOf<never>();
  expectTypeOf<FieldAsOperatorInput<E, 'optB', '>='>>().toEqualTypeOf<never>();
  expectTypeOf<FieldAsOperatorInput<E, 'optN', '<'>>().toEqualTypeOf<number>();
  expectTypeOf<FieldAsOperatorInput<E, 'optN', '<='>>().toEqualTypeOf<number>();
  expectTypeOf<FieldAsOperatorInput<E, 'optN', '>'>>().toEqualTypeOf<number>();
  expectTypeOf<FieldAsOperatorInput<E, 'optN', '>='>>().toEqualTypeOf<number>();
  expectTypeOf<FieldAsOperatorInput<E, 'optS', '<'>>().toEqualTypeOf<string>();
  expectTypeOf<FieldAsOperatorInput<E, 'optS', '<='>>().toEqualTypeOf<string>();
  expectTypeOf<FieldAsOperatorInput<E, 'optS', '>'>>().toEqualTypeOf<string>();
  expectTypeOf<FieldAsOperatorInput<E, 'optS', '>='>>().toEqualTypeOf<string>();

  expectTypeOf<FieldAsOperatorInput<E, 'n', 'IN'>>().toEqualTypeOf<number[]>();
  expectTypeOf<FieldAsOperatorInput<E, 'n', 'NOT IN'>>().toEqualTypeOf<
    number[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 's', 'IN'>>().toEqualTypeOf<string[]>();
  expectTypeOf<FieldAsOperatorInput<E, 's', 'NOT IN'>>().toEqualTypeOf<
    string[]
  >();
  expectTypeOf<ValueAsOperatorInput<boolean, 'IN'>>().toEqualTypeOf<
    boolean[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'e.b', 'IN'>>().toEqualTypeOf<
    boolean[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'b', 'NOT IN'>>().toEqualTypeOf<
    boolean[]
  >();

  expectTypeOf<FieldAsOperatorInput<E, 'optN', 'IN'>>().toEqualTypeOf<
    number[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'optN', 'NOT IN'>>().toEqualTypeOf<
    number[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'optS', 'IN'>>().toEqualTypeOf<
    string[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'optS', 'NOT IN'>>().toEqualTypeOf<
    string[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'optB', 'IN'>>().toEqualTypeOf<
    boolean[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'optB', 'NOT IN'>>().toEqualTypeOf<
    boolean[]
  >();

  expectTypeOf<FieldAsOperatorInput<E, 'n', 'SUPERSET'>>().toEqualTypeOf<
    number[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'n', 'DISJOINT'>>().toEqualTypeOf<
    number[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'n', 'CONGRUENT'>>().toEqualTypeOf<
    number[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'n', 'INCONGRUENT'>>().toEqualTypeOf<
    number[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 's', 'SUPERSET'>>().toEqualTypeOf<
    string[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 's', 'DISJOINT'>>().toEqualTypeOf<
    string[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 's', 'CONGRUENT'>>().toEqualTypeOf<
    string[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 's', 'INCONGRUENT'>>().toEqualTypeOf<
    string[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'e.b', 'SUPERSET'>>().toEqualTypeOf<
    boolean[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'e.b', 'DISJOINT'>>().toEqualTypeOf<
    boolean[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'e.b', 'CONGRUENT'>>().toEqualTypeOf<
    boolean[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'e.b', 'INCONGRUENT'>>().toEqualTypeOf<
    boolean[]
  >();
  expectTypeOf<FieldAsOperatorInput<E, 'b', 'DISJOINT'>>().toEqualTypeOf<
    boolean[]
  >();

  expectTypeOf<FieldAsOperatorInput<E, 'n', 'LIKE'>>().toEqualTypeOf<never>();
  expectTypeOf<
    FieldAsOperatorInput<E, 'n', 'NOT LIKE'>
  >().toEqualTypeOf<never>();
  expectTypeOf<FieldAsOperatorInput<E, 's', 'LIKE'>>().toEqualTypeOf<string>();
  expectTypeOf<
    FieldAsOperatorInput<E, 's', 'NOT LIKE'>
  >().toEqualTypeOf<string>();
  expectTypeOf<FieldAsOperatorInput<E, 'b', 'LIKE'>>().toEqualTypeOf<never>();
  expectTypeOf<
    FieldAsOperatorInput<E, 'b', 'NOT LIKE'>
  >().toEqualTypeOf<never>();

  expectTypeOf<
    FieldAsOperatorInput<E, 'optN', 'LIKE'>
  >().toEqualTypeOf<never>();
  expectTypeOf<
    FieldAsOperatorInput<E, 'optN', 'NOT LIKE'>
  >().toEqualTypeOf<never>();
  expectTypeOf<
    FieldAsOperatorInput<E, 'optS', 'LIKE'>
  >().toEqualTypeOf<string>();
  expectTypeOf<
    FieldAsOperatorInput<E, 'optS', 'NOT LIKE'>
  >().toEqualTypeOf<string>();
  expectTypeOf<
    FieldAsOperatorInput<E, 'optB', 'LIKE'>
  >().toEqualTypeOf<never>();
  expectTypeOf<
    FieldAsOperatorInput<E, 'optB', 'NOT LIKE'>
  >().toEqualTypeOf<never>();

  const q = new EntityQuery<E>(context, 'e', entitiesPrefix);
  q.where('n', '<', 1);
  q.where('s', '>', 'a');
  q.where('b', '=', true);
  q.where('id', '=', 'a');
  // @ts-expect-error Argument of type 'boolean' is not assignable to parameter of type 'never'.ts(2345)
  q.where('b', '<', false);
  // @ts-expect-error Argument of type 'boolean' is not assignable to parameter of type 'never'.ts(2345)
  q.where('b', '<=', false);
  // @ts-expect-error Argument of type 'boolean' is not assignable to parameter of type 'never'.ts(2345)
  q.where('b', '>', false);
  // @ts-expect-error Argument of type 'boolean' is not assignable to parameter of type 'never'.ts(2345)
  q.where('b', '>=', false);

  // @ts-expect-error Argument of type 'string' is not assignable to parameter of type 'never'.ts(2345)
  q.where('n', 'LIKE', 'abc');
  // @ts-expect-error Argument of type 'number' is not assignable to parameter of type 'never'.ts(2345)
  q.where('n', 'ILIKE', 123);
  q.where('s', 'LIKE', 'abc');
  // @ts-expect-error Argument of type 'number' is not assignable to parameter of type 'string'.ts(2345)
  q.where('s', 'ILIKE', 123);
  // @ts-expect-error Argument of type 'string' is not assignable to parameter of type 'never'.ts(2345)
  q.where('b', 'LIKE', 'abc');
  // @ts-expect-error Argument of type 'boolean' is not assignable to parameter of type 'never'.ts(2345)
  q.where('b', 'ILIKE', true);

  q.where('n', 'IN', [1, 2, 3]);
  // @ts-expect-error Argument of type 'number' is not assignable to parameter of type 'number[]'.ts(2345)
  q.where('n', 'IN', 1);
  q.where('s', 'IN', ['a', 'b', 'c']);
  // @ts-expect-error Argument of type 'string' is not assignable to parameter of type 'string[]'.ts(2345)
  q.where('s', 'IN', 'a');
  q.where('b', 'IN', [true, false]);
  // @ts-expect-error Argument of type 'boolean' is not assignable to parameter of type 'boolean[]'.ts(2345)
  q.where('b', 'IN', true);
});

const e1 = z.object({
  id: z.string(),
  a: z.number(),
  b: z.bigint(),
  c: z.string().optional(),
  d: z.boolean(),
});

type E1 = z.infer<typeof e1>;
const dummyObject: E1 = {
  id: 'a',
  a: 1,
  b: 1n,
  c: '',
  d: true,
};
describe('ast', () => {
  test('select', () => {
    const q = new EntityQuery<{e1: E1}>(context, 'e1', entitiesPrefix);

    // each individual field is selectable on its own
    Object.keys(dummyObject).forEach(k => {
      const newq = q.select(k as keyof E1);
      expect(ast(newq).select).toEqual([['e1.' + k, k]]);
    });

    // all fields are selectable together
    let newq = q.select(...(Object.keys(dummyObject) as (keyof E1)[]));
    expect(ast(newq).select).toEqual(
      Object.keys(dummyObject).map(k => ['e1.' + k, k]),
    );

    // we can call select many times to build up the selection set
    newq = q;
    Object.keys(dummyObject).forEach(k => {
      newq = newq.select(k as keyof E1);
    });
    expect(ast(newq).select).toEqual(
      Object.keys(dummyObject).map(k => ['e1.' + k, k]),
    );

    // we remove duplicates
    newq = q;
    Object.keys(dummyObject).forEach(k => {
      newq = newq.select(k as keyof E1);
    });
    Object.keys(dummyObject).forEach(k => {
      newq = newq.select(k as keyof E1);
    });
    expect(ast(newq).select).toEqual(
      Object.keys(dummyObject).map(k => ['e1.' + k, k]),
    );
  });

  test('where', () => {
    let q = new EntityQuery<{e1: E1}>(context, 'e1', entitiesPrefix);

    // where is applied
    q = q.where('id', '=', 'a');

    expect(ast(q)).toEqual({
      table: 'e1',
      orderBy: [['id'], 'asc'],
      where: {
        field: 'e1.id',
        op: '=',
        value: {
          type: 'literal',
          value: 'a',
        },
      },
    });

    // additional wheres are anded
    q = q.where('a', '>', 0);

    expect(ast(q)).toEqual({
      table: 'e1',
      orderBy: [['id'], 'asc'],
      where: {
        op: 'AND',
        conditions: [
          {
            field: 'e1.id',
            op: '=',
            value: {
              type: 'literal',
              value: 'a',
            },
          },
          {
            field: 'e1.a',
            op: '>',
            value: {
              type: 'literal',
              value: 0,
            },
          },
        ],
      },
    });

    q = q.where('c', '=', 'foo');
    // multiple ANDs are flattened
    expect(ast(q)).toEqual({
      table: 'e1',
      orderBy: [['id'], 'asc'],
      where: {
        op: 'AND',
        conditions: [
          {
            field: 'e1.id',
            op: '=',
            value: {
              type: 'literal',
              value: 'a',
            },
          },
          {
            field: 'e1.a',
            op: '>',
            value: {
              type: 'literal',
              value: 0,
            },
          },
          {
            field: 'e1.c',
            op: '=',
            value: {
              type: 'literal',
              value: 'foo',
            },
          },
        ],
      },
    });
  });

  test('limit', () => {
    const q = new EntityQuery<{e1: E1}>(context, 'e1', entitiesPrefix);
    expect(ast(q.limit(10))).toEqual({
      orderBy: [['id'], 'asc'],
      table: 'e1',
      limit: 10,
    });
  });

  test('asc/desc', () => {
    const q = new EntityQuery<{e1: E1}>(context, 'e1', entitiesPrefix);

    // order methods update the ast
    expect(ast(q.asc('id'))).toEqual({
      table: 'e1',
      orderBy: [['e1.id'], 'asc'],
    });
    expect(ast(q.desc('id'))).toEqual({
      table: 'e1',
      orderBy: [['e1.id'], 'desc'],
    });
    expect(ast(q.asc('id', 'a', 'b', 'c', 'd'))).toEqual({
      table: 'e1',
      orderBy: [['e1.id', 'e1.a', 'e1.b', 'e1.c', 'e1.d'], 'asc'],
    });
  });

  test('independent of method call order', () => {
    const base = new EntityQuery<{e1: E1}>(context, 'e1', entitiesPrefix);

    const calls = {
      select(q: typeof base) {
        return q.select('b');
      },
      where(q: typeof base) {
        return q.where('c', 'LIKE', 'foo');
      },
      limit(q: typeof base) {
        return q.limit(10);
      },
      asc(q: typeof base) {
        return q.asc('a');
      },
    };

    let q = base;
    for (const call of Object.values(calls)) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      q = call(q) as any;
    }
    const inOrderToAST = ast(q);

    q = base;
    for (const call of Object.values(calls).reverse()) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      q = call(q) as any;
    }
    const reverseToAST = ast(q);

    expect({
      ...inOrderToAST,
    }).toEqual({
      ...reverseToAST,
    });
  });

  test('or', () => {
    const q = new EntityQuery<{e1: E1}>(context, 'e1', entitiesPrefix);

    expect(ast(q.where(or(exp('a', '=', 123), exp('c', '=', 'abc'))))).toEqual({
      table: 'e1',
      orderBy: [['id'], 'asc'],
      where: {
        op: 'OR',
        conditions: [
          {op: '=', field: 'e1.a', value: {type: 'literal', value: 123}},
          {op: '=', field: 'e1.c', value: {type: 'literal', value: 'abc'}},
        ],
      },
    });

    expect(
      ast(
        q.where(
          and(
            exp('e1.a', '=', 1),
            or(exp('e1.d', '=', true), exp('e1.c', '=', 'hello')),
          ),
        ),
      ),
    ).toEqual({
      table: 'e1',
      orderBy: [['id'], 'asc'],
      where: {
        op: 'AND',
        conditions: [
          {op: '=', field: 'e1.a', value: {type: 'literal', value: 1}},
          {
            op: 'OR',
            conditions: [
              {op: '=', field: 'e1.d', value: {type: 'literal', value: true}},
              {
                op: '=',
                field: 'e1.c',
                value: {type: 'literal', value: 'hello'},
              },
            ],
          },
        ],
      },
    });
  });

  test('flatten ands', () => {
    type S = {s: {id: string; a: number; b: string; c: boolean; d: string}};

    expect(
      and<S>(
        exp('a', '=', 1),
        exp('b', '=', '2'),
        and<S>(exp('c', '=', true), exp('d', '=', '3')),
      ),
    ).toEqual(
      and<S>(
        exp('a', '=', 1),
        exp('b', '=', '2'),
        exp('c', '=', true),
        exp('d', '=', '3'),
      ),
    );

    expect(
      and<S>(
        exp('a', '=', 1),
        and<S>(exp('c', '=', true), exp('d', '=', '3')),
        exp('b', '=', '2'),
      ),
    ).toEqual(
      and<S>(
        exp('a', '=', 1),
        exp('c', '=', true),
        exp('d', '=', '3'),
        exp('b', '=', '2'),
      ),
    );

    expect(
      and<S>(
        and<S>(exp('c', '=', true), exp('d', '=', '3')),
        exp('a', '=', 1),
        exp('b', '=', '2'),
      ),
    ).toEqual(
      and<S>(
        exp('c', '=', true),
        exp('d', '=', '3'),
        exp('a', '=', 1),
        exp('b', '=', '2'),
      ),
    );

    expect(
      and<S>(
        and<S>(exp('c', '=', true), exp('d', '=', '3')),
        and<S>(exp('a', '=', 1), exp('b', '=', '2')),
      ),
    ).toEqual(
      and<S>(
        exp('c', '=', true),
        exp('d', '=', '3'),
        exp('a', '=', 1),
        exp('b', '=', '2'),
      ),
    );
  });

  test('flatten ors', () => {
    type S = {s: {id: string; a: number; b: string; c: boolean; d: string}};

    expect(
      or<S>(
        exp('a', '=', 1),
        or<S>(exp('c', '=', true), exp('d', '=', '3')),
        exp('b', '=', '2'),
      ),
    ).toEqual(
      or<S>(
        exp('a', '=', 1),
        exp('c', '=', true),
        exp('d', '=', '3'),
        exp('b', '=', '2'),
      ),
    );
  });

  test('consecutive wheres/ands should be merged', () => {
    const q = new EntityQuery<{e1: E1}>(context, 'e1', entitiesPrefix);

    expect(
      ast(
        q
          .where(and(exp('a', '=', 1), exp('a', '=', 2)))
          .where(and(exp('c', '=', 'a'), exp('c', '=', 'b'))),
      ).where,
    ).toEqual({
      op: 'AND',
      conditions: [
        {
          field: 'e1.a',
          op: '=',
          value: {
            type: 'literal',
            value: 1,
          },
        },
        {
          field: 'e1.a',
          op: '=',
          value: {
            type: 'literal',
            value: 2,
          },
        },
        {
          field: 'e1.c',
          op: '=',
          value: {
            type: 'literal',
            value: 'a',
          },
        },
        {
          field: 'e1.c',
          op: '=',
          value: {
            type: 'literal',
            value: 'b',
          },
        },
      ],
    });

    expect(
      ast(
        q
          .where(exp('a', '=', 123))
          .where(exp('c', '=', 'abc'))
          .where(exp('d', '=', true)),
      ).where,
    ).toEqual({
      op: 'AND',
      conditions: [
        {
          field: 'e1.a',
          op: '=',
          value: {
            type: 'literal',
            value: 123,
          },
        },
        {
          field: 'e1.c',
          op: '=',
          value: {
            type: 'literal',
            value: 'abc',
          },
        },
        {
          field: 'e1.d',
          op: '=',
          value: {
            type: 'literal',
            value: true,
          },
        },
      ],
    });

    expect(
      ast(
        q
          .where(exp('a', '=', 123))
          .where(or(exp('c', '=', 'abc'), exp('c', '=', 'def')))
          .where(exp('d', '=', true)),
      ).where,
    ).toEqual({
      op: 'AND',
      conditions: [
        {
          field: 'e1.a',
          op: '=',
          value: {
            type: 'literal',
            value: 123,
          },
        },
        {
          op: 'OR',
          conditions: [
            {
              field: 'e1.c',
              op: '=',
              value: {
                type: 'literal',
                value: 'abc',
              },
            },
            {
              field: 'e1.c',
              op: '=',
              value: {
                type: 'literal',
                value: 'def',
              },
            },
          ],
        },
        {
          field: 'e1.d',
          op: '=',
          value: {
            type: 'literal',
            value: true,
          },
        },
      ],
    });
  });

  test('consecutive ors', () => {
    const q = new EntityQuery<{e1: E1}>(context, 'e1', entitiesPrefix);

    expect(
      ast(q.where(or(exp('a', '=', 123), exp('a', '=', 456)))).where,
    ).toEqual({
      op: 'OR',
      conditions: [
        {
          field: 'e1.a',
          op: '=',
          value: {
            type: 'literal',
            value: 123,
          },
        },
        {
          field: 'e1.a',
          op: '=',
          value: {
            type: 'literal',
            value: 456,
          },
        },
      ],
    });

    expect(
      ast(
        q
          .where(or(exp('a', '=', 123), exp('a', '=', 456)))
          .where(or(exp('c', '=', 'abc'), exp('c', '=', 'def'))),
      ).where,
    ).toEqual({
      op: 'AND',
      conditions: [
        {
          op: 'OR',
          conditions: [
            {
              field: 'e1.a',
              op: '=',
              value: {
                type: 'literal',
                value: 123,
              },
            },
            {
              field: 'e1.a',
              op: '=',
              value: {
                type: 'literal',
                value: 456,
              },
            },
          ],
        },
        {
          op: 'OR',
          conditions: [
            {
              field: 'e1.c',
              op: '=',
              value: {
                type: 'literal',
                value: 'abc',
              },
            },
            {
              field: 'e1.c',
              op: '=',
              value: {
                type: 'literal',
                value: 'def',
              },
            },
          ],
        },
      ],
    });
  });
});

describe('NOT', () => {
  describe('Negate Ops', () => {
    const cases: {
      in: SimpleOperator;
      out: SimpleOperator;
    }[] = [
      {in: '=', out: '!='},
      {in: '!=', out: '='},
      {in: '<', out: '>='},
      {in: '>', out: '<='},
      {in: '>=', out: '<'},
      {in: '<=', out: '>'},
      {in: 'IN', out: 'NOT IN'},
      {in: 'NOT IN', out: 'IN'},
      {in: 'LIKE', out: 'NOT LIKE'},
      {in: 'NOT LIKE', out: 'LIKE'},
      {in: 'ILIKE', out: 'NOT ILIKE'},
      {in: 'NOT ILIKE', out: 'ILIKE'},
    ];

    for (const c of cases) {
      test(`${c.in} -> ${c.out}`, () => {
        const q = new EntityQuery<{e1: E1}>(context, 'e1', entitiesPrefix);
        expect(ast(q.where(not(exp('a', c.in, 1)))).where).toEqual({
          op: c.out,
          field: 'e1.a',
          value: {type: 'literal', value: 1},
        });
      });
    }
  });
});

describe("De Morgan's Law", () => {
  type S = {
    s: {
      id: string;
      n: number;
      s: string;
    };
  };

  const cases: {
    condition: WhereCondition<S>;
    expected: WhereCondition<S>;
  }[] = [
    {
      condition: exp('n', '=', 1),
      expected: exp('n', '!=', 1),
    },

    {
      condition: and(exp('n', '!=', 1), exp('n', '<', 2)),
      expected: or(exp('n', '=', 1), exp('n', '>=', 2)),
    },

    {
      condition: or(exp('n', '<=', 1), exp('n', '>', 2)),
      expected: and(exp('n', '>', 1), exp('n', '<=', 2)),
    },

    {
      condition: or(
        and(exp('n', '>=', 1), exp('n', 'IN', [1, 2])),
        exp('n', 'NOT IN', [3, 4]),
      ),
      expected: and(
        or(exp('n', '<', 1), exp('n', 'NOT IN', [1, 2])),
        exp('n', 'IN', [3, 4]),
      ),
    },

    {
      condition: and(
        or(exp('n', 'NOT IN', [5, 6]), exp('s', 'LIKE', 'Hi')),
        exp('s', 'NOT LIKE', 'Hi'),
      ),
      expected: or(
        and(exp('n', 'IN', [5, 6]), exp('s', 'NOT LIKE', 'Hi')),
        exp('s', 'LIKE', 'Hi'),
      ),
    },

    {
      condition: not(exp('s', 'ILIKE', 'hi')),
      expected: exp('s', 'ILIKE', 'hi'),
    },

    {
      condition: not(exp('s', 'NOT ILIKE', 'bye')),
      expected: exp('s', 'NOT ILIKE', 'bye'),
    },
  ];

  for (const c of cases) {
    test(
      'NOT(' +
        conditionToString(c.condition) +
        ') -> ' +
        conditionToString(c.expected),
      () => {
        expect(not(c.condition)).toEqual(c.expected);
      },
    );
  }
});

test('where is always qualified', () => {
  const q = new EntityQuery<{e1: E1}>(context, 'e1', 'e1');
  expect(ast(q.where(exp('a', '=', 1))).where).toEqual({
    field: 'e1.a',
    op: '=',
    value: {type: 'literal', value: 1},
  });

  expect(
    ast(
      // TODO: self-join should require qualification, no?
      q.where('a', '=', 1).join(q, 'e2', 'e1.a', 'a').where('e2.c', '=', 'sdf'),
    ),
  ).toEqual({
    table: 'e1',
    orderBy: [['id'], 'asc'],
    where: {
      op: 'AND',
      conditions: [
        {op: '=', field: 'e1.a', value: {type: 'literal', value: 1}},
        {op: '=', field: 'e2.c', value: {type: 'literal', value: 'sdf'}},
      ],
    },
    joins: [
      {
        type: 'inner',
        other: {table: 'e1', orderBy: [['id'], 'asc']},
        as: 'e2',
        on: ['e1.a', 'e2.a'],
      },
    ],
  });
});

describe('all references to columns are always qualified', () => {
  const q = new EntityQuery<{e1: E1}>(context, 'e1', 'e1');
  test.each([
    {
      test: 'unqalified where',
      q: q.where('a', '=', 1),
      expected: {
        orderBy: [['id'], 'asc'],
        table: 'e1',
        where: {
          op: '=',
          field: 'e1.a',
          value: {type: 'literal', value: 1},
        },
      },
    },
    {
      test: 'qualified where',
      q: q.where('e1.a', '=', 1),
      expected: {
        orderBy: [['id'], 'asc'],
        table: 'e1',
        where: {
          op: '=',
          field: 'e1.a',
          value: {type: 'literal', value: 1},
        },
      },
    },
    {
      test: 'unqalified select',
      q: q.select('a'),
      expected: {
        aggregate: [],
        orderBy: [['id'], 'asc'],
        table: 'e1',
        select: [['e1.a', 'a']],
      },
    },
    {
      test: 'order by',
      q: q.asc('a'),
      expected: {
        orderBy: [['e1.a', 'e1.id'], 'asc'],
        table: 'e1',
      },
    },
    {
      test: 'unqalified on conditions for join',
      q: q.join(q, 'e2', 'a', 'a'),
      expected: {
        table: 'e1',
        orderBy: [['id'], 'asc'],
        joins: [
          {
            type: 'inner',
            other: {table: 'e1', orderBy: [['id'], 'asc']},
            as: 'e2',
            on: ['e1.a', 'e2.a'],
          },
        ],
      },
    },
    {
      test: 'qualified on conditions for join',
      // TODO: join type signature is wrong.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      q: q.join(q, 'e2', 'e1.a', 'e2.a' as any),
      expected: {
        table: 'e1',
        orderBy: [['id'], 'asc'],
        joins: [
          {
            type: 'inner',
            other: {table: 'e1', orderBy: [['id'], 'asc']},
            as: 'e2',
            on: ['e1.a', 'e2.a'],
          },
        ],
      },
    },
    {
      test: 'unqualified junction join',
      q: q.join(q, 'e2', 'a', 'a').join(q, 'e3', 'e2.a', 'a'),
      expected: {
        table: 'e1',
        orderBy: [['id'], 'asc'],
        joins: [
          {
            type: 'inner',
            other: {table: 'e1', orderBy: [['id'], 'asc']},
            as: 'e2',
            on: ['e1.a', 'e2.a'],
          },
          {
            type: 'inner',
            other: {table: 'e1', orderBy: [['id'], 'asc']},
            as: 'e3',
            on: ['e2.a', 'e3.a'],
          },
        ],
      },
    },
    {
      test: 'having is not qualified auto-qualified',
      q: q.having(exp('a', '=', 1)),
      expected: {
        table: 'e1',
        orderBy: [['id'], 'asc'],
        having: {
          field: 'a',
          op: '=',
          value: {type: 'literal', value: 1},
        },
      },
    },
  ])('$test', ({q, expected}) => {
    expect(ast(q)).toEqual(expected);
  });
});
