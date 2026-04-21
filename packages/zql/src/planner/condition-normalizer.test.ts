import fc from 'fast-check';
import {expect, test} from 'vitest';
import type {
  AST,
  Condition,
  CorrelatedSubqueryCondition,
  LiteralValue,
  SimpleOperator,
} from '../../../zero-protocol/src/ast.ts';
import {createPredicate, type NoSubqueryCondition} from '../builder/filter.ts';
import {normalizePlannerAST} from './condition-normalizer.ts';

const TRUE: Condition = {type: 'and', conditions: []};
const FALSE: Condition = {type: 'or', conditions: []};

test('drops identity branches without losing real filters', () => {
  const active = eq('active', true);

  expect(normalizeWhere({type: 'or', conditions: [active, FALSE]})).toEqual(
    active,
  );
  expect(normalizeWhere({type: 'and', conditions: [active, TRUE]})).toEqual(
    active,
  );
  expect(normalizeWhere({type: 'and', conditions: [active, FALSE]})).toEqual(
    FALSE,
  );
});

test('rewrites same-column OR equalities to IN', () => {
  expect(
    normalizeWhere({
      type: 'or',
      conditions: [eq('status', 'active'), eq('status', 'pending')],
    }),
  ).toEqual({
    type: 'simple',
    left: {type: 'column', name: 'status'},
    op: 'IN',
    right: {type: 'literal', value: ['active', 'pending']},
  });

  expect(
    normalizeWhere({
      type: 'or',
      conditions: [
        eq('status', 'active'),
        inCondition('status', 'IN', ['active']),
      ],
    }),
  ).toEqual(eq('status', 'active'));

  expect(
    normalizeWhere({
      type: 'or',
      conditions: [
        inCondition('status', 'IN', ['active', 'pending']),
        inCondition('status', 'IN', ['pending', 'draft']),
      ],
    }),
  ).toEqual({
    type: 'simple',
    left: {type: 'column', name: 'status'},
    op: 'IN',
    right: {type: 'literal', value: ['active', 'pending', 'draft']},
  });
});

test('normalizes degenerate IN predicates', () => {
  const notInEmpty = inCondition('status', 'NOT IN', []);

  expect(normalizeWhere(inCondition('status', 'IN', []))).toEqual(FALSE);
  expect(normalizeWhere(inCondition('status', 'IN', ['active']))).toEqual(
    eq('status', 'active'),
  );
  expect(normalizeWhere(notInEmpty)).toEqual(notInEmpty);
  expect(normalizeWhere(inCondition('status', 'NOT IN', ['active']))).toEqual({
    type: 'simple',
    left: {type: 'column', name: 'status'},
    op: '!=',
    right: {type: 'literal', value: 'active'},
  });
});

test('intersects same-column AND equalities and IN predicates', () => {
  expect(
    normalizeWhere({
      type: 'and',
      conditions: [
        inCondition('status', 'IN', ['active', 'pending']),
        eq('status', 'pending'),
      ],
    }),
  ).toEqual(eq('status', 'pending'));

  expect(
    normalizeWhere({
      type: 'and',
      conditions: [
        inCondition('status', 'IN', ['active', 'pending']),
        inCondition('status', 'IN', ['pending', 'draft']),
      ],
    }),
  ).toEqual(eq('status', 'pending'));

  expect(
    normalizeWhere({
      type: 'and',
      conditions: [eq('status', 'active'), eq('status', 'pending')],
    }),
  ).toEqual(FALSE);
});

test('applies same-column AND exclusions', () => {
  expect(
    normalizeWhere({
      type: 'and',
      conditions: [eq('status', 'active'), ne('status', 'active')],
    }),
  ).toEqual(FALSE);

  expect(
    normalizeWhere({
      type: 'and',
      conditions: [
        inCondition('status', 'IN', ['active', 'pending', 'draft']),
        ne('status', 'pending'),
        inCondition('status', 'NOT IN', ['draft']),
      ],
    }),
  ).toEqual(eq('status', 'active'));

  expect(
    normalizeWhere({
      type: 'and',
      conditions: [
        inCondition('status', 'NOT IN', ['active']),
        inCondition('status', 'NOT IN', ['pending']),
      ],
    }),
  ).toEqual(inCondition('status', 'NOT IN', ['active', 'pending']));
});

test('merges OR exists branches over the same relationship', () => {
  expect(
    normalizeWhere({
      type: 'or',
      conditions: [exists(eq('title', 'hello')), exists(eq('title', 'world'))],
    }),
  ).toEqual(
    exists({
      type: 'simple',
      left: {type: 'column', name: 'title'},
      op: 'IN',
      right: {type: 'literal', value: ['hello', 'world']},
    }),
  );
});

test('merges OR exists branches after implicit ordering is completed', () => {
  expect(
    normalizeWhere({
      type: 'or',
      conditions: [
        exists(eq('title', 'hello'), {subquery: {orderBy: [['id', 'asc']]}}),
        exists(eq('title', 'world'), {subquery: {orderBy: [['id', 'asc']]}}),
      ],
    }),
  ).toEqual(
    exists(
      {
        type: 'simple',
        left: {type: 'column', name: 'title'},
        op: 'IN',
        right: {type: 'literal', value: ['hello', 'world']},
      },
      {subquery: {orderBy: [['id', 'asc']]}},
    ),
  );
});

test('preserves unrestricted exists when merging narrower exists branches', () => {
  expect(
    normalizeWhere({
      type: 'or',
      conditions: [exists(undefined), exists(eq('title', 'hello'))],
    }),
  ).toEqual(exists(undefined));
});

test('factors common parent filters before merging child exists branches', () => {
  const active = eq('active', true);

  expect(
    normalizeWhere({
      type: 'or',
      conditions: [
        {
          type: 'and',
          conditions: [active, exists(eq('title', 'hello'))],
        },
        {
          type: 'and',
          conditions: [active, exists(eq('title', 'world'))],
        },
      ],
    }),
  ).toEqual({
    type: 'and',
    conditions: [
      active,
      exists({
        type: 'simple',
        left: {type: 'column', name: 'title'},
        op: 'IN',
        right: {type: 'literal', value: ['hello', 'world']},
      }),
    ],
  });
});

test('absorbs redundant branches after factoring common predicates', () => {
  const active = eq('active', true);
  const archived = eq('archived', false);
  const pending = eq('status', 'pending');

  expect(
    normalizeWhere({
      type: 'or',
      conditions: [
        active,
        {
          type: 'and',
          conditions: [active, eq('status', 'pending')],
        },
      ],
    }),
  ).toEqual(active);

  expect(
    normalizeWhere({
      type: 'or',
      conditions: [
        active,
        {
          type: 'and',
          conditions: [active, pending],
        },
        archived,
      ],
    }),
  ).toEqual({
    type: 'or',
    conditions: [active, archived],
  });

  expect(
    normalizeWhere({
      type: 'or',
      conditions: [
        {
          type: 'and',
          conditions: [active, pending],
        },
        {
          type: 'and',
          conditions: [active, pending, archived],
        },
      ],
    }),
  ).toEqual({
    type: 'and',
    conditions: [active, pending],
  });
});

test('collapses impossible exists subqueries', () => {
  const active = eq('active', true);

  expect(normalizeWhere(exists(FALSE))).toEqual(FALSE);
  expect(normalizeWhere(exists(FALSE, {op: 'NOT EXISTS'}))).toEqual(TRUE);
  expect(normalizeWhere(exists(undefined, {subquery: {limit: 0}}))).toEqual(
    FALSE,
  );

  expect(
    normalizeWhere({
      type: 'or',
      conditions: [active, exists(FALSE)],
    }),
  ).toEqual(active);

  expect(normalizeWhere(exists(FALSE, {scalar: true}))).toEqual(
    exists(FALSE, {scalar: true}),
  );
});

test('does not merge exists branches with different semantics', () => {
  expect(
    normalizeWhere({
      type: 'or',
      conditions: [
        exists(eq('title', 'hello'), {op: 'NOT EXISTS'}),
        exists(eq('title', 'world'), {op: 'NOT EXISTS'}),
      ],
    }),
  ).toMatchObject({
    type: 'or',
    conditions: [{op: 'NOT EXISTS'}, {op: 'NOT EXISTS'}],
  });

  expect(
    normalizeWhere({
      type: 'or',
      conditions: [
        exists(eq('title', 'hello'), {scalar: true}),
        exists(eq('title', 'world'), {scalar: true}),
      ],
    }),
  ).toMatchObject({type: 'or', conditions: [{scalar: true}, {scalar: true}]});

  expect(
    normalizeWhere({
      type: 'or',
      conditions: [
        exists(eq('title', 'hello'), {subquery: {limit: 1}}),
        exists(eq('title', 'world'), {subquery: {limit: 1}}),
      ],
    }),
  ).toMatchObject({
    type: 'or',
    conditions: [
      {related: {subquery: {limit: 1}}},
      {related: {subquery: {limit: 1}}},
    ],
  });
});

test('normalizes related subqueries recursively', () => {
  expect(
    normalizePlannerAST({
      table: 'users',
      related: [
        {
          correlation: {
            parentField: ['id'],
            childField: ['userId'],
          },
          subquery: {
            table: 'posts',
            where: {
              type: 'or',
              conditions: [eq('title', 'hello'), eq('title', 'world')],
            },
          },
        },
      ],
    }).related?.[0].subquery.where,
  ).toEqual({
    type: 'simple',
    left: {type: 'column', name: 'title'},
    op: 'IN',
    right: {type: 'literal', value: ['hello', 'world']},
  });
});

test('preserves simple filter semantics under generated rows', () => {
  fc.assert(
    fc.property(
      filterConditionArbitrary(),
      rowArbitrary(),
      (condition, row) => {
        const normalized = normalizePlannerAST({
          table: 'users',
          where: condition,
        }).where as NoSubqueryCondition | undefined;
        const before = createPredicate(condition)(row);
        const after = normalized ? createPredicate(normalized)(row) : true;

        expect(after).toBe(before);
      },
    ),
    {numRuns: 1_000},
  );
});

test('is idempotent for generated simple filters', () => {
  fc.assert(
    fc.property(filterConditionArbitrary(), condition => {
      const once = normalizePlannerAST({table: 'users', where: condition});
      const twice = normalizePlannerAST(once);

      expect(twice).toEqual(once);
    }),
    {numRuns: 1_000},
  );
});

test('is idempotent for generated planner filters with correlated subqueries', () => {
  fc.assert(
    fc.property(plannerConditionArbitrary(), condition => {
      const once = normalizePlannerAST({table: 'users', where: condition});
      const twice = normalizePlannerAST(once);

      expect(twice).toEqual(once);
    }),
    {numRuns: 1_000},
  );
});

function normalizeWhere(where: Condition): Condition | undefined {
  return normalizePlannerAST({table: 'users', where}).where;
}

function eq(name: string, value: LiteralValue): Condition {
  return {
    type: 'simple',
    left: {type: 'column', name},
    op: '=',
    right: {type: 'literal', value},
  };
}

function ne(name: string, value: LiteralValue): Condition {
  return {
    type: 'simple',
    left: {type: 'column', name},
    op: '!=',
    right: {type: 'literal', value},
  };
}

function inCondition(
  name: string,
  op: 'IN' | 'NOT IN',
  values: readonly (string | number | boolean)[],
): Condition {
  return {
    type: 'simple',
    left: {type: 'column', name},
    op,
    right: {type: 'literal', value: values},
  };
}

function exists(
  where: Condition | undefined,
  options: {
    readonly op?: 'EXISTS' | 'NOT EXISTS' | undefined;
    readonly scalar?: boolean | undefined;
    readonly subquery?: Partial<AST> | undefined;
  } = {},
): CorrelatedSubqueryCondition {
  return {
    type: 'correlatedSubquery',
    op: options.op ?? 'EXISTS',
    scalar: options.scalar,
    related: {
      correlation: {
        parentField: ['id'],
        childField: ['userId'],
      },
      subquery: {
        table: 'posts',
        where,
        ...options.subquery,
      },
    },
  };
}

function filterConditionArbitrary(): fc.Arbitrary<NoSubqueryCondition> {
  return fc.letrec(tie => ({
    condition: fc.oneof(
      simpleConditionArbitrary(),
      fc.record({
        type: fc.constant('and' as const),
        conditions: fc.array(tie('condition'), {maxLength: 4}),
      }),
      fc.record({
        type: fc.constant('or' as const),
        conditions: fc.array(tie('condition'), {maxLength: 4}),
      }),
    ),
  })).condition as fc.Arbitrary<NoSubqueryCondition>;
}

function plannerConditionArbitrary(): fc.Arbitrary<Condition> {
  return fc.letrec(tie => ({
    condition: fc.oneof(
      simpleConditionArbitrary(),
      correlatedSubqueryConditionArbitrary(),
      fc.record({
        type: fc.constant('and' as const),
        conditions: fc.array(tie('condition'), {maxLength: 4}),
      }),
      fc.record({
        type: fc.constant('or' as const),
        conditions: fc.array(tie('condition'), {maxLength: 4}),
      }),
    ),
  })).condition as fc.Arbitrary<Condition>;
}

function correlatedSubqueryConditionArbitrary(): fc.Arbitrary<CorrelatedSubqueryCondition> {
  return fc.record({
    type: fc.constant('correlatedSubquery' as const),
    op: fc.constantFrom('EXISTS' as const, 'NOT EXISTS' as const),
    flip: fc.option(fc.boolean(), {nil: undefined}),
    scalar: fc.option(fc.boolean(), {nil: undefined}),
    related: fc.record({
      correlation: fc.constant({
        parentField: ['id'],
        childField: ['userId'],
      } as const),
      subquery: fc.record({
        table: fc.constant('posts'),
        where: fc.option(filterConditionArbitrary(), {nil: undefined}),
        limit: fc.option(fc.constantFrom(0, 1, 10), {nil: undefined}),
      }),
    }),
  });
}

function simpleConditionArbitrary(): fc.Arbitrary<NoSubqueryCondition> {
  return fc.oneof(
    fc.record({
      type: fc.constant('simple' as const),
      left: columnReferenceArbitrary(),
      op: fc.constantFrom<SimpleOperator>('=', '!=', '<', '<=', '>', '>='),
      right: fc.record({
        type: fc.constant('literal' as const),
        value: scalarLiteralArbitrary(),
      }),
    }),
    fc.record({
      type: fc.constant('simple' as const),
      left: columnReferenceArbitrary(),
      op: fc.constantFrom<SimpleOperator>('IS', 'IS NOT'),
      right: fc.record({
        type: fc.constant('literal' as const),
        value: fc.constant(null),
      }),
    }),
    fc.record({
      type: fc.constant('simple' as const),
      left: columnReferenceArbitrary(),
      op: fc.constantFrom<SimpleOperator>('IN', 'NOT IN'),
      right: fc.record({
        type: fc.constant('literal' as const),
        value: fc.array(inLiteralArbitrary(), {maxLength: 6}),
      }),
    }),
  );
}

function rowArbitrary() {
  return fc.record({
    a: rowValueArbitrary(),
    b: rowValueArbitrary(),
    c: rowValueArbitrary(),
  });
}

function columnReferenceArbitrary() {
  return fc.record({
    type: fc.constant('column' as const),
    name: fc.constantFrom('a', 'b', 'c'),
  });
}

function scalarLiteralArbitrary(): fc.Arbitrary<
  string | number | boolean | null
> {
  return fc.oneof(inLiteralArbitrary(), fc.constant(null));
}

function rowValueArbitrary(): fc.Arbitrary<
  string | number | boolean | null | undefined
> {
  return fc.oneof(
    inLiteralArbitrary(),
    fc.constant(null),
    fc.constant(undefined),
  );
}

function inLiteralArbitrary(): fc.Arbitrary<string | number | boolean> {
  return fc.oneof(
    fc.string({maxLength: 8}),
    fc.integer({min: -20, max: 20}),
    fc.boolean(),
  );
}
